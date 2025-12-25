from app.db.neo4j import run
from app.services.embeddings import EmbeddingService
from app.models.response import MovieRecommendation, ParsedQuery

class RecommenderService:
    def __init__(self, embedding_service: EmbeddingService):
        self.embedding_service = embedding_service

    def recommend(self, parsed_query: ParsedQuery, limit: int = 5, debug: bool = False) -> tuple[list[MovieRecommendation], dict | None]:
        # 1. Embed the semantic query
        vector = self.embedding_service.embed_text(parsed_query.semantic_query)
        
        # Candidate pool size (for filtering depth)
        K_POOL = 100
        
        from datetime import datetime
        current_year = datetime.now().year

        # 2. Build Hybrid Cypher Query with Recency, Award, and Comedy Boosts
        # Final Score = similarity + recency_boost + award_boost + comedy_boost
        cypher = """
        CALL db.index.vector.queryNodes("movie_embedding_index", $k, $embedding)
        YIELD node, score as similarity
        """
        
        params = {
            "embedding": vector,
            "k": K_POOL,
            "current_year": current_year
        }
        
        # Example Augmented Ranking Query:
        # CALL db.index.vector.queryNodes("movie_embedding_index", 15, $embedding)
        # YIELD node, similarity
        # WITH node, similarity,
        #      CASE 
        #        WHEN rel_year < 2000 THEN 0
        #        WHEN rel_year >= 2018 THEN 0.05
        #        ELSE (rel_year - 2000.0) / (2018 - 2000) * 0.05
        #      END AS recency_boost,
        #      CASE WHEN EXISTS { (node)-[:RECEIVED]->() } THEN 0.05 ELSE 0 END AS award_boost,
        #      CASE WHEN EXISTS { (node)-[:HAS_GENRE]->(:Genre {name: "Comedy"}) } THEN 0.05 ELSE 0 END AS comedy_boost
        # RETURN node, similarity, (similarity + recency_boost + award_boost + comedy_boost) AS final_score
        
        conditions = []
        
        # Temporal Filter
        if parsed_query.filters and parsed_query.filters.year_from:
            conditions.append("toInteger(substring(node.release_date, 0, 4)) >= $year_from")
            params["year_from"] = parsed_query.filters.year_from
            
        # Award Filter
        if parsed_query.filters and parsed_query.filters.award_event:
            award_match = """EXISTS {
                MATCH (node)-[r:RECEIVED]->(c:AwardCategory)<-[:HAS_CATEGORY]-(e:AwardEvent)
                WHERE e.name = $award_event
                  AND r.result = $award_result
            }"""
            conditions.append(award_match)
            params["award_event"] = parsed_query.filters.award_event
            params["award_result"] = parsed_query.filters.award_result or "won"
            
        if conditions:
            cypher += "\nWHERE " + " AND ".join(conditions)
            
        # Final scoring and projection
        cypher += """
        WITH node, similarity,
             toInteger(substring(node.release_date, 0, 4)) AS rel_year,
             // Genre Boost: Soft preference for intent alignment (e.g. "funny" -> Comedy)
             CASE WHEN EXISTS { (node)-[:HAS_GENRE]->(:Genre {name: "Comedy"}) } THEN 0.05 ELSE 0 END AS cb,
             CASE WHEN EXISTS { (node)-[:RECEIVED]->() } THEN 0.05 ELSE 0 END AS ab
        WITH node, similarity, cb, ab,
             // Saturating Recency: Boost modern films, but plateau after 2018 
             // to avoid newer sequels always outranking slightly older originals.
             CASE 
               WHEN rel_year IS NULL OR rel_year < 2000 THEN 0
               WHEN rel_year >= 2018 THEN 0.05
               ELSE (rel_year - 2000.0) / (2018 - 2000) * 0.05
             END AS rb
        RETURN node, similarity, rb as recency_boost, ab as award_boost, cb as comedy_boost,
               (similarity + rb + ab + cb) AS final_score
        ORDER BY final_score DESC
        LIMIT $limit
        """
        params["limit"] = limit
        
        results = run(cypher, params)
        
        # 3. Handle Debugging (Counts)
        debug_info = None
        if debug:
            debug_info = self._get_debug_counts(vector, K_POOL, parsed_query)
        
        # 4. Format Recommendations
        recommendations = []
        for row in results:
            node = row["node"]
            
            # Extract awards for display
            award_names = self._get_node_awards(node["tmdb_id"])
            
            recommendations.append(MovieRecommendation(
                tmdb_id=node["tmdb_id"],
                title=node["title"],
                original_title=node["original_title"],
                release_year=int(node["release_date"][:4]) if node.get("release_date") else None,
                final_score=round(row["final_score"], 4),
                similarity_score=round(row["similarity"], 4),
                recency_boost=round(row["recency_boost"], 4),
                award_boost=round(row["award_boost"], 4),
                comedy_boost=round(row["comedy_boost"], 4),
                awards=award_names
            ))
            
        return recommendations, debug_info

    def _get_debug_counts(self, vector: list[float], k: int, parsed_query: ParsedQuery) -> dict:
        """Runs explicit COUNT(*) queries to track candidate reduction."""
        debug = {"vector_candidates": k}
        
        params = {"embedding": vector, "k": k}
        
        # Base where we started
        running_conditions = []
        
        if parsed_query.filters and parsed_query.filters.year_from:
            running_conditions.append("toInteger(substring(node.release_date, 0, 4)) >= $year_from")
            params["year_from"] = parsed_query.filters.year_from
            
            count_cypher = f"""
            CALL db.index.vector.queryNodes("movie_embedding_index", $k, $embedding)
            YIELD node
            WHERE {" AND ".join(running_conditions)}
            RETURN count(*) as count
            """
            debug["after_year_filter"] = run(count_cypher, params)[0]["count"]

        if parsed_query.filters and parsed_query.filters.award_event:
            award_match = """EXISTS {
                MATCH (node)-[r:RECEIVED]->(c:AwardCategory)<-[:HAS_CATEGORY]-(e:AwardEvent)
                WHERE e.name = $award_event
                  AND r.result = $award_result
            }"""
            running_conditions.append(award_match)
            params["award_event"] = parsed_query.filters.award_event
            params["award_result"] = parsed_query.filters.award_result or "won"
            
            count_cypher = f"""
            CALL db.index.vector.queryNodes("movie_embedding_index", $k, $embedding)
            YIELD node
            WHERE {" AND ".join(running_conditions)}
            RETURN count(*) as count
            """
            debug["after_award_filter"] = run(count_cypher, params)[0]["count"]
            
        return debug

    def _get_node_awards(self, tmdb_id: int) -> list[str]:
        """Helper to fetch award event names for a movie."""
        query = """
        MATCH (m:Movie {tmdb_id: $tmdb_id})-[:RECEIVED]->(c:AwardCategory)<-[:HAS_CATEGORY]-(e:AwardEvent)
        RETURN DISTINCT e.name as name
        """
        rows = run(query, {"tmdb_id": tmdb_id})
        return [r["name"] for r in rows]

