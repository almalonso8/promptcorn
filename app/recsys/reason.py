from app.db.neo4j import run
from collections import defaultdict

def rerank(candidates: list[dict], limit: int = 5):
    if not candidates:
        return []

    movie_ids = [c["node"]["tmdb_id"] for c in candidates]


    cypher = """
    MATCH (m:Movie)
    WHERE m.tmdb_id IN $movie_ids

    // --- Actor co-occurrence inside candidate set
    OPTIONAL MATCH (m)<-[:ACTED_IN]-(a:Person)-[:ACTED_IN]->(other:Movie)
    WHERE other.tmdb_id IN $movie_ids AND other.tmdb_id <> m.tmdb_id

    WITH m,
         count(DISTINCT a) AS shared_actors

    // --- Director clustering
    OPTIONAL MATCH (m)<-[:DIRECTED]-(d:Person)-[:DIRECTED]->(other2:Movie)
    WHERE other2.tmdb_id IN $movie_ids AND other2.tmdb_id <> m.tmdb_id

    WITH m,
         shared_actors,
         count(DISTINCT d) AS director_cluster

    // --- Genre overlap
    OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)<-[:HAS_GENRE]-(other3:Movie)
    WHERE other3.tmdb_id IN $movie_ids AND other3.tmdb_id <> m.tmdb_id

    WITH m,
         shared_actors,
         director_cluster,
         count(DISTINCT g) AS shared_genres

    RETURN
        m.tmdb_id AS id,
        shared_actors,
        director_cluster,
        shared_genres,
        coalesce(m.popularity, 0) AS popularity
    """

    rows = run(cypher, {"movie_ids": movie_ids})

    # Build lookup
    boost_map = {}
    explanation_map = defaultdict(list)

    for r in rows:
        score = 0.0

        if r["shared_actors"] > 0:
            score += min(r["shared_actors"], 3) * 0.05
            explanation_map[r["id"]].append(
                "Shares cast connections with other closely related results"
            )

        if r["director_cluster"] > 0:
            score += 0.001
            explanation_map[r["id"]].append(
                "Directed by the same filmmaker as other strong matches"
            )

        if r["shared_genres"] > 0:
            score += min(r["shared_genres"], 3) * 0.04
            explanation_map[r["id"]].append(
                "Part of a tightly connected genre cluster"
            )

        # Popularity dampening (anti-blockbuster gravity)
        if r["popularity"] > 80:
            score -= 0.09

        boost_map[r["id"]] = score

    # Merge with vector scores
    ranked = []
    for c in candidates:
        m = dict(c["node"])
        tmdb_id = m["tmdb_id"]
        final_score = c["score"] + boost_map.get(tmdb_id, 0.0)

        ranked.append({
            "title": m["title"],
            "score": final_score,
            "explanation": explanation_map.get(tmdb_id, [])[:2]  # cap explanations
        })

    return sorted(ranked, key=lambda x: x["score"], reverse=True)[:limit]
