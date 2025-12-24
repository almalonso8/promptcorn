from app.db.neo4j import run

def retrieve_candidates(
    embedding: list[float],
    limit: int = 50,
    must_have_oscar: bool = False,
    genre: str | None = None,
    language: str | None = None,
    min_year: int | None = None,
    max_year: int | None = None,
):
    cypher = """
    CALL db.index.vector.queryNodes(
      "movie_embedding_index",
      $k,
      $embedding
    )
    YIELD node, score
    
    WITH node, score,
         CASE
           WHEN node.release_date IS NOT NULL AND size(node.release_date) >= 4
           THEN toInteger(substring(node.release_date, 0, 4))
           ELSE NULL
         END AS effective_year

    WHERE 1 = 1
    """

    params = {
        "embedding": embedding,
        "k": limit,
        "min_year": min_year,
        "max_year": max_year,
    }

    if must_have_oscar:
        cypher += """
        AND EXISTS {
            (node)-[:WON]->(:AwardCategory {category: "Best Picture"})
        }
        """

    if genre:
        cypher += """
        AND EXISTS {
            (node)-[:HAS_GENRE]->(:Genre {name: $genre})
        }
        """
        params["genre"] = genre

    if language:
        cypher += "AND node.original_language = $language\n"
        params["language"] = language

    # Hard Temporal Filter
    cypher += """
    AND ($min_year IS NULL OR (effective_year IS NOT NULL AND effective_year >= $min_year))
    AND ($max_year IS NULL OR (effective_year IS NOT NULL AND effective_year <= $max_year))
    """

    cypher += """
    RETURN node, score
    ORDER BY score DESC
    """

    return run(cypher, params)
