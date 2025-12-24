from app.db.neo4j import run

def retrieve_candidates(
    embedding: list[float],
    limit: int = 50,
    must_have_oscar: bool = False,
    genre: str | None = None,
    language: str | None = None,
):
    cypher = """
    CALL db.index.vector.queryNodes(
      "movie_embedding_index",
      $k,
      $embedding
    )
    YIELD node, score
    WITH node, score
    WHERE 1 = 1
    """

    params = {"embedding": embedding, "k": limit}

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

    cypher += """
    RETURN node, score
    ORDER BY score DESC
    """

    return run(cypher, params)
