from neo4j import GraphDatabase
import pandas as pd
from collections import defaultdict
from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


OUTPUT_PATH = "data/normalized/films_core.parquet"
TOP_ACTORS = 10


def export_films() -> None:
    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
    )

    movies = {}

    with driver.session() as session:
        # --- Base movie data ---
        result = session.run(
            """
            MATCH (m:Movie)
            RETURN
              m.tmdb_id        AS tmdb_id,
              m.title          AS title,
              m.original_title AS original_title,
              m.overview       AS overview,
              m.release_date   AS release_date,
              m.poster_path    AS poster_path
            """
        )

        for row in result:
            movies[row["tmdb_id"]] = {
                "tmdb_id": row["tmdb_id"],
                "title": row["title"],
                "original_title": row["original_title"],
                "overview": row["overview"],
                "release_date": row["release_date"],
                "poster_path": row["poster_path"],
                "genres": [],
                "directors": [],
                "actors": [],
                "keywords": [],
            }

        # --- Genres ---
        result = session.run(
            """
            MATCH (m:Movie)-[:HAS_GENRE]->(g:Genre)
            RETURN m.tmdb_id AS tmdb_id, g.name AS name
            """
        )
        for row in result:
            movies[row["tmdb_id"]]["genres"].append(row["name"])

        # --- Directors ---
        result = session.run(
            """
            MATCH (p:Person)-[:DIRECTED]->(m:Movie)
            RETURN m.tmdb_id AS tmdb_id, p.name AS name
            """
        )
        for row in result:
            movies[row["tmdb_id"]]["directors"].append(row["name"])

        # --- Actors (top N per movie) ---
        result = session.run(
            """
            MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
            RETURN m.tmdb_id AS tmdb_id, p.name AS name
            """
        )

        actor_counter = defaultdict(int)
        for row in result:
            tmdb_id = row["tmdb_id"]
            if len(movies[tmdb_id]["actors"]) < TOP_ACTORS:
                movies[tmdb_id]["actors"].append(row["name"])

        # --- Keywords ---
        result = session.run(
            """
            MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword)
            RETURN m.tmdb_id AS tmdb_id, k.name AS name
            """
        )
        for row in result:
            movies[row["tmdb_id"]]["keywords"].append(row["name"])

    driver.close()

    df = pd.DataFrame(movies.values())
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Exported {len(df)} movies → {OUTPUT_PATH}")


if __name__ == "__main__":
    export_films()
