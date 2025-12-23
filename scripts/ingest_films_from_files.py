import pandas as pd
from app.db.neo4j import run

INPUT = "data/normalized/films_core.parquet"


def main():
    df = pd.read_parquet(INPUT)

    for row in df.itertuples(index=False):
        tmdb_id = int(row.tmdb_id)

        # --- Movie ---
        run(
            """
            MERGE (m:Movie {tmdb_id: $tmdb_id})
            SET
              m.title = $title,
              m.original_title = $original_title,
              m.overview = $overview,
              m.release_date = $release_date,
              m.poster_path = $poster_path
            """,
            {
                "tmdb_id": tmdb_id,
                "title": row.title,
                "original_title": row.original_title,
                "overview": row.overview,
                "release_date": row.release_date,
                "poster_path": row.poster_path,
            },
        )

        # --- Genres ---
        if row.genres is not None:
            for genre in list(row.genres):
                run(
                    """
                    MERGE (g:Genre {name: $name})
                    WITH g
                    MATCH (m:Movie {tmdb_id: $tmdb_id})
                    MERGE (m)-[:HAS_GENRE]->(g)
                    """,
                    {"name": genre, "tmdb_id": tmdb_id},
                )

        # --- Directors ---
        if row.directors is not None:
            for director in list(row.directors):
                run(
                    """
                    MERGE (p:Person {name: $name})
                    WITH p
                    MATCH (m:Movie {tmdb_id: $tmdb_id})
                    MERGE (p)-[:DIRECTED]->(m)
                    """,
                    {"name": director, "tmdb_id": tmdb_id},
                )

        # --- Actors ---
        if row.actors is not None:
            for actor in list(row.actors):
                run(
                    """
                    MERGE (p:Person {name: $name})
                    WITH p
                    MATCH (m:Movie {tmdb_id: $tmdb_id})
                    MERGE (p)-[:ACTED_IN]->(m)
                    """,
                    {"name": actor, "tmdb_id": tmdb_id},
                )

        # --- Keywords ---
        if row.keywords is not None:
            for keyword in list(row.keywords):
                run(
                    """
                    MERGE (k:Keyword {name: $name})
                    WITH k
                    MATCH (m:Movie {tmdb_id: $tmdb_id})
                    MERGE (m)-[:HAS_KEYWORD]->(k)
                    """,
                    {"name": keyword, "tmdb_id": tmdb_id},
                )

    print(f"Ingested {len(df)} movies from films_core.parquet")


if __name__ == "__main__":
    main()
