import pandas as pd
from app.db.neo4j import run

EMBEDDINGS = "data/embeddings/films_embeddings.parquet"


def main():
    df = pd.read_parquet(EMBEDDINGS)

    for row in df.itertuples(index=False):
        run(
            """
            MATCH (m:Movie {tmdb_id: $tmdb_id})
            SET m.embedding = $embedding
            """,
            {
                "tmdb_id": int(row.tmdb_id),
                "embedding": row.embedding,
            },
        )

    print(f"Loaded embeddings into Neo4j: {len(df)} movies")


if __name__ == "__main__":
    main()
