import asyncio

from app.db.neo4j import run
from app.ingestion.tmdb import TMDBClient


async def ingest_movie(movie: dict) -> None:
    """
    Ingests a single movie node and its genre relationships.

    Idempotent by design:
    - Movies are uniquely identified by tmdb_id
    - Re-running ingestion updates metadata but never duplicates nodes
    """
    run(
        """
        MERGE (m:Movie {tmdb_id: $tmdb_id})
        SET
            m.title = $title,
            m.original_title = $original_title,
            m.overview = $overview,
            m.release_date = $release_date,
            m.vote_average = $vote_average,
            m.popularity = $popularity
        """,
        {
            "tmdb_id": movie["id"],
            "title": movie["title"],
            "original_title": movie["original_title"],
            "overview": movie["overview"],
            "release_date": movie["release_date"],
            "vote_average": movie["vote_average"],
            "popularity": movie["popularity"],
        },
    )

    # Normalize genres and connect them to the movie
    for genre in movie.get("genres", []):
        run(
            """
            MERGE (g:Genre {name: $name})
            WITH g
            MATCH (m:Movie {tmdb_id: $tmdb_id})
            MERGE (m)-[:HAS_GENRE]->(g)
            """,
            {
                "name": genre["name"],
                "tmdb_id": movie["id"],
            },
        )


async def ingest_credits(movie_id: int, credits: dict) -> None:
    """
    Ingests people and their relationships to a movie.

    Modeling rules:
    - All humans are (:Person)
    - Roles are expressed via relationships
    """

    # Cast → ACTED_IN
    for cast in credits.get("cast", []):
        run(
            """
            MERGE (p:Person {tmdb_id: $id})
            SET p.name = $name
            WITH p
            MATCH (m:Movie {tmdb_id: $movie_id})
            MERGE (p)-[:ACTED_IN]->(m)
            """,
            {
                "id": cast["id"],
                "name": cast["name"],
                "movie_id": movie_id,
            },
        )

    # Crew → DIRECTED (only directors in v1)
    for crew in credits.get("crew", []):
        if crew.get("job") == "Director":
            run(
                """
                MERGE (p:Person {tmdb_id: $id})
                SET p.name = $name
                WITH p
                MATCH (m:Movie {tmdb_id: $movie_id})
                MERGE (p)-[:DIRECTED]->(m)
                """,
                {
                    "id": crew["id"],
                    "name": crew["name"],
                    "movie_id": movie_id,
                },
            )


async def ingest_keywords(movie_id: int, keywords_payload: dict) -> None:
    """
    Ingests keyword nodes and connects them to a movie.

    Keywords provide semantic resolution finer than genres,
    but are still symbolic (pre-embedding).
    """
    for kw in keywords_payload.get("keywords", []):
        run(
            """
            MERGE (k:Keyword {name: $name})
            WITH k
            MATCH (m:Movie {tmdb_id: $movie_id})
            MERGE (m)-[:HAS_KEYWORD]->(k)
            """,
            {
                "name": kw["name"],
                "movie_id": movie_id,
            },
        )


async def main() -> None:
    """
    Main TMDB ingestion entry point.

    Strategy:
    - Ingest from multiple TMDB sources to reduce bias
    - Rely on MERGE + tmdb_id for idempotency
    - Scale data volume BEFORE embeddings
    """
    client = TMDBClient()

    # ~20 movies per page
    # 25 pages × 3 sources ≈ 1,500 movies (with overlap)
    pages_per_source = 25

    sources = [
        ("popular", client.get_popular_movies),
        ("top_rated", client.get_top_rated_movies),
        ("trending", lambda page: client.get_trending_movies("week", page=page)),
    ]

    for source_name, fetch_fn in sources:
        print(f"Starting ingestion from source: {source_name}")

        for page in range(1, pages_per_source + 1):
            data = await fetch_fn(page)

            # TMDB endpoints are inconsistent in shape
            movies = data.get("results", [])

            for movie in movies:
                movie_id = movie["id"]

                # Fetch full movie payload (genres, overview, etc.)
                full_movie = await client.get_movie_details(movie_id)
                await ingest_movie(full_movie)

                # Credits → people + roles
                credits = await client.get_movie_credits(movie_id)
                await ingest_credits(movie_id, credits)

                # Keywords → semantic enrichment
                keywords = await client.get_movie_keywords(movie_id)
                await ingest_keywords(movie_id, keywords)

    print("TMDB ingestion complete (~1,500 movies)")


if __name__ == "__main__":
    asyncio.run(main())
