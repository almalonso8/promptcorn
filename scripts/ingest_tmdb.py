import asyncio

from app.db.neo4j import run
from app.ingestion.tmdb import TMDBClient


async def ingest_movie(movie: dict) -> None:
    """
    Ingests a single movie node and its genre relationships.

    This function is idempotent:
    - MERGE ensures no duplicate Movie nodes
    - Genres are normalized and reused
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

    Design choices:
    - All people are modeled as (:Person)
    - Roles are expressed via relationships:
        (:Person)-[:ACTED_IN]->(:Movie)
        (:Person)-[:DIRECTED]->(:Movie)
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

    # Crew → DIRECTED (only directors for now)
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


async def main() -> None:
    """
    Main ingestion entry point.

    Flow:
    - Iterate over a small number of TMDB 'popular' pages
    - For each movie:
        1. Fetch full movie details
        2. Ingest Movie + Genre structure
        3. Fetch credits
        4. Ingest People + relationships
    """
    client = TMDBClient()
    pages_to_ingest = 3  # keep this small and safe

    for page in range(1, pages_to_ingest + 1):
        popular = await client.get_popular_movies(page)

        # Iterate over movies in the current page
        for movie in popular["results"]:
            # Fetch full movie details (genres, metadata, etc.)
            full_movie = await client.get_movie_details(movie["id"])
            await ingest_movie(full_movie)

            # Fetch and ingest credits (actors + directors)
            credits = await client.get_movie_credits(movie["id"])
            await ingest_credits(movie["id"], credits)

    print("TMDB ingestion (movies + genres + people) complete")


if __name__ == "__main__":
    asyncio.run(main())
