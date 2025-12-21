import asyncio

from app.db.neo4j import run
from app.ingestion.tmdb import TMDBClient


async def ingest_movie(movie: dict):
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

    for genre in movie.get("genres", []):
        run(
            """
            MERGE (g:Genre {name: $name})
            WITH g
            MATCH (m:Movie {tmdb_id: $tmdb_id})
            MERGE (m)-[:HAS_GENRE]->(g)
            """,
            {"name": genre["name"], "tmdb_id": movie["id"]},
        )


async def main():
    client = TMDBClient()
    pages = 3

    for page in range(1, pages + 1):
        popular = await client.get_popular_movies(page)

        for movie in popular["results"]:
            full = await client.get_movie_details(movie["id"])
            await ingest_movie(full)

    print("TMDB ingestion complete")


if __name__ == "__main__":
    asyncio.run(main())
