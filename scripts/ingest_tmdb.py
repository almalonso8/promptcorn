import asyncio
from typing import Set

from app.db.neo4j import run
from app.ingestion.tmdb import TMDBClient


# -------------------------
# TARGETS (v1 corpus)
# -------------------------

TARGET_TOTAL = 5000
TARGET_SPANISH = 1200          # ~24% ES films
MAX_PAGES_PER_SOURCE = 200     # hard safety cap


# -------------------------
# EXISTING MOVIES
# -------------------------

def get_existing_tmdb_ids() -> Set[int]:
    """
    Load all TMDB IDs already present in Neo4j.

    This is the ONLY mechanism used to:
    - avoid duplicate ingestion
    - avoid re-fetching TMDB data
    """
    rows = run(
        """
        MATCH (m:Movie)
        RETURN m.tmdb_id AS tmdb_id
        """
    )
    return {row["tmdb_id"] for row in rows}


# -------------------------
# INGESTION HELPERS
# -------------------------

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
    """Ingest actors and directors."""
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
    """Ingest semantic keywords."""
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


# -------------------------
# STREAM INGESTION
# -------------------------

async def ingest_stream(
    label: str,
    fetch_fn,
    client: TMDBClient,
    existing_ids: Set[int],
    quota: int,
) -> int:
    """
    Generic ingestion stream with:
    - deduplication via Neo4j state
    - quota-based stopping
    """
    ingested = 0

    for page in range(1, MAX_PAGES_PER_SOURCE + 1):
        if ingested >= quota:
            break

        data = await fetch_fn(page)
        movies = data.get("results", [])

        for movie in movies:
            movie_id = movie["id"]

            if movie_id in existing_ids:
                continue

            full_movie = await client.get_movie_details(movie_id)
            await ingest_movie(full_movie)

            credits = await client.get_movie_credits(movie_id)
            await ingest_credits(movie_id, credits)

            keywords = await client.get_movie_keywords(movie_id)
            await ingest_keywords(movie_id, keywords)

            existing_ids.add(movie_id)
            ingested += 1

            if ingested >= quota:
                break

        if page % 10 == 0:
            print(f"[{label}] page {page} → ingested {ingested}")

    print(f"[{label}] completed → {ingested}")
    return ingested


# -------------------------
# MAIN
# -------------------------

async def main() -> None:
    client = TMDBClient()
    existing_ids = get_existing_tmdb_ids()

    print(f"Already ingested: {len(existing_ids)} movies")

    remaining = TARGET_TOTAL - len(existing_ids)
    if remaining <= 0:
        print("Target corpus already reached.")
        return

    # ---- Stream 1: Spanish films (explicit)
    spanish_quota = min(TARGET_SPANISH, remaining)

    async def fetch_spanish(page: int):
        return await client.discover_movies(
            page=page,
            language="es",
            region="ES",
            sort_by="popularity.desc",
        )

    ingested_es = await ingest_stream(
        label="SPANISH",
        fetch_fn=fetch_spanish,
        client=client,
        existing_ids=existing_ids,
        quota=spanish_quota,
    )

    remaining -= ingested_es

    # ---- Stream 2: Global popular
    async def fetch_popular(page: int):
        return await client.get_popular_movies(page)

    await ingest_stream(
        label="GLOBAL",
        fetch_fn=fetch_popular,
        client=client,
        existing_ids=existing_ids,
        quota=remaining,
    )

    print("TMDB ingestion complete")


if __name__ == "__main__":
    asyncio.run(main())
