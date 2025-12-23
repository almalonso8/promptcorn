from app.db.neo4j import run
from app.ingestion.wikidata_client import fetch_award_rows
from app.ingestion.wikidata_normalizer import normalize_awards


def get_all_movies() -> list[dict]:
    """
    Fetch all movies with TMDB ID and valid release year.

    Movies without a valid release year are skipped,
    because award eligibility year cannot be determined.
    """
    result = run(
        """
        MATCH (m:Movie)
        WHERE m.tmdb_id IS NOT NULL
          AND m.release_date IS NOT NULL
          AND m.release_date <> ""
          AND size(m.release_date) >= 4
        RETURN
          m.tmdb_id AS tmdb_id,
          substring(m.release_date, 0, 4) AS release_year
        """
    )

    movies = []
    for row in result:
        try:
            movies.append(
                {
                    "tmdb_id": row["tmdb_id"],
                    "release_year": int(row["release_year"]),
                }
            )
        except ValueError:
            # Defensive: skip malformed years
            continue

    return movies


def ingest_awards(movie: dict, normalized: dict) -> None:
    """
    Persist atomic award facts into Neo4j.

    Guarantees:
    - Event-scoped categories
    - No shared semantic leakage
    - Fully idempotent
    - Constraint-safe
    """
    tmdb_id = movie["tmdb_id"]
    year = movie["release_year"]

    for award in normalized["awards"]:
        run(
            """
            MATCH (m:Movie {tmdb_id: $tmdb_id})

            MERGE (e:AwardEvent {name: $event})
            SET e.source = "wikidata"

            MERGE (c:AwardCategory {
              name: $category,
              event: $event
            })
            SET c.source = "wikidata"

            MERGE (e)-[:HAS_CATEGORY]->(c)

            MERGE (m)-[r:RECEIVED {
              result: $result,
              year: $year
            }]->(c)
            """,
            {
                "tmdb_id": tmdb_id,
                "event": award["event"],
                "category": award["category"],
                "result": award["result"],
                "year": year,
            },
        )


def main() -> None:
    """
    Wikidata award enrichment.

    Safe to re-run.
    No inference.
    No language logic.
    """
    movies = get_all_movies()

    for idx, movie in enumerate(movies):
        rows = fetch_award_rows(movie["tmdb_id"])
        if not rows:
            continue

        normalized = normalize_awards(movie["tmdb_id"], rows)
        if not normalized["awards"]:
            continue

        ingest_awards(movie, normalized)

        if idx % 50 == 0:
            print(f"Processed {idx}/{len(movies)} movies")

    print("Award ingestion complete")


if __name__ == "__main__":
    main()
