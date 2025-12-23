import json
from app.ingestion.wikidata_client import fetch_award_rows
from app.db.neo4j import run

OUTPUT = "data/raw/wikidata_awards.jsonl"


def get_tmdb_ids() -> list[int]:
    rows = run(
        """
        MATCH (m:Movie)
        WHERE m.tmdb_id IS NOT NULL
        RETURN DISTINCT m.tmdb_id AS tmdb_id
        """
    )
    return [r["tmdb_id"] for r in rows]


def main():
    tmdb_ids = get_tmdb_ids()

    with open(OUTPUT, "w") as f:
        for tmdb_id in tmdb_ids:
            rows = fetch_award_rows(tmdb_id)
            if not rows:
                continue

            record = {
                "tmdb_id": tmdb_id,
                "rows": rows,
            }
            f.write(json.dumps(record) + "\n")

    print("Wikidata extraction complete")


if __name__ == "__main__":
    main()
