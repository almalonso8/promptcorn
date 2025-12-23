import csv
import json
from app.ingestion.wikidata_normalizer import normalize_awards
from app.db.neo4j import run

INPUT = "data/raw/wikidata_awards.jsonl"
OUTPUT = "data/normalized/awards.csv"


def get_release_year_map() -> dict[int, int]:
    rows = run(
        """
        MATCH (m:Movie)
        WHERE m.tmdb_id IS NOT NULL
          AND m.release_date IS NOT NULL
          AND m.release_date <> ""
        RETURN
          m.tmdb_id AS tmdb_id,
          substring(m.release_date, 0, 4) AS year
        """
    )
    return {
        r["tmdb_id"]: int(r["year"])
        for r in rows
        if r["year"].isdigit()
    }


def main():
    release_years = get_release_year_map()

    with open(INPUT) as fin, open(OUTPUT, "w", newline="") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=["tmdb_id", "event", "category", "result", "year", "source"],
        )
        writer.writeheader()

        for line in fin:
            record = json.loads(line)
            tmdb_id = record["tmdb_id"]

            if tmdb_id not in release_years:
                continue

            normalized = normalize_awards(tmdb_id, record["rows"])

            for award in normalized["awards"]:
                writer.writerow(
                    {
                        "tmdb_id": tmdb_id,
                        "event": award["event"],
                        "category": award["category"],
                        "result": award["result"],
                        "year": release_years[tmdb_id],
                        "source": "wikidata",
                    }
                )

    print("Awards normalization complete")


if __name__ == "__main__":
    main()
