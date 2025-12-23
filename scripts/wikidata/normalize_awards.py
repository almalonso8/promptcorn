import csv
import json
import pandas as pd
from app.ingestion.wikidata_normalizer import normalize_awards

RAW = "data/raw/wikidata_awards.jsonl"
FILMS = "data/normalized/films_core.parquet"
OUT = "data/normalized/awards.csv"


def main():
    films = pd.read_parquet(FILMS)
    release_year = {
        int(row.tmdb_id): int(row.release_date[:4])
        for row in films.itertuples()
        if isinstance(row.release_date, str) and len(row.release_date) >= 4
    }

    with open(RAW) as fin, open(OUT, "w", newline="") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=[
                "tmdb_id",
                "event",
                "category",
                "result",
                "year",
                "source",
            ],
        )
        writer.writeheader()

        for line in fin:
            record = json.loads(line)
            tmdb_id = record["tmdb_id"]

            if tmdb_id not in release_year:
                continue

            normalized = normalize_awards(tmdb_id, record["rows"])

            for award in normalized["awards"]:
                writer.writerow(
                    {
                        "tmdb_id": tmdb_id,
                        "event": award["event"],
                        "category": award["category"],
                        "result": award["result"],
                        "year": release_year[tmdb_id],
                        "source": "wikidata",
                    }
                )

    print(f"Awards normalized → {OUT}")


if __name__ == "__main__":
    main()
