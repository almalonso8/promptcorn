import json
import pandas as pd
from app.ingestion.wikidata_client import fetch_award_rows

INPUT = "data/normalized/films_core.parquet"
OUTPUT = "data/raw/wikidata_awards.jsonl"


def main():
    df = pd.read_parquet(INPUT)
    tmdb_ids = df["tmdb_id"].dropna().astype(int).tolist()

    with open(OUTPUT, "w") as f:
        for idx, tmdb_id in enumerate(tmdb_ids):
            rows = fetch_award_rows(tmdb_id)
            if not rows:
                continue

            record = {
                "tmdb_id": tmdb_id,
                "rows": rows,
            }
            f.write(json.dumps(record) + "\n")

            if idx % 50 == 0:
                print(f"Processed {idx}/{len(tmdb_ids)}")

    print("Wikidata extraction complete")


if __name__ == "__main__":
    main()
