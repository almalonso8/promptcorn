import time
import pandas as pd
from tqdm import tqdm
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

INPUT = "data/normalized/embedding_input.parquet"
OUTPUT = "data/embeddings/films_embeddings.parquet"

MODEL = "text-embedding-3-large"
BATCH_SIZE = 100
SLEEP_SECONDS = 1.0  # safety margin


def main():
    client = OpenAI()

    df = pd.read_parquet(INPUT)

    # Resume support: if output exists, skip done rows
    try:
        existing = pd.read_parquet(OUTPUT)
        done_ids = set(existing["tmdb_id"])
        print(f"Resuming: {len(done_ids)} embeddings already generated")
    except FileNotFoundError:
        existing = None
        done_ids = set()

    rows = []
    texts = []
    ids = []

    for row in df.itertuples(index=False):
        if row.tmdb_id in done_ids:
            continue
        ids.append(row.tmdb_id)
        texts.append(row.embedding_text)

        if len(texts) == BATCH_SIZE:
            embeddings = client.embeddings.create(
                model=MODEL,
                input=texts,
            )

            for tmdb_id, emb in zip(ids, embeddings.data):
                rows.append(
                    {
                        "tmdb_id": tmdb_id,
                        "embedding": emb.embedding,
                        "model": MODEL,
                    }
                )

            texts.clear()
            ids.clear()
            time.sleep(SLEEP_SECONDS)

    # Final partial batch
    if texts:
        embeddings = client.embeddings.create(
            model=MODEL,
            input=texts,
        )
        for tmdb_id, emb in zip(ids, embeddings.data):
            rows.append(
                {
                    "tmdb_id": tmdb_id,
                    "embedding": emb.embedding,
                    "model": MODEL,
                }
            )

    new_df = pd.DataFrame(rows)

    if existing is not None:
        final_df = pd.concat([existing, new_df], ignore_index=True)
    else:
        final_df = new_df

    final_df.to_parquet(OUTPUT, index=False)

    print(f"Embeddings stored: {len(final_df)} → {OUTPUT}")


if __name__ == "__main__":
    main()
