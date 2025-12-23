import pandas as pd

FILMS = "data/normalized/films_core.parquet"
AWARDS = "data/normalized/awards.csv"
OUT = "data/normalized/embedding_input.parquet"


def build_embedding_text(film: dict, awards: list[dict]) -> str:
    lines = []

    # --- Title ---
    lines.append(f"Title: {film['title']}")

    if (
        film.get("original_title")
        and film["original_title"] != film["title"]
    ):
        lines.append(f"Original title: {film['original_title']}")

    # --- Overview ---
    if film.get("overview"):
        lines.append("\nOverview:")
        lines.append(film["overview"])

    # --- Genres ---
    genres = film.get("genres")
    if genres is not None and len(genres) > 0:
        lines.append(f"\nGenres: {', '.join(list(genres))}")

    # --- Keywords ---
    keywords = film.get("keywords")
    if keywords is not None and len(keywords) > 0:
        lines.append(f"\nKeywords: {', '.join(list(keywords))}")

    # --- Directors ---
    directors = film.get("directors")
    if directors is not None and len(directors) > 0:
        lines.append(f"\nDirected by: {', '.join(list(directors))}")

    # --- Awards ---
    winners = [a for a in awards if a["result"] == "won"]
    nominees = [a for a in awards if a["result"] == "nominated"]

    if winners or nominees:
        lines.append("\nAwards:")

        for a in winners:
            lines.append(f"- {a['event']} – {a['category']} (winner)")

        for a in nominees:
            lines.append(f"- {a['event']} – {a['category']} (nominated)")

    return "\n".join(lines)


def main():
    films_df = pd.read_parquet(FILMS)
    awards_df = pd.read_csv(AWARDS)

    awards_by_tmdb = {
        int(k): v.to_dict(orient="records")
        for k, v in awards_df.groupby("tmdb_id")
    }

    rows = []

    for film in films_df.to_dict(orient="records"):
        tmdb_id = int(film["tmdb_id"])
        awards = awards_by_tmdb.get(tmdb_id, [])

        text = build_embedding_text(film, awards)

        rows.append(
            {
                "tmdb_id": tmdb_id,
                "title": film["title"],
                "embedding_text": text,
            }
        )

    out_df = pd.DataFrame(rows)
    out_df.to_parquet(OUT, index=False)

    print(f"Embedding input built: {len(out_df)} rows → {OUT}")


if __name__ == "__main__":
    main()
