from app.ingestion.wikidata_client import fetch_award_rows
from app.ingestion.wikidata_normalizer import normalize_awards


def test_movie(tmdb_id: int, title: str):
    print(f"\nTesting: {title} (TMDB {tmdb_id})")

    rows = fetch_award_rows(tmdb_id)
    print(f"Raw rows: {len(rows)}")

    for row in rows:
        award_uri = row.get("award", {}).get("value")
        result = row.get("result", {}).get("value")
        if award_uri:
            print("  award:", award_uri.split("/")[-1], "| result:", result)

    normalized = normalize_awards(tmdb_id, rows)
    print("Normalized output:")
    print(normalized)


if __name__ == "__main__":
    # Known test cases
    test_movie(496243, "Parasite")
    test_movie(1417, "Pan’s Labyrinth")
    test_movie(557, "Roma")
