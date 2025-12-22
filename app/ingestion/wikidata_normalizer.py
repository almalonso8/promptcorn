from app.ingestion.wikidata_mappings import AWARD_CATEGORIES


def extract_year(row: dict) -> int | None:
    """
    Extract award year from Wikidata qualifiers.

    We only accept P585 (point in time).
    """
    time_value = row.get("time", {}).get("value")
    if not time_value:
        return None

    # Wikidata time format: "+2020-01-01T00:00:00Z"
    try:
        return int(time_value[1:5])
    except Exception:
        return None


from app.ingestion.wikidata_mappings import AWARD_CATEGORIES


def normalize_awards(tmdb_id: int, rows: list[dict]) -> dict:
    """
    Normalize Wikidata award rows into atomic award facts.

    IMPORTANT:
    - Year is NOT extracted here
    - Year will be derived from Movie.release_date during ingestion
    """

    normalized = []

    for row in rows:
        award_uri = row.get("award", {}).get("value")
        result = row.get("result", {}).get("value")

        if not award_uri or not result:
            continue

        award_id = award_uri.split("/")[-1]
        if award_id not in AWARD_CATEGORIES:
            continue

        cfg = AWARD_CATEGORIES[award_id]

        normalized.append(
            {
                "event": cfg["event"],
                "category": cfg["category"],
                "result": result,
            }
        )

    return {
        "tmdb_id": tmdb_id,
        "awards": normalized,
    }
