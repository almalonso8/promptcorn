import time
import requests

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

HEADERS = {
    # REQUIRED by Wikimedia User-Agent policy
    # Include project name + contact URL or email
    "User-Agent": "Promptcorn/0.1 (https://github.com/your-org/promptcorn)",
    "Accept": "application/sparql+json",
}

SPARQL_QUERY = """
SELECT
  ?award
  ?result
  ?time
WHERE {
  ?movie wdt:P4947 "{{TMDB_ID}}".

  {
    ?movie p:P166 ?statement.
    ?statement ps:P166 ?award.
    OPTIONAL { ?statement pq:P585 ?time. }
    BIND("won" AS ?result)
  }
  UNION
  {
    ?movie p:P1411 ?statement.
    ?statement ps:P1411 ?award.
    OPTIONAL { ?statement pq:P585 ?time. }
    BIND("nominated" AS ?result)
  }
}
"""



import time
import requests


WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

HEADERS = {
    "Accept": "application/sparql+json",
    "User-Agent": "Promptcorn/0.1 (https://github.com/your-org/promptcorn; contact@promptcorn.dev)",
}


def fetch_award_rows(tmdb_id: int, retries: int = 3, timeout: int = 30) -> list[dict]:
    """
    Fetch award-related statements for a movie from Wikidata.

    Network guarantees:
    - Retries on timeout and transient failures
    - Backoff between retries
    - Never crashes the ingestion pipeline
    """
    query = SPARQL_QUERY.replace("{{TMDB_ID}}", str(tmdb_id))

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                WIKIDATA_ENDPOINT,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=timeout,
            )
            response.raise_for_status()

            data = response.json()
            return data["results"]["bindings"]

        except requests.exceptions.ReadTimeout:
            if attempt == retries:
                print(f"[Wikidata] Timeout for TMDB {tmdb_id} — skipping")
                return []
            time.sleep(attempt * 2)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status >= 500 and attempt < retries:
                time.sleep(attempt * 2)
                continue
            print(f"[Wikidata] HTTP {status} for TMDB {tmdb_id} — skipping")
            return []

        except requests.exceptions.RequestException as e:
            print(f"[Wikidata] Request error for TMDB {tmdb_id}: {e}")
            return []

    return []

