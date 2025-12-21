import requests

WIKI_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"


def get_movie_summary(title: str) -> dict | None:
    """
    Fetches the Wikipedia summary for a movie by title.
    We use this only as an entry point, not for embeddings.
    """
    url = f"{WIKI_API}{title.replace(' ', '_')}"
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        return None

    return response.json()
