import asyncio
import logging
import os
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)


class TMDBClientError(Exception):
    pass


class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self):
        self.api_key = os.getenv("TMDB_API_KEY")
        self.access_token = os.getenv("TMDB_READ_ACCESS_TOKEN")

        self.headers = {"accept": "application/json"}
        if self.access_token:
            self.headers["Authorization"] = f"Bearer {self.access_token}"

        self.params = {}
        if not self.access_token and self.api_key:
            self.params["api_key"] = self.api_key

    async def _request(
        self, method: str, endpoint: str, params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        merged_params = {**self.params, **(params or {})}

        async with httpx.AsyncClient() as client:
            for attempt in range(3):
                try:
                    response = await client.request(
                        method,
                        url,
                        headers=self.headers,
                        params=merged_params,
                        timeout=10.0,
                    )
                    response.raise_for_status()
                    return response.json()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        retry_after = e.response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after else 1
                        logger.warning(f"Rate limited. Waiting {wait_time}s.")
                        await asyncio.sleep(wait_time)
                        continue
                    raise TMDBClientError(
                        f"TMDB API Error {e.response.status_code}: {e.response.text}"
                    )
                except httpx.RequestError as e:
                    if attempt == 2:
                        raise TMDBClientError(f"TMDB Connection Error: {e}")
                    await asyncio.sleep(1)

            raise TMDBClientError("Max retries exceeded")

    async def get_popular_movies(self, page: int = 1) -> Dict[str, Any]:
        return await self._request("GET", "/movie/popular", params={"page": page})

    async def get_top_rated_movies(self, page: int = 1) -> Dict[str, Any]:
        """
        Fetches top-rated movies from TMDB.

        This endpoint favors:
        - critically acclaimed films
        - classics
        - prestige cinema

        We use it to counterbalance popularity bias.
        """
        return await self._request(
            "GET",
            "/movie/top_rated",
            params={"page": page}
        )

    async def get_trending_movies(self, time_window: str = "day", page: int = 1) -> Dict[str, Any]:
        """
        Fetches trending movies.
        Time window can be 'day' or 'week'.
        """
        return await self._request("GET", f"/trending/movie/{time_window}", params={"page": page})


    async def get_movie_details(self, movie_id: int) -> Dict[str, Any]:
        return await self._request("GET", f"/movie/{movie_id}")
    
    async def get_movie_credits(self, movie_id: int) -> Dict[str, Any]:
        """
        Fetch cast and crew information for a movie.
        Used to create Person nodes and ACTED_IN / DIRECTED relationships.
        """
        return await self._request("GET", f"/movie/{movie_id}/credits")
    
    async def get_movie_keywords(self, movie_id: int) -> Dict[str, Any]:
        """
        Fetch keywords associated with a movie.
        Used for semantic enrichment without ML.
        """
        return await self._request("GET", f"/movie/{movie_id}/keywords")