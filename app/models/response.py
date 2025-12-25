from pydantic import BaseModel
from typing import List, Optional

class QueryFilters(BaseModel):
    year_from: Optional[int] = None
    award_event: Optional[str] = None
    award_result: Optional[str] = None

class ParsedQuery(BaseModel):
    semantic_query: str
    filters: Optional[QueryFilters] = None

class MovieRecommendation(BaseModel):
    tmdb_id: int
    title: str
    original_title: str
    release_year: Optional[int]
    final_score: float
    similarity_score: float
    recency_boost: float
    award_boost: float
    comedy_boost: float
    awards: List[str] = []

class RecommendationResponse(BaseModel):
    parsed_query: ParsedQuery
    debug: Optional[dict] = None
    results: List[MovieRecommendation]
