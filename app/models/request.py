from pydantic import BaseModel, Field
from typing import Optional

class RecommendationRequest(BaseModel):
    query: str = Field(..., example="funny recent movie that won an Oscar")
    limit: int = Field(default=5, ge=1, le=20)
    debug: bool = Field(default=False)
