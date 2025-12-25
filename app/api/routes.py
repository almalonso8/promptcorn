from fastapi import APIRouter, Depends
from app.models.request import RecommendationRequest
from app.models.response import RecommendationResponse
from app.services.query_understanding import QueryUnderstandingService
from app.services.embeddings import EmbeddingService
from app.services.recommender import RecommenderService
from app.services.embeddings import EmbeddingService

router = APIRouter()

# Dependency injection
def get_recommender():
    embedding_service = EmbeddingService()
    return RecommenderService(embedding_service)

@router.get("/health")
def health_check():
    return {"status": "ok"}

@router.post("/recommend", response_model=RecommendationResponse)
def recommend(request: RecommendationRequest, service: RecommenderService = Depends(get_recommender)):
    # 1. Parse Query
    parsed_query = QueryUnderstandingService.parse(request.query)
    
    # 2. Get Recommendations
    results, debug_info = service.recommend(parsed_query, limit=request.limit, debug=request.debug)
    
    return RecommendationResponse(
        parsed_query=parsed_query,
        debug=debug_info,
        results=results
    )
