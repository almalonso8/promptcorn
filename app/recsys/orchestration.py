from app.recsys.query_embedding import embed_query
from app.recsys.retrieve import retrieve_candidates
from app.recsys.reason import rerank

from app.recsys.temporal import extract_temporal_constraint

def recommend(
    query: str,
    limit: int = 5,
    must_have_oscar: bool = False,
):
    embedding = embed_query(query)
    
    # Extract temporal intent
    min_year, max_year = extract_temporal_constraint(query)

    # Retrieval = recall, not taste
    candidates = retrieve_candidates(
        embedding=embedding,
        limit=50,
        must_have_oscar=must_have_oscar,
        genre=None,
        language=None,
        min_year=min_year,
        max_year=max_year,
    )

    return rerank(candidates, limit=limit)
