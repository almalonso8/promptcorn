from app.recsys.query_embedding import embed_query
from app.recsys.retrieve import retrieve_candidates
from app.recsys.reason import rerank

def recommend(
    query: str,
    limit: int = 5,
    must_have_oscar: bool = False,
):
    embedding = embed_query(query)

    # Retrieval = recall, not taste
    candidates = retrieve_candidates(
        embedding=embedding,
        limit=50,
        must_have_oscar=must_have_oscar,
        genre=None,
        language=None,
    )

    return rerank(candidates, limit=limit)
