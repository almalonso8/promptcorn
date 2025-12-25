from openai import OpenAI
from app.config import OPENAI_API_KEY, EMBEDDING_MODEL

class EmbeddingService:
    def __init__(self):
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.model = EMBEDDING_MODEL or "text-embedding-3-large"

    def embed_text(self, text: str) -> list[float]:
        """
        Converts text into a vector using OpenAI.
        """
        response = self.client.embeddings.create(
            input=[text],
            model=self.model
        )
        return response.data[0].embedding
