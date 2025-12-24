from openai import OpenAI

client = OpenAI()

def embed_query(text: str) -> list[float]:
    return client.embeddings.create(
        model="text-embedding-3-large",
        input=text,
    ).data[0].embedding
