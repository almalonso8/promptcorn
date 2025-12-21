from neo4j import GraphDatabase
from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )
    return _driver


def run(query: str, params: dict | None = None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(query, params or {})
        return result.data()
