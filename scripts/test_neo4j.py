from app.db.neo4j import run

result = run("RETURN 42 AS answer")
print(result)
