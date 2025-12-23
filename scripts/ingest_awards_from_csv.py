import csv
from app.db.neo4j import run

INPUT = "data/normalized/awards.csv"


def main():
    with open(INPUT) as f:
        reader = csv.DictReader(f)

        for row in reader:
            run(
                """
                MATCH (m:Movie {tmdb_id: $tmdb_id})

                MERGE (e:AwardEvent {name: $event})
                MERGE (c:AwardCategory {
                  name: $category,
                  event: $event
                })
                MERGE (e)-[:HAS_CATEGORY]->(c)

                MERGE (m)-[:RECEIVED {
                  result: $result,
                  year: $year
                }]->(c)
                """,
                {
                    "tmdb_id": int(row["tmdb_id"]),
                    "event": row["event"],
                    "category": row["category"],
                    "result": row["result"],
                    "year": int(row["year"]),
                },
            )

    print("Awards ingested from CSV")


if __name__ == "__main__":
    main()
