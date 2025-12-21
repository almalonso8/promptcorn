# Promptcorn Graph Domain Model

This file defines the canonical domain model.
If it is not represented here, it does not exist.

## Nodes

- Movie
- Person
- Genre
- Keyword
- Country
- Era (derived)

## Relationships

- (Person)-[:ACTED_IN]->(Movie)
- (Person)-[:DIRECTED]->(Movie)
- (Movie)-[:HAS_GENRE]->(Genre)
- (Movie)-[:HAS_KEYWORD]->(Keyword)
- (Movie)-[:PRODUCED_IN]->(Country)
- (Movie)-[:IN_ERA]->(Era)

## Rules

- All writes go through ingestion
- Queries are read-only
- Embeddings never replace structure
