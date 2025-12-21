from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field

# Base Models
class NodeBase(BaseModel):
    id: Optional[str] = Field(None, description="Neo4j element ID")

class Person(NodeBase):
    name: str
    tmdb_id: Optional[int] = None

class Genre(NodeBase):
    name: str

class Keyword(NodeBase):
    name: str

class Country(NodeBase):
    iso_3166_1: str
    name: str

class Era(NodeBase):
    name: str
    start_year: int
    end_year: int

class Movie(NodeBase):
    title: str
    tmdb_id: int
    tagline: Optional[str] = None
    overview: Optional[str] = None
    release_date: Optional[date] = None
    runtime: Optional[int] = None
    poster_path: Optional[str] = None
    
    # Relationships (for API responses, not necessarily storage structure)
    genres: List[Genre] = []
    keywords: List[Keyword] = []
    countries: List[Country] = []
    actors: List[Person] = []
    directors: List[Person] = []
    era: Optional[Era] = None
