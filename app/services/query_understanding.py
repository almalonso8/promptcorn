import re
from datetime import datetime
from typing import Optional, Tuple
from app.models.response import ParsedQuery, QueryFilters

class QueryUnderstandingService:
    @staticmethod
    def parse(query: str) -> ParsedQuery:
        current_year = datetime.now().year
        filters = QueryFilters()
        
        # 1. Temporal extraction
        semantic_query = query.lower()
        
        # "last X years" OR "from X years ago"
        temporal_match = re.search(r"(?:last|from)\s+(\d+)\s+years(?:\s+ago)?", semantic_query)
        if temporal_match:
            years = int(temporal_match.group(1))
            filters.year_from = current_year - years
            semantic_query = semantic_query.replace(temporal_match.group(0), "")
        else:
            # "recent"
            if "recent" in semantic_query:
                filters.year_from = current_year - 10
                semantic_query = semantic_query.replace("recent", "")
        
        # 2. Award extraction
        AWARD_EVENT_MAP = {
            "oscar": "Academy Awards",
            "bafta": "British Academy Film Awards",
            "goya": "Goya Awards"
        }
        
        for kw, event_name in AWARD_EVENT_MAP.items():
            if kw in semantic_query:
                filters.award_event = event_name
                semantic_query = semantic_query.replace(kw, "")
                break
                
        # 3. Award result extraction
        if "winner" in semantic_query or "won" in semantic_query:
            filters.award_result = "won"
            semantic_query = semantic_query.replace("winner", "").replace("won", "")
        elif "nominee" in semantic_query or "nominated" in semantic_query:
            filters.award_result = "nominated"
            semantic_query = semantic_query.replace("nominee", "").replace("nominated", "")
            
        # 4. Clean up semantic query
        # Remove extra spaces, filler words, and numeric year hints
        semantic_query = re.sub(r"\b(that|which|a|an|the|movie|film|movies|films|with|from|in|about)\b", "", semantic_query)
        semantic_query = re.sub(r"\d{4}", "", semantic_query) # Strip year-like strings
        semantic_query = " ".join(semantic_query.split()).strip()
        
        # Check if filters are empty
        has_filters = filters.year_from or filters.award_event or filters.award_result
        
        return ParsedQuery(
            semantic_query=semantic_query or query, # Fallback to original if empty
            filters=filters if has_filters else None
        )
