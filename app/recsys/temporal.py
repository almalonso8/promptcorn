import re

# Deterministic reference year based on system time (2025)
CURRENT_YEAR = 2025

def extract_temporal_constraint(query: str) -> tuple[int | None, int | None]:
    """
    Extracts temporal constraints from a query.
    Returns (min_year, max_year).
    
    Rules:
    - Keywords like "classic" => no constraint (None, None)
    - "menos de X años" => min_year = CURRENT_YEAR - X
    - Default => (2000, None)
    """
    q_lower = query.lower()
    
    # 1. Classics override
    # "classic", "clásica", "antigua", "old movie"
    classic_keywords = ["classic", "clásica", "antigua", "old movie", "old school"]
    if any(w in q_lower for w in classic_keywords):
        return (None, None)
        
    # 2. Explicit relative time: "menos de X años" / "less than X years"
    # Regex captures the number
    match = re.search(r"(?:menos de|less than)\s+(\d+)\s+(?:años|years?)", q_lower)
    if match:
        years = int(match.group(1))
        return (CURRENT_YEAR - years, None)
        
    # 3. Default floor
    return (2000, None)
