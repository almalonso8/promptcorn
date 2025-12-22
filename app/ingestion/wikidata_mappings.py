"""
Canonical mapping of Wikidata award Q-IDs to Promptcorn award semantics.

Principles:
- Award Q-ID == category
- High-prestige, film-level awards only
- False negatives > false positives
"""

AWARD_CATEGORIES = {
    # ─────────────────────────
    # Academy Awards (Oscars)
    # ─────────────────────────
    "Q103360": {  # Academy Award for Best Picture
        "event": "Academy Awards",
        "awarding_body": "Academy of Motion Picture Arts and Sciences",
        "category": "Best Picture",
        "accept": ["won"],
    },

    # ─────────────────────────
    # Golden Globe Awards
    # ─────────────────────────
    "Q830079": {  # Golden Globe Award for Best Foreign Language Film
        "event": "Golden Globe Awards",
        "awarding_body": "Golden Globe Awards",
        "category": "Best Motion Picture – Non-English Language",
        "accept": ["won", "nominated"],
    },

    # ─────────────────────────
    # BAFTA Awards
    # ─────────────────────────
    "Q277751": {  # BAFTA Award for Best Film Not in the English Language
        "event": "BAFTA Awards",
        "awarding_body": "British Academy of Film and Television Arts",
        "category": "Best Film Not in the English Language",
        "accept": ["won", "nominated"],
    },

    # ─────────────────────────
    # Cannes Film Festival
    # ─────────────────────────
    "Q1063016": {  # Palme d'Or
        "event": "Cannes Film Festival",
        "awarding_body": "Cannes Film Festival",
        "category": "Palme d’Or",
        "accept": ["won"],
    },

    # ─────────────────────────
    # Venice Film Festival
    # ─────────────────────────
    "Q152150": {  # Golden Lion
        "event": "Venice Film Festival",
        "awarding_body": "Venice Film Festival",
        "category": "Golden Lion",
        "accept": ["won"],
    },

    # ─────────────────────────
    # Premios Goya (Spain)
    # ─────────────────────────
    "Q131520": {  # Goya Award for Best Film
        "event": "Premios Goya",
        "awarding_body": "Academy of Cinematographic Arts and Sciences of Spain",
        "category": "Best Film",
        "accept": ["won"],
    },
}
