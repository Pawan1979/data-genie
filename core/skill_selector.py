"""Skill selector — matches user intent to available skills."""

from typing import List, Dict
from .skill_registry import load_registry


def select_skills(user_message: str, top_k: int = 3) -> List[Dict]:
    """
    Select top K skills matching user message intent.

    Args:
        user_message: Natural language request from user
        top_k: Number of top matches to return

    Returns:
        List of skill dicts with match_score, sorted descending by score
        Empty list if no matches found (score < 20)
    """
    registry = load_registry()

    if not registry:
        return []

    # Normalize message
    message_lower = user_message.lower()
    message_words = set(message_lower.split())

    # Score each skill
    scored_skills = []

    for skill_name, skill_meta in registry.items():
        score = _score_skill(message_lower, message_words, skill_meta)

        if score >= 1:  # Minimum threshold (any match counts)
            scored_skills.append({
                "name": skill_name,
                "description": skill_meta.get("description", ""),
                "match_score": score,
                "keywords_matched": skill_meta.get("intent_keywords", []),
                "entry_module": skill_meta.get("entry_module"),
                "schema_path": skill_meta.get("schema_path"),
            })

    # Sort by score descending
    scored_skills.sort(key=lambda x: x["match_score"], reverse=True)

    return scored_skills[:top_k]


def _score_skill(message_lower: str, message_words: set, skill_meta: Dict) -> float:
    """Score a skill against user message."""
    score = 0.0

    # Keywords scoring
    keywords = [kw.lower() for kw in skill_meta.get("intent_keywords", [])]
    keyword_matches = 0
    exact_keyword_matches = 0

    for keyword in keywords:
        # Check if keyword appears in message
        if keyword in message_lower:
            keyword_matches += 1
            # Bonus for exact word match (not substring)
            if keyword in message_words:
                exact_keyword_matches += 1

    # Scoring: exact matches worth 3x, substring matches worth 1x
    score += exact_keyword_matches * 3
    score += (keyword_matches - exact_keyword_matches) * 1

    # Description/when_to_use scoring
    when_to_use = skill_meta.get("when_to_use", "").lower()
    description = skill_meta.get("description", "").lower()

    # Count word overlaps in when_to_use
    when_words = set(when_to_use.split())
    description_matches = len(message_words & when_words)
    score += description_matches * 0.5

    return score
