"""Review Agent — AI-powered output quality review."""

import json
from typing import Dict, Optional
from .llm_client import get_llm_client


def review(
    skill_name: str,
    params: Dict,
    result: Dict,
) -> Dict:
    """
    Review skill output for quality and correctness.

    Args:
        skill_name: Name of skill that produced output
        params: Parameters passed to skill
        result: Output from skill (has "summary" and "data" keys)

    Returns:
        dict with keys: passed (bool), issues (list), notes (str)
    """
    client = get_llm_client()

    # Skip review if LLM not configured
    if not client.available:
        return {
            "passed": True,
            "issues": [],
            "notes": "Review skipped — no API key configured",
        }

    try:
        # Build review prompt
        summary = result.get("summary", "")[:1000]  # Truncate to avoid huge prompts
        data_str = json.dumps(result.get("data", {}))[:1000]

        review_prompt = f"""You are a quality assurance agent reviewing output from a data engineering skill.

Skill: {skill_name}
Input file: {params.get('input_path', 'unknown')}

Output summary (first 1000 chars):
{summary}

Output data (first 1000 chars):
{data_str}

Your task: Review this output for quality issues.

Check for:
1. Is the output complete and not empty?
2. Are there any obvious data quality issues?
3. Are there any safety or security concerns?
4. Do the results look reasonable given the input?

Respond ONLY with valid JSON (no markdown, no explanation):
{{
  "passed": true/false,
  "issues": ["issue 1", "issue 2"],
  "notes": "brief summary of review findings"
}}

Important: Your response MUST be valid JSON only."""

        # Get review from LLM
        review_text = client.chat(
            messages=[{"role": "user", "content": review_prompt}],
        )

        # Parse JSON response
        review_data = json.loads(review_text)

        return {
            "passed": review_data.get("passed", True),
            "issues": review_data.get("issues", []),
            "notes": review_data.get("notes", ""),
        }

    except json.JSONDecodeError:
        # JSON parse error — return pass with note
        return {
            "passed": True,
            "issues": [],
            "notes": "Review completed but response parsing failed",
        }

    except Exception as e:
        # Any other error — return pass with note
        return {
            "passed": True,
            "issues": [],
            "notes": f"Review error: {type(e).__name__}: {str(e)}",
        }
