"""Agent state definition for Data Genie."""

from typing import TypedDict, Optional, List


class AgentState(TypedDict):
    """Shared state across LangGraph nodes."""

    # User inputs
    user_message: str
    input_path: str
    output_path: str

    # Phase 1 — routing
    candidate_skills: List[dict]  # top 3 matches from selector
    selected_skill: Optional[dict]
    skill_params: Optional[dict]

    # Phase 2 — execution
    progress_log: List[str]  # streamed live to Streamlit
    raw_result: Optional[dict]

    # Phase 3 — review
    review_passed: bool
    review_notes: Optional[str]

    # Final outputs
    output_files: List[str]
    summary_md: Optional[str]

    # User interaction
    approved: bool
    feedback: Optional[str]
    iteration: int  # max 3 rejection loops
    error: Optional[str]
