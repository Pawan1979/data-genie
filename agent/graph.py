"""LangGraph state machine for Data Genie agent."""

from pathlib import Path
from typing import Dict, Optional
from langgraph.graph import StateGraph, END

from .state import AgentState
from core.skill_selector import select_skills
from core.skill_executor import execute_skill
from core.review_agent import review


def build_graph():
    """Build LangGraph state machine and compile it."""
    graph = StateGraph(AgentState)

    # Define nodes
    graph.add_node("parse_input", node_parse_input)
    graph.add_node("select_skills", node_select_skills)
    graph.add_node("await_skill_choice", node_await_skill_choice)
    graph.add_node("confirm_execution", node_confirm_execution)
    graph.add_node("execute_skill", node_execute_skill)
    graph.add_node("review_output", node_review_output)
    graph.add_node("generate_response", node_generate_response)
    graph.add_node("await_approval", node_await_approval)
    graph.add_node("handle_feedback", node_handle_feedback)

    # Define edges
    graph.add_edge("parse_input", "select_skills")
    graph.add_edge("select_skills", "await_skill_choice")
    graph.add_edge("await_skill_choice", "confirm_execution")
    graph.add_edge("confirm_execution", "execute_skill")
    graph.add_edge("execute_skill", "review_output")
    graph.add_edge("review_output", "generate_response")
    graph.add_edge("generate_response", "await_approval")
    graph.add_conditional_edges(
        "handle_feedback",
        route_feedback,
        {
            "execute_skill": "execute_skill",
            "end": END,
        }
    )
    graph.add_edge("await_approval", "handle_feedback")

    # Entry point
    graph.set_entry_point("parse_input")

    # Compile and return
    return graph.compile()


def node_parse_input(state: AgentState) -> AgentState:
    """Parse and validate user input."""
    # Validate input_path exists
    input_path = Path(state["input_path"]).resolve()
    if not input_path.exists():
        state["error"] = f"Input file not found: {input_path}"
        return state

    # Validate output_path is writable (create if needed)
    output_path = Path(state["output_path"]).resolve()
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        state["error"] = f"Cannot create output folder: {e}"
        return state

    # Normalize message
    state["user_message"] = state["user_message"].strip()
    state["input_path"] = str(input_path)
    state["output_path"] = str(output_path)
    state["iteration"] = 0
    state["progress_log"] = []
    state["candidate_skills"] = []
    state["raw_result"] = None
    state["review_passed"] = True
    state["review_notes"] = None
    state["output_files"] = []
    state["summary_md"] = None
    state["approved"] = False
    state["feedback"] = None
    state["error"] = None

    return state


def node_select_skills(state: AgentState) -> AgentState:
    """Select matching skills."""
    if state.get("error"):
        return state

    candidate_skills = select_skills(state["user_message"], top_k=3)

    if not candidate_skills:
        state["error"] = "No matching skills found for your request"
        return state

    state["candidate_skills"] = candidate_skills
    return state


def node_await_skill_choice(state: AgentState) -> AgentState:
    """INTERRUPT: Await user skill selection."""
    # Interrupt and wait for Streamlit to set state["selected_skill"]
    # This is handled by the Streamlit app calling graph.resume()
    return state


def node_confirm_execution(state: AgentState) -> AgentState:
    """INTERRUPT: Await execution confirmation."""
    if not state.get("selected_skill"):
        state["error"] = "No skill selected"
        return state

    # Build skill params from schema defaults
    skill_meta = state["selected_skill"]
    state["skill_params"] = {
        "input_path": state["input_path"],
        "output_path": state["output_path"],
    }

    # Interrupt and wait for Streamlit to confirm with state["approved"]
    return state


def node_execute_skill(state: AgentState) -> AgentState:
    """Execute selected skill."""
    if state.get("error") or not state.get("selected_skill"):
        return state

    skill_meta = state["selected_skill"]
    params = state["skill_params"]

    # Append feedback if present
    if state.get("feedback"):
        params["feedback"] = state["feedback"]

    # Define progress callback that updates state
    def progress_cb(msg: str):
        state["progress_log"].append(msg)

    # Execute skill
    result = execute_skill(skill_meta, params, progress_callback=progress_cb)

    state["raw_result"] = result
    return state


def node_review_output(state: AgentState) -> AgentState:
    """Review skill output."""
    if state.get("error") or not state.get("raw_result"):
        return state

    skill_meta = state["selected_skill"]
    result = state["raw_result"]

    review_result = review(skill_meta["name"], state["skill_params"], result)

    state["review_passed"] = review_result.get("passed", True)
    state["review_notes"] = review_result.get("notes")

    return state


def node_generate_response(state: AgentState) -> AgentState:
    """Generate final markdown response."""
    if state.get("error"):
        return state

    result = state.get("raw_result", {})
    summary = result.get("summary", "")
    output_files = result.get("output_files", [])

    # Append review notes if failed
    if not state.get("review_passed") and state.get("review_notes"):
        summary += f"\n\n⚠ **Review Note:** {state['review_notes']}"

    state["summary_md"] = summary
    state["output_files"] = output_files

    return state


def node_await_approval(state: AgentState) -> AgentState:
    """INTERRUPT: Await user approval or feedback."""
    # Interrupt and wait for Streamlit to set state["approved"] or state["feedback"]
    return state


def node_handle_feedback(state: AgentState) -> AgentState:
    """Handle user feedback or approval."""
    if state.get("approved"):
        # User approved — end
        return state

    if state.get("feedback") and state.get("iteration", 0) < 3:
        # User provided feedback — loop back to execute_skill
        state["iteration"] += 1
        return state

    # No feedback or max iterations reached — end
    state["approved"] = True
    return state


def route_feedback(state: AgentState) -> str:
    """Route based on feedback."""
    if state.get("approved"):
        return "end"
    if state.get("feedback") and state.get("iteration", 0) < 3:
        return "execute_skill"
    return "end"
