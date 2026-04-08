"""Data Genie — Streamlit UI for skill-driven data engineering assistant."""

import streamlit as st
import os
from pathlib import Path
from agent.graph import build_graph
from agent.state import AgentState
from core.skill_registry import load_registry


# Page config
app_title = os.getenv("APP_TITLE", "Data Genie")

st.set_page_config(
    page_title=app_title,
    page_icon="🧞",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
    <style>
    .stMetric { margin-bottom: 10px; }
    .skill-card {
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 10px;
        background: #f9f9f9;
    }
    .workflow-step {
        padding: 12px;
        border-radius: 6px;
        text-align: center;
        font-weight: bold;
        margin: 5px 0;
    }
    .step-active { background: #90EE90; color: #000; }
    .step-pending { background: #E0E0E0; color: #666; }
    .step-complete { background: #87CEEB; color: #000; }
    .arrow { text-align: center; font-size: 20px; margin: 0; }
    </style>
""", unsafe_allow_html=True)


def init_session_state():
    """Initialize session state."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "graph" not in st.session_state:
        st.session_state.graph = build_graph()
    if "agent_state" not in st.session_state:
        st.session_state.agent_state = None
    if "awaiting_input" not in st.session_state:
        st.session_state.awaiting_input = None


def render_sidebar():
    """Render left sidebar with config."""
    with st.sidebar:
        st.title(f"🧞 {app_title}")
        st.write("Local-first skill-driven data engineering assistant")

        st.divider()

        # Input file path
        input_path = st.text_input(
            "📁 Input file path",
            value=st.session_state.get("input_path", ""),
            placeholder="C:/data/myfile.json",
            help="Path to your input file (required)"
        )
        st.session_state.input_path = input_path

        # Validate input file
        if input_path:
            if Path(input_path).exists():
                st.success("✓ File found")
            else:
                st.error("✗ File not found")

        # Output folder path
        output_path = st.text_input(
            "📁 Output folder",
            value=st.session_state.get("output_path", ""),
            placeholder="C:/results/",
            help="Folder to write output files (will be created)"
        )
        st.session_state.output_path = output_path

        if output_path:
            try:
                Path(output_path).mkdir(parents=True, exist_ok=True)
                st.success("✓ Folder ready")
            except Exception as e:
                st.error(f"✗ Cannot create folder: {e}")

        st.divider()

        # Skills info
        registry = load_registry()
        st.metric("Skills loaded", len(registry))

        # Display skill names
        if registry:
            st.write("**Available Skills:**")
            for skill_name in registry.keys():
                st.write(f"  • {skill_name}")

        st.divider()

      


def render_chat():
    """Render main chat area."""
    # Display message history
    for message in st.session_state.messages:
        role = message["role"]
        content = message["content"]

        if role == "user":
            with st.chat_message("user"):
                st.write(content)
        else:
            with st.chat_message("assistant"):
                if message.get("type") == "skill_selection":
                    render_skill_selection(message)
                elif message.get("type") == "confirmation":
                    render_confirmation(message)
                elif message.get("type") == "progress":
                    render_progress(message)
                elif message.get("type") == "result":
                    render_result(message)
                elif message.get("type") == "approval":
                    render_approval(message)
                else:
                    st.markdown(content)

    # User input
    user_input = st.chat_input("What would you like to do?")

    if user_input:
        # Validate required fields
        if not st.session_state.input_path:
            st.error("⚠ Please provide an input file path in the sidebar")
            return

        if not st.session_state.output_path:
            st.error("⚠ Please provide an output folder path in the sidebar")
            return

        # Add user message
        st.session_state.messages.append({
            "role": "user",
            "content": user_input,
        })

        # Initialize agent state
        st.session_state.agent_state = {
            "user_message": user_input,
            "input_path": st.session_state.input_path,
            "output_path": st.session_state.output_path,
            "candidate_skills": [],
            "selected_skill": None,
            "skill_params": None,
            "progress_log": [],
            "raw_result": None,
            "review_passed": True,
            "review_notes": None,
            "output_files": [],
            "summary_md": None,
            "approved": False,
            "feedback": None,
            "iteration": 0,
            "error": None,
        }

        # Run agent
        run_agent()
        st.rerun()


def run_agent():
    """Run agent pipeline — step 1: parse and select skills."""
    state = st.session_state.agent_state

    try:
        # Step 1: Parse input
        from agent.state import AgentState
        from core.skill_selector import select_skills

        # Validate input_path exists
        from pathlib import Path
        input_path = Path(state["input_path"]).resolve()
        if not input_path.exists():
            state["error"] = f"Input file not found: {input_path}"
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"## Error\n\n{state['error']}",
            })
            return

        # Validate output_path is writable
        output_path = Path(state["output_path"]).resolve()
        try:
            output_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            state["error"] = f"Cannot create output folder: {e}"
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"## Error\n\n{state['error']}",
            })
            return

        state["user_message"] = state["user_message"].strip()
        state["input_path"] = str(input_path)
        state["output_path"] = str(output_path)

        # Step 2: Select skills
        candidate_skills = select_skills(state["user_message"], top_k=3)

        if not candidate_skills:
            state["error"] = "No matching skills found for your request"
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"## Error\n\n{state['error']}",
            })
            return

        state["candidate_skills"] = candidate_skills

        # Show skill selection
        st.session_state.messages.append({
            "role": "assistant",
            "type": "skill_selection",
            "candidate_skills": state["candidate_skills"],
            "content": "I found these matching skills:",
        })

        st.session_state.awaiting_input = "skill_selection"

    except Exception as e:
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"## Error\n\n{str(e)}",
        })


def render_skill_selection(message):
    """Render skill selection UI."""
    st.write(message["content"])

    skills = message.get("candidate_skills", [])

    # Create options for radio selection (without match score)
    skill_options = [f"{i+1}. {skill['name']}" for i, skill in enumerate(skills)]

    # Radio selection
    selected_option = st.radio(
        "Select a skill:",
        options=skill_options,
        key="skill_selection_radio"
    )

    # Show skill details
    if selected_option:
        selected_idx = int(selected_option.split(".")[0]) - 1
        skill = skills[selected_idx]

        st.write(f"**Description:** {skill['description']}")

        if st.button("✓ Use this skill", use_container_width=True):
            st.session_state.agent_state["selected_skill"] = skill
            st.session_state.awaiting_input = None
            run_agent_from_confirmation()
            st.rerun()


def run_agent_from_confirmation():
    """Show execution confirmation."""
    state = st.session_state.agent_state

    if not state.get("selected_skill"):
        st.session_state.messages.append({
            "role": "assistant",
            "content": "## Error\n\nNo skill selected",
        })
        return

    # Build skill params from schema defaults
    skill_meta = state["selected_skill"]
    state["skill_params"] = {
        "input_path": state["input_path"],
        "output_path": state["output_path"],
    }

    st.session_state.messages.append({
        "role": "assistant",
        "type": "confirmation",
        "state": state,
        "content": "Here is my plan:",
    })

    st.session_state.awaiting_input = "confirmation"


def render_confirmation(message):
    """Render execution confirmation UI."""
    st.write(message["content"])

    state = message.get("state", {})
    skill = state.get("selected_skill", {})

    st.markdown(f"""
    **Skill:** {skill.get('name', 'unknown')}
    **Input:** {state.get('input_path', 'unknown')}
    **Output:** {state.get('output_path', 'unknown')}
    """)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("✓ Confirm and Run", use_container_width=True):
            st.session_state.agent_state["approved"] = True
            st.session_state.awaiting_input = None
            run_agent_from_execution()
            st.rerun()

    with col2:
        if st.button("✗ Cancel", use_container_width=True):
            st.session_state.messages.append({
                "role": "assistant",
                "content": "Execution cancelled.",
            })
            st.session_state.awaiting_input = None
            st.rerun()


def run_agent_from_execution():
    """Run skill execution and review."""
    from core.skill_executor import execute_skill
    from core.review_agent import review

    state = st.session_state.agent_state

    try:
        skill_meta = state["selected_skill"]
        params = state["skill_params"]

        # Append feedback if present
        if state.get("feedback"):
            params["feedback"] = state["feedback"]

        # Show progress placeholder
        progress_placeholder = st.empty()

        def progress_cb(msg: str):
            state["progress_log"].append(msg)
            # Update progress display
            with progress_placeholder.container():
                st.write("🔄 **Running...**")
                for log_msg in state["progress_log"][-10:]:  # Show last 10
                    st.write(f"  • {log_msg}")

        # Execute skill
        result = execute_skill(skill_meta, params, progress_callback=progress_cb)
        state["raw_result"] = result

        # Review output
        review_result = review(skill_meta["name"], params, result)
        state["review_passed"] = review_result.get("passed", True)
        state["review_notes"] = review_result.get("notes")

        # Generate response
        summary = result.get("summary", "")
        output_files = result.get("output_files", [])

        if not state.get("review_passed") and state.get("review_notes"):
            summary += f"\n\n⚠ **Review Note:** {state['review_notes']}"

        state["summary_md"] = summary
        state["output_files"] = output_files

        # Show result
        st.session_state.messages.append({
            "role": "assistant",
            "type": "result",
            "state": state,
            "content": "Execution complete!",
        })

        st.session_state.messages.append({
            "role": "assistant",
            "type": "approval",
            "state": state,
            "content": "Is this what you needed?",
        })

        st.session_state.awaiting_input = "approval"

    except Exception as e:
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"## Error\n\n{str(e)}",
        })


def render_progress(message):
    """Render progress display."""
    logs = message.get("progress_log", [])
    for log in logs:
        st.write(f"✓ {log}")


def render_result(message):
    """Render result with downloads."""
    state = message.get("state", {})
    summary = state.get("summary_md", "")
    files = state.get("output_files", [])

    if summary:
        st.markdown(summary)

    if files:
        st.write("### 📥 Download Results")
        for file_path in files:
            file_obj = Path(file_path)
            if file_obj.exists():
                with open(file_obj, "rb") as f:
                    st.download_button(
                        label=f"Download {file_obj.name}",
                        data=f,
                        file_name=file_obj.name,
                        mime="application/octet-stream",
                    )


def render_approval(message):
    """Render approval UI."""
    st.write(message["content"])

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("✓ Looks good, thanks!", use_container_width=True):
            st.session_state.agent_state["approved"] = True
            st.session_state.awaiting_input = None
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Great! All files saved to {st.session_state.output_path}",
            })
            st.rerun()

    with col2:
        if st.button("🔄 Try again", use_container_width=True):
            # Re-run skill with same params
            st.session_state.agent_state["iteration"] += 1
            st.session_state.awaiting_input = None
            run_agent_from_execution()
            st.rerun()

    with col3:
        feedback_text = st.text_input(
            "Not quite — tell me what to change:",
            key="feedback_input",
        )
        if feedback_text and st.button("Refine", use_container_width=True):
            st.session_state.agent_state["feedback"] = feedback_text
            st.session_state.agent_state["iteration"] += 1
            st.session_state.awaiting_input = None
            run_agent_from_execution()
            st.rerun()


def get_skill_tools(skill_name: str) -> list:
    """Get tools used by each skill."""
    skill_tools_map = {
        "ARM Template Analyser": [
            "arm_template_tool.py",
        ],
        "Data Discovery": [
            "discovery.py",
        ],
        "Data Validation": [
            "validator.py",
        ],
        "Data Copy Validator": [
            "data_copy.py",
        ],
        "Pandas to PySpark Converter": [
            "converter.py",
        ],
        "PySpark Migrator": [
            "migrator.py",
        ],
    }
    return skill_tools_map.get(skill_name, ["Custom Tool"])


def render_workflow_graph():
    """Render agent workflow visualization (vertical) with tools panel."""
    state = st.session_state.get("agent_state")

    if not state:
        return st.info("Start a task to see workflow")

    # Determine current step
    current_step = None
    if st.session_state.awaiting_input == "skill_selection":
        current_step = 0
    elif st.session_state.awaiting_input == "confirmation":
        current_step = 1
    elif state.get("selected_skill") and not state.get("raw_result"):
        current_step = 2
    elif state.get("raw_result") and not state.get("review_passed") is True:
        current_step = 3
    elif state.get("review_passed") is not None:
        current_step = 4

    # Define workflow steps
    steps = [
        ("🔍 Router Agent", "Identify skill\navailability"),
        ("👤 User Confirm", "Confirm\nselection"),
        ("⚙️ Executor Agent", "Run skill\n& tools"),
        ("🤖 Review Agent", "Verify output\nquality"),
        ("✅ Complete", "Output ready"),
    ]

    st.markdown("### 🔄 Workflow")

    for idx, (title, desc) in enumerate(steps):
        # Determine step status
        if current_step is None:
            status_class = "step-pending"
        elif idx < current_step:
            status_class = "step-complete"
        elif idx == current_step:
            status_class = "step-active"
        else:
            status_class = "step-pending"

        st.markdown(f'<div class="workflow-step {status_class}">{title}<br><small>{desc}</small></div>',
                   unsafe_allow_html=True)

        # Add arrow between steps
        if idx < len(steps) - 1:
            st.markdown('<div class="arrow">↓</div>', unsafe_allow_html=True)

    # Show selected skill and its tools
    st.divider()

    if state.get("selected_skill"):
        skill_name = state["selected_skill"].get("name", "Unknown")
        st.markdown(f"### 🎯 Selected Skill")
        st.write(f"**{skill_name}**")

        # Show tools being used
        tools = get_skill_tools(skill_name)
        st.markdown("**🛠️ Tools Used:**")
        for tool in tools:
            st.write(f"  • {tool}")

        # Show progress if executing
        if current_step == 2 and state.get("progress_log"):
            st.markdown("**📊 Progress:**")
            for log in state.get("progress_log", [])[-3:]:  # Show last 3 logs
                st.caption(f"✓ {log}")
    else:
        st.info("No skill selected yet")


def main():
    """Main app."""
    init_session_state()
    render_sidebar()

    # Create two-column layout: chat on left, workflow on right
    chat_col, workflow_col = st.columns([2, 1], gap="medium")

    with chat_col:
        render_chat()

    with workflow_col:
        render_workflow_graph()


if __name__ == "__main__":
    main()
