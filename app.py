"""Data Genie — Streamlit UI for skill-driven data engineering assistant."""

import streamlit as st
import os
import uuid
import datetime
from pathlib import Path
from agent.graph import build_graph
from agent.state import AgentState
from core.skill_registry import load_registry
from core.batch_processor import BatchProcessor
from core.skill_executor import execute_skill


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
    if "batch_mode" not in st.session_state:
        st.session_state.batch_mode = False
    if "batch_file_pattern" not in st.session_state:
        st.session_state.batch_file_pattern = "*"
    if "batch_processing_mode" not in st.session_state:
        st.session_state.batch_processing_mode = "parallel"
    if "batch_max_workers" not in st.session_state:
        st.session_state.batch_max_workers = 4
    if "user_session_id" not in st.session_state:
        st.session_state.user_session_id = uuid.uuid4().hex[:8]
    if "session_start_time" not in st.session_state:
        st.session_state.session_start_time = datetime.datetime.now().isoformat()


def get_user_output_folder(base_output_path: str) -> str:
    """Generate user-specific output folder to prevent multi-user conflicts.

    Creates a folder structure like: output_path/user_id/timestamp/
    This ensures multiple concurrent users don't overwrite each other's results.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    user_id = st.session_state.user_session_id

    user_folder = Path(base_output_path) / user_id / timestamp
    return str(user_folder)


@st.cache_resource
def get_skill_registry():
    """Load skill registry with caching (thread-safe).

    Using @st.cache_resource ensures the registry is loaded once
    and reused across all users, preventing unnecessary file I/O
    and potential race conditions in multi-user scenarios.
    """
    return load_registry()


def render_sidebar():
    """Render left sidebar with config."""
    with st.sidebar:
        st.title(f"🧞 {app_title}")
        st.write("Local-first skill-driven data engineering assistant")

        st.divider()

        # Session info (for debugging multi-user scenarios)
        with st.expander("👤 Session Info"):
            st.caption(f"Session ID: `{st.session_state.user_session_id}`")
            st.caption(f"Started: {st.session_state.session_start_time}")

        st.divider()

        # Input type toggle
        input_type = st.radio(
            "📋 Processing Mode",
            options=["Single File", "Batch (Folder)"],
            key="input_type_radio",
            help="Choose single file or batch processing"
        )
        st.session_state.batch_mode = (input_type == "Batch (Folder)")

        st.divider()

        if not st.session_state.batch_mode:
            # Single file mode
            input_path = st.text_input(
                "📁 Input file path",
                value=st.session_state.get("input_path", ""),
                placeholder="C:/data/myfile.json",
                help="Path to your input file (required)",
                key="single_file_input"
            )
            st.session_state.input_path = input_path

            # Validate input file
            if input_path:
                if Path(input_path).exists():
                    st.success("✓ File found")
                else:
                    st.error("✗ File not found")
        else:
            # Batch mode
            input_path = st.text_input(
                "📁 Input folder",
                value=st.session_state.get("input_path", ""),
                placeholder="C:/data/",
                help="Path to folder containing input files (required)",
                key="batch_folder_input"
            )
            st.session_state.input_path = input_path

            # Validate input folder
            if input_path:
                input_folder = Path(input_path)
                if input_folder.exists() and input_folder.is_dir():
                    file_count = len(list(input_folder.glob("*")))
                    st.success(f"✓ Folder found ({file_count} items)")
                else:
                    st.error("✗ Folder not found or not a directory")

            # Batch-specific options
            st.markdown("**Batch Settings:**")

            file_pattern = st.text_input(
                "File Pattern",
                value=st.session_state.get("batch_file_pattern", "*"),
                placeholder="*.json, *.csv",
                help="Glob pattern for files to process (e.g., *.json, *.csv)",
                key="batch_file_pattern_input"
            )
            st.session_state.batch_file_pattern = file_pattern

            processing_mode = st.radio(
                "Processing Mode",
                options=["Parallel", "Threaded", "Sequential"],
                key="batch_mode_radio",
                help="Parallel: CPU-heavy tasks (4-8x faster). Threaded: I/O-bound (more efficient). Sequential: Debugging."
            )
            mode_map = {"Parallel": "parallel", "Threaded": "threaded", "Sequential": "sequential"}
            st.session_state.batch_processing_mode = mode_map[processing_mode]

            max_workers = st.slider(
                "Max Workers",
                min_value=1,
                max_value=16,
                value=st.session_state.get("batch_max_workers", 4),
                help="Number of parallel/threaded workers (auto-adjusted for multi-user)",
                key="batch_max_workers_slider"
            )
            st.session_state.batch_max_workers = max_workers

            if processing_mode == "Parallel":
                st.caption("⚡ Best for: ARM templates, PySpark migration, data validation")
            elif processing_mode == "Threaded":
                st.caption("🔄 Best for: Data discovery, CSV reading, data copy")
            else:
                st.caption("🐛 Best for: Debugging, small datasets")

        # Output folder path
        output_path = st.text_input(
            "📁 Output folder",
            value=st.session_state.get("output_path", ""),
            placeholder="C:/results/",
            help="Folder to write output files (will be created)",
            key="output_path_input"
        )
        st.session_state.output_path = output_path

        if output_path:
            try:
                Path(output_path).mkdir(parents=True, exist_ok=True)
                st.success("✓ Folder ready")
            except Exception as e:
                st.error(f"✗ Cannot create folder: {e}")

        st.divider()

        # Skills info (using cached registry)
        registry = get_skill_registry()
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
            input_type = "folder" if st.session_state.batch_mode else "file"
            st.error(f"⚠ Please provide an input {input_type} path in the sidebar")
            return

        if not st.session_state.output_path:
            st.error("⚠ Please provide an output folder path in the sidebar")
            return

        # Add user message
        st.session_state.messages.append({
            "role": "user",
            "content": user_input,
        })

        # Generate user-specific output folder to prevent multi-user conflicts
        user_output_path = get_user_output_folder(st.session_state.output_path)

        # Initialize agent state
        st.session_state.agent_state = {
            "user_message": user_input,
            "input_path": st.session_state.input_path,
            "output_path": user_output_path,  # User-specific, not shared
            "batch_mode": st.session_state.batch_mode,
            "batch_file_pattern": st.session_state.batch_file_pattern,
            "batch_processing_mode": st.session_state.batch_processing_mode,
            "batch_max_workers": st.session_state.batch_max_workers,
            "user_session_id": st.session_state.user_session_id,
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
        import shutil

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

        # Validate output_path is writable and has sufficient disk space
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

        # Check available disk space (warn if < 500MB)
        parent_folder = output_path.parent
        disk_stats = shutil.disk_usage(parent_folder)
        free_gb = disk_stats.free / (1024**3)

        if free_gb < 0.5:
            state["error"] = f"Insufficient disk space: {free_gb:.2f}GB available (need ≥0.5GB)"
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

        if st.button("✓ Use this skill", use_container_width=True, key="use_skill_button"):
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
    is_batch = state.get("batch_mode", False)

    input_label = "Input Folder" if is_batch else "Input File"
    batch_info = ""
    if is_batch:
        batch_info = f"\n**Pattern:** {state.get('batch_file_pattern', '*')}\n**Mode:** {state.get('batch_processing_mode', 'parallel').title()}\n**Workers:** {state.get('batch_max_workers', 4)}"

    st.markdown(f"""
    **Skill:** {skill.get('name', 'unknown')}
    **{input_label}:** {state.get('input_path', 'unknown')}
    **Output Base:** {state.get('output_path', 'unknown')}
    **Results Location:** `{state.get('output_path', 'unknown')}/[RUN_ID]/` (auto-generated run folder){batch_info}
    """)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("✓ Confirm and Run", use_container_width=True, key="confirm_run_button"):
            st.session_state.agent_state["approved"] = True
            st.session_state.awaiting_input = None
            run_agent_from_execution()
            st.rerun()

    with col2:
        if st.button("✗ Cancel", use_container_width=True, key="cancel_button"):
            st.session_state.messages.append({
                "role": "assistant",
                "content": "Execution cancelled.",
            })
            st.session_state.awaiting_input = None
            st.rerun()


def run_agent_from_execution():
    """Run skill execution and review."""
    from core.review_agent import review

    state = st.session_state.agent_state

    try:
        skill_meta = state["selected_skill"]
        params = state["skill_params"]

        # Generate run ID for this execution (each "Try again" gets a new run)
        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:20]
        state["run_id"] = run_id

        # Create run-specific output folder under base output path
        base_output_path = state["output_path"]
        run_output_path = str(Path(base_output_path) / run_id)
        Path(run_output_path).mkdir(parents=True, exist_ok=True)

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

        # Execute skill (single or batch)
        if state.get("batch_mode"):
            # Batch processing (no progress_cb - can't pickle for worker processes)
            processor = BatchProcessor(
                skill_executor=execute_skill,
                input_folder=state["input_path"],
                output_folder=run_output_path,
                progress_callback=None,  # Progress reported as files complete
                max_workers=state.get("batch_max_workers", 4),
                file_pattern=state.get("batch_file_pattern", "*"),
                batch_mode=state.get("batch_processing_mode", "parallel"),
            )
            result = processor.process_batch(skill_meta, params)
        else:
            # Single file processing
            params["output_path"] = run_output_path
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
    import hashlib

    state = message.get("state", {})
    summary = state.get("summary_md", "")
    files = state.get("output_files", [])
    run_id = state.get("run_id", "")

    # Display run ID
    if run_id:
        st.info(f"📋 **Run ID:** `{run_id}` — Results archived in `{run_id}/` subfolder")

    if summary:
        st.markdown(summary)

    if files:
        st.write("### Download Results")

        # Deduplicate files and read contents
        seen_paths = set()
        file_data = []

        for file_path in files:
            file_obj = Path(file_path)
            # Skip duplicates
            if str(file_obj) in seen_paths:
                continue
            seen_paths.add(str(file_obj))

            if file_obj.exists():
                try:
                    # Read file contents fully before creating button
                    with open(file_obj, "rb") as f:
                        file_contents = f.read()
                    file_data.append((file_obj, file_contents))
                except Exception as e:
                    st.warning(f"Could not read {file_obj.name}: {str(e)}")

        # Render download buttons with deduplicated, pre-read files
        for file_obj, file_contents in file_data:
            # Use run_id + hash of full path for truly unique keys
            key_prefix = run_id if run_id else datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            path_hash = hashlib.md5(str(file_obj).encode()).hexdigest()[:8]
            unique_key = f"download_{key_prefix}_{path_hash}"

            st.download_button(
                label=f"Download {file_obj.name}",
                data=file_contents,
                file_name=file_obj.name,
                mime="application/octet-stream",
                key=unique_key
            )


def render_approval(message):
    """Render approval UI."""
    st.write(message["content"])

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("✓ Looks good, thanks!", use_container_width=True, key="approval_yes_button"):
            st.session_state.agent_state["approved"] = True
            st.session_state.awaiting_input = None
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Great! All files saved to {st.session_state.output_path}",
            })
            st.rerun()

    with col2:
        if st.button("🔄 Try again", use_container_width=True, key="retry_button"):
            # Remove old result and approval messages to avoid duplicate element keys
            old_run_id = message.get("state", {}).get("run_id", "")
            st.session_state.messages = [
                msg for msg in st.session_state.messages
                if msg.get("type") not in ["result", "approval"]
            ]
            # Add archive message
            if old_run_id:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"📦 Previous run archived: `{old_run_id}/`",
                })
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
        if feedback_text and st.button("Refine", use_container_width=True, key="refine_button"):
            # Remove old result and approval messages to avoid duplicate element keys
            old_run_id = message.get("state", {}).get("run_id", "")
            st.session_state.messages = [
                msg for msg in st.session_state.messages
                if msg.get("type") not in ["result", "approval"]
            ]
            # Add archive message
            if old_run_id:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"📦 Previous run archived: `{old_run_id}/` — Your feedback: _{feedback_text}_",
                })
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
