# Data Genie

A local-first, cloud-agnostic, skill-driven agentic assistant for data engineering tasks. Inspired by Databricks Genie, Data Genie runs entirely on your local machine, works with any LLM provider, and plugs in existing Python scripts as skills — no code modifications required.

## Quick Start

```bash
git clone <repo-url>
cd data_genie
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your LLM API key
streamlit run app.py
```

Open `http://localhost:8501` in your browser.

## How It Works

1. **Skill Discovery** — You describe what you need in natural language (e.g., "Analyze my ARM template"). Data Genie scans its skills and shows you the top 3 matches.

2. **Confirmation Checkpoint** — Before running anything, you review the selected skill, input file, and output folder. You must explicitly approve before execution begins.

3. **Skill Execution** — The selected skill runs and streams progress messages live to the chat so you always see what's happening. Each step is marked with a checkmark as it completes.

4. **Output Review** — Skill results are displayed as a markdown report with metrics and download buttons. An optional AI reviewer can flag quality issues (disabled if no API key configured).

5. **Refinement Loop** — You can approve the results, request changes, or try again (max 3 iterations). Feedback is passed back to the skill to improve its output.

## Adding a New Skill

Adding a new skill requires no changes to the core code — just create a folder with these files:

1. Create a directory:
   ```bash
   mkdir -p skills/my_new_skill/tools
   ```

2. Write `SKILL.md` (copy from an existing skill):
   ```
   # My New Skill
   
   ## description
   [2-3 sentences about what it does]
   
   ## intent_keywords
   [15-20 keywords covering user intents]
   
   ## entry_point
   my_skill.py :: run(params: dict, progress_callback=None) -> dict
   
   ## inputs
   - input_path (str, required): path to input file
   - output_path (str, required): folder to write outputs
   
   ## outputs
   - summary (str): markdown report
   - output_files (list): absolute file paths
   - data (dict): structured results
   
   ## when_to_use
   [3-5 example user messages]
   ```

3. Write `schema.json` (input parameters and their types):
   ```json
   {
     "input": {
       "input_path": {"type": "string", "required": true},
       "output_path": {"type": "string", "required": true}
     },
     "output": {
       "summary": {"type": "string"},
       "output_files": {"type": "array"},
       "data": {"type": "object"}
     }
   }
   ```

4. Write `tools/my_tool.py` (the actual tool class):
   ```python
   class MyTool:
       def __init__(self, input_path, output_path, progress_callback=None, verbose=False):
           self.input_path = Path(input_path)
           self.output_path = Path(output_path)
           self.callback = progress_callback or (lambda m: None)
       
       def run(self) -> dict:
           self.callback("Starting...")
           # TODO: Real implementation
           return {
               "summary": "# Report\n\nDone!",
               "output_files": [...],
               "data": {...}
           }
   ```

5. Write `my_new_skill.py` (thin wrapper, in `skills/my_new_skill/`):
   ```python
   from typing import Dict, Callable, Optional
   from .tools.my_tool import MyTool
   
   def run(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
       tool = MyTool(params["input_path"], params["output_path"], progress_callback)
       return tool.run()
   ```

That's it! The skill registry auto-discovers your new skill on next app run (no rebuild needed).

## Swapping the LLM Provider

Edit `.env` and change these lines:

```env
LLM_PROVIDER=anthropic
LLM_MODEL=claude-sonnet-4-20250514
ANTHROPIC_API_KEY=your_key_here
```

### Supported Providers

- **anthropic** — Claude models via Anthropic API
- **openai** — GPT-4, GPT-4o via OpenAI API
- **azure** — Azure OpenAI (requires AZURE_API_KEY, AZURE_ENDPOINT, AZURE_DEPLOYMENT, and AZURE_API_VERSION)
- **google** — Gemini models via Google API
- **groq** — Llama 3 via Groq API

For **Azure OpenAI**, add these to `.env`:
```env
LLM_PROVIDER=azure
AZURE_API_KEY=your_azure_api_key_here
AZURE_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_DEPLOYMENT=your-deployment-name
AZURE_API_VERSION=2024-02-15-preview
```

**No code changes required.** The app auto-configures based on `.env`.

## Project Structure

```
data_genie/
├── app.py                            # Streamlit UI — entry point
├── .env.example                      # Config template
├── requirements.txt
├── README.md
├── skill_registry.json               # Auto-generated index
│
├── agent/
│   ├── __init__.py
│   ├── state.py                      # TypedDict for agent state
│   └── graph.py                      # LangGraph state machine
│
├── core/
│   ├── __init__.py
│   ├── skill_registry.py             # Scans skills/, builds index
│   ├── skill_selector.py             # Matches intent → skills
│   ├── skill_executor.py             # Validates, runs skills
│   ├── llm_client.py                 # liteLLM wrapper
│   └── review_agent.py               # AI quality review
│
├── skills/
│   ├── arm_template_skill/
│   │   ├── SKILL.md
│   │   ├── schema.json
│   │   ├── arm_analyzer.py           # Wrapper
│   │   └── tools/arm_template_tool.py
│   │
│   ├── discovery_skill/
│   ├── pandas_pyspark_skill/
│   ├── pyspark_migrator_skill/
│   ├── data_copy_skill/
│   └── validation_skill/
│
└── tests/
    └── sample_inputs/
        └── arm_template_sample.json
```

## Running a Skill Standalone (CLI)

Each skill's tool can be run independently without Streamlit:

```bash
python skills/arm_template_skill/tools/arm_template_tool.py \
  --input tests/sample_inputs/arm_template_sample.json \
  --output /tmp/results/ \
  --verbose
```

Output:
- `inventory.xlsx` — Excel workbook with resource tables
- `inventory_summary.json` — Structured JSON data
- `report.md` — Markdown report

All skills follow the same CLI pattern: `--input`, `--output`, `--verbose`.

## System Design

Data Genie follows a **skill-driven agent architecture** with three core layers:

```
┌─────────────────────────────────────────────────────────────┐
│                  STREAMLIT USER INTERFACE                   │
│  (Chat, Skill Selection, Execution Monitoring, Downloads)   │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│            AGENT ORCHESTRATION (LangGraph)                  │
│  • Parse & Validate Input                                   │
│  • Select Best Matching Skills                              │
│  • Route to Skill Executor                                  │
│  • Review & Refine Output (max 3 iterations)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ Skill 1 │   │ Skill 2 │   │ Skill N │
   └────┬────┘   └────┬────┘   └────┬────┘
        │              │              │
        └──────────────┼──────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
   ┌─────────┐   ┌──────────┐   ┌──────────┐
   │  Tool 1 │   │  Tool 2  │   │  Tool N  │
   │ (Python)│   │ (Python) │   │ (Python) │
   └─────────┘   └──────────┘   └──────────┘
```

### **Three Agents in the System:**

1. **Router Agent (LangGraph State Machine)**
   - Parses user input and validates file paths
   - Matches user intent to available skills using keyword scoring
   - Orchestrates the workflow with human-in-the-loop checkpoints
   - Manages feedback loops (max 3 iterations)

2. **Executor Agent (Skill Wrapper)**
   - Calls the selected skill's `run()` function
   - Streams progress messages back to Streamlit
   - Validates parameters against skill schema
   - Returns: `{summary, output_files, data}`

3. **Review Agent (LLM-based)**
   - Analyzes skill output for quality issues
   - Checks for completeness, correctness, and safety
   - Flags potential problems for user review
   - Disabled if LLM API key not configured

---

## Workflow Diagram

### **Complete User Workflow (5 Steps)**

```
USER SUBMITS REQUEST
        │
        ▼
┌─────────────────────────────────────┐
│ 🔍 ROUTER AGENT IDENTIFIES SKILLS   │
│  ✓ Analyze user input               │
│  ✓ Match intent to top 3 skills     │
│  ✓ Score by relevance               │
└─────────────────────────────────────┘
        │
        ▼ [PRESENTS OPTIONS]
┌─────────────────────────────────────┐
│ 👤 USER SELECTS & CONFIRMS SKILL    │
│  ✓ Review skill details             │
│  ✓ Approve input/output paths       │
│  ✓ Confirm execution                │
└─────────────────────────────────────┘
        │
        ▼ [USER APPROVES]
┌─────────────────────────────────────┐
│ ⚙️ EXECUTOR AGENT RUNS SKILL         │
│  ✓ Invoke skill tool                │
│  ✓ Stream progress live             │
│  ✓ Generate output files            │
└─────────────────────────────────────┘
        │
        ▼ [EXECUTION COMPLETE]
┌─────────────────────────────────────┐
│ 🤖 REVIEW AGENT VALIDATES OUTPUT    │
│  ✓ Check quality & completeness     │
│  ✓ Verify safety & correctness      │
│  ✓ Flag issues if found             │
└─────────────────────────────────────┘
        │
        ▼ [SHOWS RESULTS]
┌─────────────────────────────────────┐
│ ✅ PRESENT RESULTS TO USER          │
│  ✓ Display markdown report          │
│  ✓ Offer file downloads             │
│  ✓ Allow approval or refinement     │
└─────────────────────────────────────┘
        │
        ├─→ [APPROVED]  ──→ END
        │
        └─→ [FEEDBACK]  ──→ Loop back to Executor (max 3 times)
```

---

## Architecture Details

### **LangGraph State Machine**

The router agent is a LangGraph state machine with 9 nodes and 3 human-in-the-loop checkpoints:

```
[START] → node_parse_input 
            ↓
         node_select_skills 
            ↓
         node_await_skill_choice    ⏸️ [HITL 1: User picks skill]
            ↓
         node_confirm_execution     ⏸️ [HITL 2: User approves]
            ↓
         node_execute_skill 
            ↓
         node_review_output 
            ↓
         node_generate_response 
            ↓
         node_await_approval        ⏸️ [HITL 3: User reviews result]
            ↓
         node_handle_feedback 
            ├─→ [approved]  ──→ [END]
            └─→ [feedback]  ──→ Loop back to node_execute_skill (max 3 iterations)
```

### **Skill Contract**

Every skill exposes this interface:

```python
def run(
    params: dict,                          # {"input_path": "...", "output_path": "..."}
    progress_callback: Optional[Callable] = None
) -> dict:
    return {
        "summary": "# Markdown Report\n\n...",  # Human-readable markdown
        "output_files": ["/path/file1.xlsx", "/path/file2.json"],  # Downloadable files
        "data": {                          # Structured results for programmatic use
            "resource_count": 42,
            "risk_flags": [...],
            "dependencies": [...]
        }
    }
```

### **Tool Structure**

Each skill has a tool that does the actual work:

```python
class MyTool:
    def __init__(self, input_path: str, output_path: str, 
                 progress_callback=None, verbose=False):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.callback = progress_callback or (lambda m: None)
    
    def run(self) -> dict:
        self.callback("Step 1: Loading data...")
        # ... implementation
        self.callback("Step 2: Processing...")
        # ... implementation
        return {
            "summary": "# Results\n\nProcessing complete!",
            "output_files": [...],
            "data": {...}
        }
```

---

## UI Workflow Panel

The Streamlit UI shows real-time workflow progress:

```
┌──────────────────────────────────────────────────────────┐
│ Chat Input/Output                  │ 🔄 WORKFLOW STATUS  │
│                                    │ ┌──────────────────┐│
│ User: "Analyze my ARM template"    │ │🔍 Router Agent   ││
│                                    │ │   (Active)       ││
│ Assistant: Found 3 skills          │ │        ↓         ││
│                                    │ │👤 User Confirm   ││
│ [Skill Options with Select]        │ │   (Pending)      ││
│                                    │ │        ↓         ││
│ User: Selects ARM Template Analyser│ │⚙️ Executor Agent ││
│                                    │ │   (Pending)      ││
│ Assistant: Running analysis...     │ │        ↓         ││
│ ✓ Reading template                 │ │🤖 Review Agent   ││
│ ✓ Extracting resources             │ │   (Pending)      ││
│ ✓ Detecting risks                  │ │        ↓         ││
│                                    │ │✅ Complete       ││
│ Results:                           │ │   (Pending)      ││
│ • 42 resources found               │ └──────────────────┘│
│ • 8 critical risks                 │                     │
│                                    │ Tools Used:         │
│ [Download inventory.xlsx]          │ • arm_template_tool │
│                                    │ • JSON Parser       │
│ [Approve] [Refine] [Try Again]     │ • openpyxl         │
└──────────────────────────────────────────────────────────┘
```

---

## Skill Contract

Every skill must expose `run(params, progress_callback)` that returns:
```python
{
    "summary": str,           # Markdown report for display
    "output_files": [str],    # Paths to downloadable files
    "data": dict              # Structured data for programmatic use
}
```

## Troubleshooting

### "LLM not configured — review agent disabled"
**Solution:** Add your API key to `.env`:
```env
OPENAI_API_KEY=sk-proj-your-actual-key-here
```

### "File not found" error
**Solution:** Use absolute file paths:
```
WRONG:  ~/data/file.json
RIGHT:  C:/Users/username/data/file.json
```

### "Permission denied" on output folder
**Solution:** Ensure the folder is writable:
```bash
chmod 755 /path/to/output/folder  # Linux/Mac
# Windows: Right-click folder → Properties → Security → Edit
```

### Skill doesn't appear in UI
**Solution:** Ensure the skill folder structure is correct:
```
skills/my_skill/
├── SKILL.md
├── schema.json
├── my_skill.py
└── tools/
    └── my_tool.py
```

### Skill execution hangs
**Solution:** Check if the input file exists and is valid. Skills have a 5-minute timeout.

## Limitations & Future Work

- Local execution only (no cloud deployment)
- Single-user (Streamlit limitation)
- Skill parameter validation uses schema.json only (no custom validators)
- Progress streaming uses Streamlit's `st.empty()` re-renders (not true streaming)
- AI review disabled if no LLM API key configured

## Contributing

To add a new skill, follow the "Adding a New Skill" section above. To modify core behavior:
1. Change `agent/graph.py` for workflow changes
2. Change `core/skill_selector.py` for matching logic
3. Change `core/llm_client.py` for LLM provider changes
4. Change `app.py` for UI changes

All skill wrapper modules auto-register — no code changes needed.

## License

MIT
