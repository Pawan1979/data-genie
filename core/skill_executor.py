"""Skill executor — runs selected skill with error handling."""

import sys
import importlib.util
import json
from pathlib import Path
from typing import Dict, Callable, Optional


def execute_skill(
    skill_meta: Dict,
    params: Dict,
    progress_callback: Optional[Callable] = None,
) -> Dict:
    """
    Execute a selected skill.

    Args:
        skill_meta: Skill metadata from registry (includes entry_module, schema_path)
        params: Skill parameters (input_path, output_path, etc.)
        progress_callback: Optional callback for progress messages

    Returns:
        dict with keys: summary (str), output_files (list), data (dict)
        On error: {"error": str, "summary": "## Error\\n\\n..."}
    """
    try:
        # Load schema and apply defaults
        params = _apply_schema_defaults(skill_meta, params)

        # Validate required params
        _validate_required_params(params)

        # Resolve paths
        params["input_path"] = str(Path(params["input_path"]).resolve())
        params["output_path"] = str(Path(params["output_path"]).resolve())

        # Dynamically import skill module
        entry_module = skill_meta.get("entry_module")
        if not entry_module:
            raise ValueError(f"No entry_module defined for {skill_meta.get('name')}")

        entry_path = Path(entry_module).resolve()
        if not entry_path.exists():
            raise FileNotFoundError(f"Skill module not found: {entry_module}")

        # Add skill directory to sys.path so relative imports work
        skill_dir = str(entry_path.parent)
        if skill_dir not in sys.path:
            sys.path.insert(0, skill_dir)

        try:
            # Load module with package context
            spec = importlib.util.spec_from_file_location(
                entry_path.stem,
                entry_path,
                submodule_search_locations=[]
            )
            if not spec or not spec.loader:
                raise ImportError(f"Could not load module: {entry_module}")

            module = importlib.util.module_from_spec(spec)

            # Register in sys.modules before executing (for relative imports)
            sys.modules[entry_path.stem] = module
            spec.loader.exec_module(module)

            # Call skill's run() function
            if not hasattr(module, "run"):
                raise AttributeError(f"Module {entry_module} has no run() function")

            result = module.run(params, progress_callback=progress_callback)

            # Ensure result has required keys
            if not isinstance(result, dict):
                raise TypeError(f"Skill run() must return dict, got {type(result)}")

            # Validate result structure
            if "summary" not in result:
                result["summary"] = ""
            if "output_files" not in result:
                result["output_files"] = []
            if "data" not in result:
                result["data"] = {}

            return result

        finally:
            # Clean up sys.modules
            if entry_path.stem in sys.modules:
                del sys.modules[entry_path.stem]

    except Exception as e:
        # Catch all exceptions — never crash Streamlit
        error_msg = f"## Skill Execution Error\n\n{type(e).__name__}: {str(e)}"
        return {
            "summary": error_msg,
            "output_files": [],
            "data": {"error": str(e), "error_type": type(e).__name__},
        }


def _apply_schema_defaults(skill_meta: Dict, params: Dict) -> Dict:
    """Apply schema defaults to params."""
    schema_path = skill_meta.get("schema_path")
    if not schema_path or not Path(schema_path).exists():
        return params

    try:
        schema_doc = json.loads(Path(schema_path).read_text())
        input_schema = schema_doc.get("input", {})

        # Apply defaults for missing optional fields
        for field_name, field_def in input_schema.items():
            if field_name not in params and "default" in field_def:
                params[field_name] = field_def["default"]

        return params
    except Exception:
        # If schema parsing fails, just return params as-is
        return params


def _validate_required_params(params: Dict):
    """Validate that required params are present."""
    required = ["input_path", "output_path"]
    for field in required:
        if field not in params:
            raise ValueError(f"Missing required parameter: {field}")

        value = params[field]
        if not value or (isinstance(value, str) and not value.strip()):
            raise ValueError(f"Parameter {field} cannot be empty")
