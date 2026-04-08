"""ARM Template skill wrapper — thin layer over arm_template_tool."""

from typing import Dict, Callable, Optional
import sys
from pathlib import Path

# Handle both relative and absolute imports
try:
    from .tools.arm_template_tool import ArmTemplateAnalyser
except ImportError:
    # Fallback for dynamic module loading
    tools_dir = Path(__file__).parent / "tools"
    sys.path.insert(0, str(tools_dir))
    from arm_template_tool import ArmTemplateAnalyser


def run(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
    """
    Run ARM Template Analyser skill.

    Args:
        params: dict with keys:
            - input_path (str): path to ARM template JSON
            - output_path (str): folder to write outputs
            - include_child_resources (bool, optional): parse nested resources
            - min_risk_severity (str, optional): minimum risk level
        progress_callback: optional callback for progress messages

    Returns:
        dict with keys: summary (str), output_files (list), data (dict)
    """
    analyser = ArmTemplateAnalyser(
        input_path=params["input_path"],
        output_path=params["output_path"],
        progress_callback=progress_callback,
    )

    return analyser.run()
