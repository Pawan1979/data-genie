"""Data Validation skill wrapper."""

from typing import Dict, Callable, Optional
import sys
from pathlib import Path

try:
    from .tools.validation_tool import DataValidationTool
except ImportError:
    tools_dir = Path(__file__).parent / "tools"
    sys.path.insert(0, str(tools_dir))
    from validation_tool import DataValidationTool


def run(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
    """Run Data Validation skill."""
    tool = DataValidationTool(
        input_path=params["input_path"],
        output_path=params["output_path"],
        progress_callback=progress_callback,
    )
    return tool.run()
