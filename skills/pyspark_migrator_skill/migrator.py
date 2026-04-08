"""PySpark Migrator skill wrapper."""

from typing import Dict, Callable, Optional
import sys
from pathlib import Path

try:
    from .tools.pyspark_migrator_tool import PySparkMigrator
except ImportError:
    tools_dir = Path(__file__).parent / "tools"
    sys.path.insert(0, str(tools_dir))
    from pyspark_migrator_tool import PySparkMigrator


def run(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
    """Run PySpark Migrator skill."""
    migrator = PySparkMigrator(
        input_path=params["input_path"],
        output_path=params["output_path"],
        progress_callback=progress_callback,
    )
    return migrator.run()
