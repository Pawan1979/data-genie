"""Pandas to PySpark Converter skill wrapper."""

from typing import Dict, Callable, Optional
import sys
from pathlib import Path

try:
    from .tools.pandas_pyspark_tool import PandasPySparkConverter
except ImportError:
    tools_dir = Path(__file__).parent / "tools"
    sys.path.insert(0, str(tools_dir))
    from pandas_pyspark_tool import PandasPySparkConverter


def run(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
    """Run Pandas to PySpark Converter skill."""
    converter = PandasPySparkConverter(
        input_path=params["input_path"],
        output_path=params["output_path"],
        progress_callback=progress_callback,
    )
    return converter.run()
