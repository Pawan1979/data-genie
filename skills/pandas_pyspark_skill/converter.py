"""Pandas to PySpark Converter skill wrapper with auto-dependency management."""

from typing import Dict, Callable, Optional
import sys
from pathlib import Path
import shutil

try:
    from .tools.pandas_pyspark_tool import PandasToSparkTool
except ImportError:
    tools_dir = Path(__file__).parent / "tools"
    sys.path.insert(0, str(tools_dir))
    from pandas_pyspark_tool import PandasToSparkTool


def run(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
    """Run Pandas to PySpark Converter skill with auto-dependency installation.

    Features:
    - Auto-installs missing optional dependencies (astor, pandas)
    - Supports both single files and folder hierarchies
    - Optional side-by-side Spark testing (with graceful fallback)
    - Automatic Databricks/Spark detection
    - Auto-consolidates converted files into single folder (default: enabled)

    Parameters:
        input_path: Path to Python file or folder with pandas code
        output_path: Output folder for converted code and reports
        path_mapping: Optional dict mapping old paths to new (e.g., {'/mnt/old': '/mnt/new'})
        auto_install_deps: If True, auto-install missing dependencies (default: True)
        enable_spark_testing: If True, run side-by-side Spark tests (default: True)
        auto_consolidate: If True, automatically consolidate files into converted_pyspark_files/ (default: True)

    Returns:
        Dict with:
        - summary: Markdown report
        - output_files: List of generated file paths
        - data: Metrics (operations_found, changes_made, spark_available, spark_tested, consolidated_folder, etc.)

    Examples:
        # Single file conversion (consolidation enabled by default)
        result = run({
            'input_path': 'my_script.py',
            'output_path': 'converted_output'
        })

        # Folder conversion with consolidation disabled
        result = run({
            'input_path': '/path/to/pandas_scripts',
            'output_path': '/path/to/output',
            'auto_consolidate': False
        })

        # Folder conversion with all options
        result = run({
            'input_path': '/path/to/pandas_scripts',
            'output_path': '/path/to/output',
            'path_mapping': {'/mnt/old': '/mnt/new'},
            'auto_install_deps': True,
            'enable_spark_testing': True,
            'auto_consolidate': True
        })
    """
    auto_consolidate = params.get("auto_consolidate", True)

    tool = PandasToSparkTool(
        input_path=params["input_path"],
        output_path=params["output_path"],
        progress_callback=progress_callback,
        path_mapping=params.get("path_mapping"),
        auto_install_deps=params.get("auto_install_deps", True),
        enable_spark_testing=params.get("enable_spark_testing", True),
        auto_consolidate=auto_consolidate,  # Pass to tool
    )
    result = tool.run()

    # Post-processing: Create consolidated folder by default (skill-level consolidation)
    if auto_consolidate:
        _create_consolidated_pyspark_folder(params["output_path"])

    return result


def _create_consolidated_pyspark_folder(output_path: str) -> None:
    """Create consolidated folder with only converted PySpark files.

    This handles the case where batch processor calls the skill for each file
    individually. Works with parallel processing by continuously updating the
    consolidated folder on each call.

    Args:
        output_path: Root output path containing individual conversion folders
    """
    output_path = Path(output_path)
    if not output_path.exists():
        return

    # Create consolidated folder (always, even if called multiple times)
    consolidated_folder = output_path / "converted_pyspark_files"
    consolidated_folder.mkdir(parents=True, exist_ok=True)

    # Find all .py files in subdirectories and copy to consolidated
    # This works even if called before all files are processed - just copies what's available
    converted_files = list(output_path.glob("*/*.py"))

    for py_file in converted_files:
        # Skip if already in consolidated folder
        if py_file.parent.name == "converted_pyspark_files":
            continue

        dest_file = consolidated_folder / py_file.name
        try:
            shutil.copy2(str(py_file), str(dest_file))
        except Exception:
            pass  # Silent fail - don't interrupt the skill


def consolidate(params: Dict, progress_callback: Optional[Callable] = None) -> Dict:
    """Consolidate converted PySpark files into a single clean folder.

    This is a separate tool that takes an existing converter output folder and creates
    a single consolidated folder containing only the converted .py files.

    Parameters:
        output_path: Path to existing converter output folder
        consolidate_mode: Type of consolidation:
            - 'files_only' (default): Copy only .py files to a single folder
            - 'with_reports': Copy .py files and their conversion reports
            - 'clean_output': Create a completely clean output folder

    Returns:
        Dict with:
        - summary: Markdown report
        - output_files: List of generated file paths
        - data: Metrics (files_consolidated, consolidated_folder, etc.)

    Examples:
        # Consolidate existing output
        result = consolidate({
            'output_path': 'my_output/session/timestamp',
            'consolidate_mode': 'files_only'
        })
    """
    output_path = Path(params.get("output_path", ""))
    consolidate_mode = params.get("consolidate_mode", "files_only")

    if not output_path.exists():
        return {
            "summary": f"Error: Output path not found: {output_path}",
            "output_files": [],
            "data": {
                "error": "path_not_found",
                "consolidated_files": 0,
            },
        }

    # Find all converted .py files
    converted_files = list(output_path.glob("*/*.py"))
    converted_files.extend(list(output_path.glob("*.py")))  # Top-level files too
    converted_files = list(set(converted_files))  # Remove duplicates

    if not converted_files:
        return {
            "summary": f"No converted files found in: {output_path}",
            "output_files": [],
            "data": {
                "error": "no_files_found",
                "consolidated_files": 0,
            },
        }

    # Create consolidated folder
    if consolidate_mode == "files_only":
        consolidated_folder = output_path / "consolidated_pyspark"
    elif consolidate_mode == "with_reports":
        consolidated_folder = output_path / "consolidated_with_reports"
    else:  # clean_output
        consolidated_folder = output_path / "clean_pyspark_output"

    consolidated_folder.mkdir(parents=True, exist_ok=True)

    # Copy files based on mode
    copied_count = 0
    report_count = 0

    for py_file in converted_files:
        # Skip if already in consolidated folder
        if py_file.parent.name in ["consolidated_pyspark", "consolidated_with_reports", "clean_pyspark_output"]:
            continue

        dest_file = consolidated_folder / py_file.name
        try:
            shutil.copy2(str(py_file), str(dest_file))
            copied_count += 1

            # Copy reports if requested
            if consolidate_mode in ["with_reports", "clean_output"]:
                # Look for .json reports next to the .py file
                report_file = py_file.with_suffix(".conversion_report.json")
                if report_file.exists():
                    dest_report = consolidated_folder / report_file.name
                    shutil.copy2(str(report_file), str(dest_report))
                    report_count += 1

                # Also copy summary if it exists
                summary_file = py_file.parent / "conversion_summary.json"
                if summary_file.exists():
                    dest_summary = consolidated_folder / f"{py_file.stem}_summary.json"
                    shutil.copy2(str(summary_file), str(dest_summary))
                    report_count += 1

        except Exception as e:
            if progress_callback:
                progress_callback(f"Warning: Could not copy {py_file.name}: {e}")

    # Generate summary
    summary = f"""# Pandas to PySpark Files Consolidation Report

**Consolidation Mode:** {consolidate_mode.replace('_', ' ').title()}
**Source Folder:** {output_path}
**Consolidated Folder:** {consolidated_folder}

## Summary
- **Files Consolidated:** {copied_count}
- **Reports Included:** {report_count}
- **Consolidation Status:** Success

## Consolidated Folder Contents

### Mode: {consolidate_mode.replace('_', ' ').title()}
"""

    if consolidate_mode == "files_only":
        summary += """- Only converted .py files
- Clean, minimal output
- Ready to copy to your project
"""
    elif consolidate_mode == "with_reports":
        summary += """- Converted .py files
- Conversion reports (.conversion_report.json)
- Full metadata for review
"""
    else:  # clean_output
        summary += """- Converted .py files
- Conversion reports
- Summary reports
- Complete information package
"""

    summary += f"""
## Next Steps
1. Review files in: {consolidated_folder}
2. Copy to your PySpark project as needed
3. Delete original output folder if consolidation is complete

## File List
"""

    for f in sorted(consolidated_folder.glob("*")):
        if f.is_file():
            size = f.stat().st_size / 1024
            summary += f"- {f.name} ({size:.1f} KB)\n"

    if progress_callback:
        progress_callback(f"Consolidation complete: {copied_count} files consolidated")

    return {
        "summary": summary,
        "output_files": [str(consolidated_folder)],
        "data": {
            "consolidated_files": copied_count,
            "reports_included": report_count,
            "consolidation_mode": consolidate_mode,
            "consolidated_folder": str(consolidated_folder),
        },
    }
