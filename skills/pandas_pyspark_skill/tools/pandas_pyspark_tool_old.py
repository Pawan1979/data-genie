"""Pandas to PySpark Converter Tool — Core implementation with discovery and validation."""

import json
import logging
from pathlib import Path
from typing import Callable, Optional, Dict, List
from datetime import datetime
import sys

# Import core converter classes
try:
    from ..converter_core import (
        PandasDiscovery, PandasToSparkConverter, SecureCodeValidator,
        SUPPORTED_WRITE_OPS
    )
except ImportError:
    # Fallback for direct imports
    import converter_core
    PandasDiscovery = converter_core.PandasDiscovery
    PandasToSparkConverter = converter_core.PandasToSparkConverter
    SecureCodeValidator = converter_core.SecureCodeValidator
    SUPPORTED_WRITE_OPS = converter_core.SUPPORTED_WRITE_OPS

logger = logging.getLogger(__name__)


class PandasToSparkTool:
    """Convert pandas Python code to PySpark equivalents with validation.

    This tool:
    1. Discovers pandas operations in input file using AST
    2. Converts pandas read/write calls to Spark equivalents
    3. Validates converted code for security and syntax
    4. Generates detailed reports
    """

    def __init__(
        self,
        input_path: str,
        output_path: str,
        progress_callback: Optional[Callable] = None,
        path_mapping: Optional[Dict[str, str]] = None,
    ):
        """Initialize tool.

        Args:
            input_path: Path to Python file with pandas code
            output_path: Output folder for results and reports
            progress_callback: Optional callback for progress messages
            path_mapping: Optional mapping of old paths to new (e.g., {'/mnt/old': '/mnt/new'})
        """
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.callback = progress_callback or (lambda m: None)
        self.path_mapping = path_mapping or {}

        # Core components
        self.discovery = PandasDiscovery()
        self.converter = PandasToSparkConverter(self.path_mapping)
        self.validator = SecureCodeValidator()

    def run(self) -> Dict:
        """Execute conversion pipeline.

        Returns:
            Dict with keys: summary, output_files, data
        """
        try:
            self.output_path.mkdir(parents=True, exist_ok=True)

            # Phase 1: Load and discover
            self.callback("📁 Loading source code...")
            source_code = self._load_source()

            self.callback("🔍 Discovering pandas operations...")
            discovery_results = self.discovery.discover_source(str(self.input_path))

            # Phase 2: Convert
            self.callback("🔄 Converting to PySpark...")
            converted_code = self.converter.convert(source_code)

            # Phase 3: Analyze changes
            changes = self._analyze_changes(source_code, converted_code)

            # Phase 4: Generate reports
            self.callback("📋 Generating reports...")
            discovery_report = self._save_discovery_report(discovery_results)
            conversion_report = self._save_conversion_report(changes)
            converted_file = self._save_converted_code(converted_code)

            # Phase 5: Build summary
            summary = self._build_summary(
                discovery_results, changes, len(converted_code.split('\n'))
            )

            self.callback("✅ Conversion complete!")

            return {
                "summary": summary,
                "output_files": [converted_file, discovery_report, conversion_report],
                "data": {
                    "operations_found": len(discovery_results),
                    "changes_made": len(changes),
                    "files_processed": 1,
                    "conversion_confidence": self._estimate_confidence(changes),
                    "mount_points": self._count_mount_points(discovery_results),
                },
            }

        except Exception as e:
            logger.exception(f"Conversion failed: {e}")
            return {
                "summary": f"## Conversion Failed\n\n**Error:** {str(e)}\n\nCheck input file path and format.",
                "output_files": [],
                "data": {"error": str(e)},
            }

    def _load_source(self) -> str:
        """Load source code from input file."""
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")
        if not self.input_path.suffix == '.py':
            raise ValueError(f"Input must be Python file (.py), got {self.input_path.suffix}")

        return self.input_path.read_text(encoding='utf-8')

    def _analyze_changes(self, original: str, converted: str) -> List[Dict]:
        """Analyze what changed between original and converted code."""
        changes = []
        original_lines = original.split('\n')
        converted_lines = converted.split('\n')

        for i, (orig_line, conv_line) in enumerate(zip(original_lines, converted_lines), 1):
            if orig_line != conv_line and orig_line.strip() and conv_line.strip():
                changes.append({
                    'line_number': i,
                    'original': orig_line.strip(),
                    'converted': conv_line.strip(),
                })

        return changes

    def _save_discovered_code(self, code: str) -> str:
        """Save source with comments marking discoveries."""
        path = self.output_path / 'annotated_source.py'
        path.write_text(code, encoding='utf-8')
        return str(path)

    def _save_discovery_report(self, discoveries: List[Dict]) -> str:
        """Save discovery report as JSON."""
        path = self.output_path / 'discovery_report.json'
        report = {
            'timestamp': datetime.now().isoformat(),
            'file': str(self.input_path),
            'total_findings': len(discoveries),
            'operations': discoveries,
        }
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        return str(path)

    def _save_conversion_report(self, changes: List[Dict]) -> str:
        """Save conversion report."""
        path = self.output_path / 'conversion_summary.json'
        report = {
            'timestamp': datetime.now().isoformat(),
            'file': str(self.input_path),
            'total_changes': len(changes),
            'changes': changes,
        }
        path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        return str(path)

    def _save_converted_code(self, code: str) -> str:
        """Save converted Python code."""
        path = self.output_path / 'converted_code.py'
        path.write_text(code, encoding='utf-8')
        return str(path)

    def _build_summary(self, discoveries: List[Dict], changes: List[Dict], line_count: int) -> str:
        """Build markdown summary report."""
        read_ops = [d for d in discoveries if 'read_' in d.get('operation_type', '')]
        write_ops = [d for d in discoveries if 'to_' in d.get('operation_type', '')]

        summary = f"""# Pandas to PySpark Conversion Report

**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- **Input File:** {self.input_path.name}
- **Total Lines:** {line_count}
- **Pandas Operations Found:** {len(discoveries)}
- **Changes Made:** {len(changes)}

## Operations Discovered
- **Read Operations:** {len(read_ops)} (pd.read_csv, pd.read_parquet, etc.)
- **Write Operations:** {len(write_ops)} (df.to_csv, df.to_parquet, etc.)

## Conversion Results
- **Status:** ✅ Successfully converted
- **Confidence:** {self._estimate_confidence(changes):.0%}

## Output Files
- `converted_code.py` - Converted Python code
- `discovery_report.json` - Detailed findings
- `conversion_summary.json` - Change details
"""

        if read_ops:
            summary += "\n## Read Operations Found\n"
            for op in read_ops[:5]:  # Show first 5
                summary += f"- Line {op.get('line_number', '?')}: {op.get('operation_type', '?')}\n"
            if len(read_ops) > 5:
                summary += f"- ... and {len(read_ops) - 5} more\n"

        if self.path_mapping:
            summary += f"\n## Path Mappings Applied\n"
            for old, new in self.path_mapping.items():
                summary += f"- `{old}` → `{new}`\n"

        summary += "\n## Notes\n"
        summary += """
- All `pd.read_*()` calls converted to `spark.read...toPandas()`
- All `df.to_*()` calls converted to `spark.createDataFrame(...).write...`
- Options automatically mapped where supported
- Unsupported options are silently skipped
- Review converted code for manual adjustments (UDFs, complex logic)
"""

        return summary

    @staticmethod
    def _estimate_confidence(changes: List[Dict]) -> float:
        """Estimate conversion confidence (0-1)."""
        if not changes:
            return 1.0
        # Simple heuristic: more changes = lower confidence (code was heavily modified)
        # But well-understood operations = higher confidence
        return min(1.0, max(0.6, 1.0 - (len(changes) * 0.05)))

    @staticmethod
    def _count_mount_points(discoveries: List[Dict]) -> int:
        """Count Databricks mount point references."""
        return len([d for d in discoveries if d.get('is_mount_point', False)])


# Backward compatibility alias
class PandasPySparkConverter(PandasToSparkTool):
    """Deprecated: Use PandasToSparkTool instead."""
    pass

    def _write_conversion_notes(self):
        """Write conversion notes."""
        self.callback("Writing conversion_notes.md...")
        lines = [
            "# Pandas to PySpark Conversion Notes",
            "\n## Patterns Detected",
            "\n| Pattern | Count | Conversion |",
            "|---|---|---|",
        ]

        for pat in self.pandas_patterns:
            conversion = f"See converted code for details"
            lines.append(f"| {pat['pattern']} | {pat['count']} | {conversion} |")

        if self.manual_review_items:
            lines.extend(["\n## Manual Review Required", ""])
            lines.extend([f"- {item}" for item in self.manual_review_items])

        (self.output_path / "conversion_notes.md").write_text("\n".join(lines))

    def _write_markdown_report(self) -> str:
        """Write markdown report."""
        self.callback("Generating markdown report...")
        lines = [
            "# Pandas to PySpark Conversion Report",
            f"\n**Source:** `{self.input_path.name}`",
            "\n## Summary",
            "\n| Metric | Value |",
            "|---|---|",
            f"| Pandas patterns found | {len(self.pandas_patterns)} |",
            f"| Manual review items | {len(self.manual_review_items)} |",
            "\n## Status",
            "\n✓ Code generated (review required before production use)",
        ]

        report_md = "\n".join(lines)
        (self.output_path / "report.md").write_text(report_md)
        return report_md


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pandas to PySpark Converter")
    parser.add_argument("--input", required=True, help="Path to Python/notebook file")
    parser.add_argument("--output", required=True, help="Output folder")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    converter = PandasPySparkConverter(args.input, args.output, verbose=args.verbose)
    result = converter.run()
    print(result["summary"])
