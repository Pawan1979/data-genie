"""PySpark Migrator — assess migration risks and compatibility."""

import json
from pathlib import Path
from typing import Callable, Optional, Dict


class PySparkMigrator:
    """Assess PySpark code for migration compatibility."""

    def __init__(
        self,
        input_path: str,
        output_path: str,
        progress_callback: Optional[Callable] = None,
        verbose: bool = False,
    ):
        """Initialize migrator."""
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.callback = progress_callback or (print if verbose else lambda m: None)
        self.source_code = ""

    def run(self) -> Dict:
        """Execute migration assessment."""
        try:
            self._load_source()
            self._detect_incompatibilities()
            self._assess_migration_risk()
            self._identify_deprecated_apis()

            self.output_path.mkdir(parents=True, exist_ok=True)
            self._write_migrated_script()
            self._write_migration_plan()
            summary_md = self._write_markdown_report()

            return {
                "summary": summary_md,
                "output_files": [
                    str(self.output_path / "migrated_script.py"),
                    str(self.output_path / "migration_plan.md"),
                    str(self.output_path / "risk_report.xlsx"),
                ],
                "data": {
                    "incompatibilities_found": len(self.incompatibilities),
                    "migration_difficulty": self.risk_level,
                    "changes_needed": len(self.api_changes),
                },
            }
        except Exception as e:
            return {
                "summary": f"## Error\n\n{str(e)}",
                "output_files": [],
                "data": {"error": str(e)},
            }

    def _load_source(self):
        """Load source code."""
        self.callback("Loading PySpark code...")
        if not self.input_path.exists():
            raise FileNotFoundError(f"File not found: {self.input_path}")

        self.source_code = self.input_path.read_text()

    def _detect_incompatibilities(self):
        """Detect Databricks/migration incompatibilities."""
        self.callback("Detecting incompatibilities...")
        # TODO: Check for RDD usage, legacy APIs, SC instead of SparkSession, etc.
        self.incompatibilities = []

        if "sc." in self.source_code and "SparkContext" in self.source_code:
            self.incompatibilities.append({
                "code": "sc.",
                "severity": "HIGH",
                "recommendation": "Use SparkSession instead of SparkContext"
            })

        if "rdd" in self.source_code.lower():
            self.incompatibilities.append({
                "code": "RDD usage",
                "severity": "MEDIUM",
                "recommendation": "Use DataFrames API for better performance"
            })

    def _assess_migration_risk(self):
        """Assess overall migration risk level."""
        self.callback("Assessing migration risk...")
        # TODO: Risk scoring based on incompatibilities, custom UDFs, etc.
        high_count = sum(1 for i in self.incompatibilities if i["severity"] == "HIGH")
        self.risk_level = "CRITICAL" if high_count > 3 else "HIGH" if high_count > 0 else "LOW"

    def _identify_deprecated_apis(self):
        """Identify deprecated APIs."""
        self.callback("Identifying deprecated APIs...")
        # TODO: Check for deprecated Spark APIs from older versions
        self.api_changes = []

        deprecated_apis = [
            ("SQLContext", "Use SparkSession"),
            ("HiveContext", "Use SparkSession"),
            ("sqlContext", "Use spark from SparkSession"),
        ]

        for old_api, recommendation in deprecated_apis:
            if old_api in self.source_code:
                self.api_changes.append({
                    "old": old_api,
                    "new": recommendation,
                })

    def _write_migrated_script(self):
        """Write migration plan script."""
        self.callback("Writing migrated_script.py...")
        migrated = self.source_code.replace("SQLContext", "SparkSession")
        migrated = migrated.replace("HiveContext", "SparkSession")
        (self.output_path / "migrated_script.py").write_text(migrated)

    def _write_migration_plan(self):
        """Write detailed migration plan."""
        self.callback("Writing migration_plan.md...")
        lines = [
            "# PySpark Migration Plan",
            f"\n**Source:** `{self.input_path.name}`",
            f"\n**Risk Level:** {self.risk_level}",
            "\n## Incompatibilities Found",
            "\n| Code | Severity | Recommendation |",
            "|---|---|---|",
        ]

        for incompat in self.incompatibilities:
            lines.append(
                f"| {incompat['code']} | {incompat['severity']} | {incompat['recommendation']} |"
            )

        if self.api_changes:
            lines.extend(["\n## API Changes", "\n| Deprecated | Recommended |", "|---|---|"])
            for change in self.api_changes:
                lines.append(f"| {change['old']} | {change['new']} |")

        (self.output_path / "migration_plan.md").write_text("\n".join(lines))

    def _write_markdown_report(self) -> str:
        """Write markdown report."""
        self.callback("Generating markdown report...")
        lines = [
            "# PySpark Migration Assessment Report",
            f"\n**Risk Level:** {self.risk_level}",
            "\n## Summary",
            "\n| Metric | Value |",
            "|---|---|",
            f"| Incompatibilities | {len(self.incompatibilities)} |",
            f"| API changes needed | {len(self.api_changes)} |",
        ]

        report_md = "\n".join(lines)
        (self.output_path / "report.md").write_text(report_md)
        return report_md


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PySpark Migrator")
    parser.add_argument("--input", required=True, help="Path to PySpark script")
    parser.add_argument("--output", required=True, help="Output folder")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    migrator = PySparkMigrator(args.input, args.output, verbose=args.verbose)
    result = migrator.run()
    print(result["summary"])
