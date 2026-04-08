"""Data Copy Tool — transfer files with checksum validation."""

import hashlib
import json
import shutil
from pathlib import Path
from typing import Callable, Optional, Dict
from datetime import datetime


class DataCopyTool:
    """Copy files with integrity verification."""

    def __init__(
        self,
        input_path: str,
        output_path: str,
        progress_callback: Optional[Callable] = None,
        verbose: bool = False,
    ):
        """Initialize copy tool."""
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.callback = progress_callback or (print if verbose else lambda m: None)
        self.checksum_match = False

    def run(self) -> Dict:
        """Execute copy pipeline."""
        try:
            self._validate_source()
            self._validate_destination()
            self._check_file_sizes()
            self._copy_file()
            self._verify_checksums()
            self._write_manifest()

            summary_md = self._generate_summary()

            return {
                "summary": summary_md,
                "output_files": [
                    str(self.output_path / "copy_manifest.json"),
                    str(self.output_path / "copy_report.md"),
                ],
                "data": {
                    "source": str(self.input_path),
                    "destination": str(self.output_path),
                    "checksum_match": self.checksum_match,
                },
            }
        except Exception as e:
            return {
                "summary": f"## Error\n\n{str(e)}",
                "output_files": [],
                "data": {"error": str(e)},
            }

    def _validate_source(self):
        """Validate source file exists."""
        self.callback("Validating source file...")
        if not self.input_path.exists():
            raise FileNotFoundError(f"Source file not found: {self.input_path}")

    def _validate_destination(self):
        """Validate destination path."""
        self.callback("Validating destination...")
        self.output_path.mkdir(parents=True, exist_ok=True)

    def _check_file_sizes(self):
        """Check source file size."""
        self.callback("Checking file sizes...")
        self.source_size = self.input_path.stat().st_size

    def _copy_file(self):
        """Copy file with progress tracking."""
        self.callback("Copying file...")
        # TODO: Implement incremental/chunked copy for large files
        dest_file = self.output_path / self.input_path.name
        shutil.copy2(self.input_path, dest_file)
        self.dest_file = dest_file
        self.dest_size = dest_file.stat().st_size

    def _verify_checksums(self):
        """Verify file checksums match."""
        self.callback("Verifying checksums...")
        # TODO: SHA256 or other hash algorithm
        source_hash = self._compute_hash(self.input_path)
        dest_hash = self._compute_hash(self.dest_file)
        self.checksum_match = source_hash == dest_hash
        self.source_hash = source_hash
        self.dest_hash = dest_hash

    def _compute_hash(self, path: Path) -> str:
        """Compute SHA256 hash of file."""
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _write_manifest(self):
        """Write copy manifest JSON."""
        self.callback("Writing manifest...")
        manifest = {
            "source": str(self.input_path),
            "destination": str(self.dest_file),
            "source_size_bytes": self.source_size,
            "dest_size_bytes": self.dest_size,
            "source_checksum_sha256": self.source_hash,
            "dest_checksum_sha256": self.dest_hash,
            "checksums_match": self.checksum_match,
            "copied_at": datetime.utcnow().isoformat(),
        }

        (self.output_path / "copy_manifest.json").write_text(json.dumps(manifest, indent=2))

    def _generate_summary(self) -> str:
        """Generate markdown summary."""
        self.callback("Generating summary...")
        lines = [
            "# Data Copy Report",
            f"\n**Source:** `{self.input_path.name}`",
            f"**Destination:** `{self.output_path}`",
            f"**Copied:** {datetime.utcnow().isoformat()}",
            "\n## Summary",
            "\n| Metric | Value |",
            "|---|---|",
            f"| Source size | {self.source_size:,} bytes |",
            f"| Dest size | {self.dest_size:,} bytes |",
            f"| Checksums match | {'✓ Yes' if self.checksum_match else '✗ No'} |",
            f"| Source hash | `{self.source_hash[:16]}...` |",
            f"| Dest hash | `{self.dest_hash[:16]}...` |",
        ]

        summary_md = "\n".join(lines)
        (self.output_path / "copy_report.md").write_text(summary_md)
        return summary_md


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Data Copy Tool")
    parser.add_argument("--input", required=True, help="Source file path")
    parser.add_argument("--output", required=True, help="Destination folder")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    tool = DataCopyTool(args.input, args.output, verbose=args.verbose)
    result = tool.run()
    print(result["summary"])
