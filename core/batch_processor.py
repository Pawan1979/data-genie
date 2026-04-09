"""Batch processor — handle multiple files in parallel with scalability."""

import os
import json
from pathlib import Path
from typing import Dict, List, Callable, Optional, Any
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
import logging
import psutil

logger = logging.getLogger(__name__)


def _noop_progress(msg: str) -> None:
    """No-op progress callback (pickleable, for multiprocessing)."""
    pass


@dataclass
class BatchResult:
    """Result for a single file in batch processing."""
    file_path: str
    status: str  # "success", "error", "skipped"
    summary: str = ""
    output_files: List[str] = None
    data: Dict = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.output_files is None:
            self.output_files = []
        if self.data is None:
            self.data = {}


class BatchProcessor:
    """Process multiple files in parallel with scalability options."""

    def __init__(
        self,
        skill_executor,
        input_folder: str,
        output_folder: str,
        progress_callback: Optional[Callable] = None,
        max_workers: int = 4,
        file_pattern: str = "*",
        batch_mode: str = "parallel",  # "parallel", "sequential", "threaded"
    ):
        """
        Initialize batch processor.

        Args:
            skill_executor: The skill executor function
            input_folder: Folder containing input files
            output_folder: Folder for output files
            progress_callback: Optional callback for progress messages
            max_workers: Number of parallel workers
            file_pattern: Glob pattern for files to process (e.g., "*.json", "*.csv")
            batch_mode: Processing mode - "parallel" (multiprocessing), "threaded" (threading), "sequential"
        """
        self.skill_executor = skill_executor
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.progress_callback = progress_callback or _noop_progress  # Use module-level function (pickleable)
        self.file_pattern = file_pattern
        self.batch_mode = batch_mode

        # Smart worker allocation for multi-user scenarios
        self.max_workers = self._allocate_workers(max_workers, batch_mode)

        # Validate input folder
        if not self.input_folder.exists():
            raise FileNotFoundError(f"Input folder not found: {input_folder}")
        if not self.input_folder.is_dir():
            raise NotADirectoryError(f"Input path is not a directory: {input_folder}")

        # Create output folder
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def _allocate_workers(self, requested_workers: int, batch_mode: str) -> int:
        """Intelligently allocate workers based on system resources.

        In multi-user scenarios, this prevents one user's batch job from
        monopolizing system resources.

        Strategy:
        - For parallel (CPU-bound): Cap at 1/4 of available CPU cores
        - For threaded (I/O-bound): Can use more threads, cap at available cores
        - For sequential: Use 1 worker always
        """
        if batch_mode == "sequential":
            return 1

        try:
            available_cores = psutil.cpu_count(logical=True) or 4
            available_memory_gb = psutil.virtual_memory().available / (1024**3)

            if batch_mode == "parallel":
                # CPU-bound: allocate conservatively for multi-user (1/4 of cores)
                # This allows ~4 concurrent users without CPU contention
                safe_limit = max(1, available_cores // 4)
                allocated = min(requested_workers, safe_limit)

                if allocated < requested_workers:
                    logger.warning(
                        f"Parallel workers reduced: {requested_workers} → {allocated} "
                        f"(available cores: {available_cores}, safe limit: {safe_limit})"
                    )

            elif batch_mode == "threaded":
                # I/O-bound: can use more threads (up to available cores)
                # Each thread uses less memory than processes
                safe_limit = min(16, available_cores)
                allocated = min(requested_workers, safe_limit)

                if allocated < requested_workers:
                    logger.info(
                        f"Threaded workers limited: {requested_workers} → {allocated} "
                        f"(available cores: {available_cores})"
                    )
            else:
                allocated = requested_workers

            # Additional check: ensure we have enough memory per worker
            if available_memory_gb < 1.0 and allocated > 2:
                allocated = max(1, allocated // 2)
                logger.warning(f"Workers reduced due to low memory: {available_memory_gb:.1f}GB available")

            return allocated

        except Exception as e:
            logger.warning(f"Worker allocation error: {e}, using requested: {requested_workers}")
            return requested_workers

    def discover_files(self) -> List[Path]:
        """Discover input files matching pattern."""
        files = sorted(self.input_folder.glob(self.file_pattern))
        self.progress_callback(f"📁 Found {len(files)} files matching '{self.file_pattern}'")
        return files

    def process_batch(self, skill_meta: Dict, base_params: Dict) -> Dict:
        """
        Process all files in batch.

        Args:
            skill_meta: Skill metadata
            base_params: Base parameters (will be extended for each file)

        Returns:
            Batch results with aggregated stats
        """
        files = self.discover_files()

        if not files:
            self.progress_callback("⚠️ No files found to process")
            return self._empty_batch_result(skill_meta, files)

        self.progress_callback(f"🚀 Starting batch processing ({self.batch_mode} mode)...")

        # Process files
        if self.batch_mode == "parallel":
            results = self._process_parallel(files, skill_meta, base_params)
        elif self.batch_mode == "threaded":
            results = self._process_threaded(files, skill_meta, base_params)
        else:  # sequential
            results = self._process_sequential(files, skill_meta, base_params)

        # Aggregate results
        return self._aggregate_results(skill_meta, files, results)

    def _process_parallel(
        self, files: List[Path], skill_meta: Dict, base_params: Dict
    ) -> List[BatchResult]:
        """Process files in parallel using multiprocessing.

        Note: Progress callback is not passed to workers (can't pickle local functions).
        Individual file progress is reported as results complete.
        """
        results = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks (without progress_callback - it can't be passed to worker processes)
            future_to_file = {
                executor.submit(
                    self._process_single_file, file, skill_meta, base_params
                ): file
                for file in files
            }

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    completed += 1
                    self.progress_callback(
                        f"✓ [{completed}/{len(files)}] {file.name}"
                    )
                except Exception as e:
                    results.append(
                        BatchResult(
                            file_path=str(file),
                            status="error",
                            error=str(e),
                        )
                    )
                    self.progress_callback(f"✗ [{completed}/{len(files)}] {file.name}: {str(e)}")

        return results

    def _process_threaded(
        self, files: List[Path], skill_meta: Dict, base_params: Dict
    ) -> List[BatchResult]:
        """Process files using thread pool (for I/O-bound operations)."""
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(
                    self._process_single_file, file, skill_meta, base_params
                ): file
                for file in files
            }

            completed = 0
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    completed += 1
                    self.progress_callback(
                        f"✓ [{completed}/{len(files)}] {file.name}"
                    )
                except Exception as e:
                    results.append(
                        BatchResult(
                            file_path=str(file),
                            status="error",
                            error=str(e),
                        )
                    )
                    self.progress_callback(f"✗ [{completed}/{len(files)}] {file.name}: {str(e)}")

        return results

    def _process_sequential(
        self, files: List[Path], skill_meta: Dict, base_params: Dict
    ) -> List[BatchResult]:
        """Process files sequentially (for debugging/testing)."""
        results = []

        for idx, file in enumerate(files, 1):
            try:
                result = self._process_single_file(file, skill_meta, base_params)
                results.append(result)
                self.progress_callback(f"✓ [{idx}/{len(files)}] {file.name}")
            except Exception as e:
                results.append(
                    BatchResult(
                        file_path=str(file),
                        status="error",
                        error=str(e),
                    )
                )
                self.progress_callback(f"✗ [{idx}/{len(files)}] {file.name}: {str(e)}")

        return results

    def _process_single_file(
        self, file: Path, skill_meta: Dict, base_params: Dict
    ) -> BatchResult:
        """Process a single file."""
        try:
            # Create file-specific params
            params = base_params.copy()
            params["input_path"] = str(file)

            # Create output subfolder for this file
            file_output_folder = self.output_folder / file.stem
            file_output_folder.mkdir(parents=True, exist_ok=True)
            params["output_path"] = str(file_output_folder)

            # Execute skill
            result = self.skill_executor(skill_meta, params, progress_callback=None)

            return BatchResult(
                file_path=str(file),
                status="success" if "error" not in result else "error",
                summary=result.get("summary", ""),
                output_files=result.get("output_files", []),
                data=result.get("data", {}),
                error=result.get("error"),
            )

        except Exception as e:
            return BatchResult(
                file_path=str(file),
                status="error",
                error=str(e),
            )

    def _aggregate_results(
        self, skill_meta: Dict, files: List[Path], results: List[BatchResult]
    ) -> Dict:
        """Aggregate batch results into summary report."""
        successful = [r for r in results if r.status == "success"]
        failed = [r for r in results if r.status == "error"]

        # Aggregate data
        aggregated_data = {
            "total_files": len(files),
            "successful": len(successful),
            "failed": len(failed),
            "batch_mode": self.batch_mode,
            "max_workers": self.max_workers,
            "skill": skill_meta.get("name"),
            "results": [asdict(r) for r in results],
        }

        # Build summary markdown
        summary_md = self._build_summary_markdown(aggregated_data, successful, failed)

        # Save detailed results
        results_json_path = self.output_folder / "batch_results.json"
        results_json_path.write_text(json.dumps(aggregated_data, indent=2))

        # Post-processing: Create consolidated folder for Pandas to PySpark Converter
        if skill_meta.get("name") == "Pandas to PySpark Converter":
            self._create_consolidated_pyspark_folder()

        return {
            "summary": summary_md,
            "output_files": [str(results_json_path)],
            "data": aggregated_data,
        }

    def _build_summary_markdown(
        self, aggregated_data: Dict, successful: List[BatchResult], failed: List[BatchResult]
    ) -> str:
        """Build markdown summary report."""
        markdown = f"""# Batch Processing Report

## Summary
- **Total Files:** {aggregated_data['total_files']}
- **Successful:** {aggregated_data['successful']} ✓
- **Failed:** {aggregated_data['failed']} ✗
- **Processing Mode:** {aggregated_data['batch_mode'].upper()}
- **Max Workers:** {aggregated_data['max_workers']}
- **Skill:** {aggregated_data['skill']}

"""

        if successful:
            markdown += "## Successful Processes\n"
            for result in successful:
                markdown += f"- ✓ {Path(result.file_path).name}\n"
            markdown += "\n"

        if failed:
            markdown += "## Failed Processes\n"
            for result in failed:
                markdown += f"- ✗ {Path(result.file_path).name}: {result.error}\n"
            markdown += "\n"

        markdown += "## Detailed Results\nSee `batch_results.json` for complete details."

        return markdown

    def _create_consolidated_pyspark_folder(self) -> None:
        """Create consolidated folder with converted PySpark files after batch completes."""
        import shutil

        try:
            output_path = Path(self.output_folder)
            consolidated_folder = output_path / "converted_pyspark_files"
            consolidated_folder.mkdir(parents=True, exist_ok=True)

            # Find all .py files in subdirectories
            converted_files = list(output_path.glob("*/*.py"))

            for py_file in converted_files:
                # Skip if already in consolidated folder
                if py_file.parent.name == "converted_pyspark_files":
                    continue

                dest_file = consolidated_folder / py_file.name
                try:
                    shutil.copy2(str(py_file), str(dest_file))
                except Exception:
                    pass  # Silent fail - don't interrupt batch processing
        except Exception:
            pass  # Silently skip if consolidation fails

    def _empty_batch_result(self, skill_meta: Dict, files: List[Path]) -> Dict:
        """Return empty batch result when no files found."""
        return {
            "summary": f"# Batch Processing Report\n\nNo files found matching pattern.",
            "output_files": [],
            "data": {
                "total_files": 0,
                "successful": 0,
                "failed": 0,
                "skill": skill_meta.get("name"),
            },
        }
