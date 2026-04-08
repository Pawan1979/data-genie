# Data Copy Validator

## description
Copy data files between paths with checksum validation and incremental progress tracking. Verifies integrity after transfer and generates a manifest for audit trail purposes.

## intent_keywords
copy data, transfer file, data transfer, file copy, validate copy, checksums, data integrity, incremental backup, archive data, data migration copy

## entry_point
data_copy.py :: run(params: dict, progress_callback=None) -> dict

## inputs
- input_path  (str, required): source file path
- output_path (str, required): destination folder
- verify_checksum (boolean, optional, default: true): verify SHA256 after copy

## outputs
- summary      (str): markdown copy report
- output_files (list): absolute paths of [copy_manifest.json, copy_report.md]
- data         (dict): source_size, dest_size, checksum_match, copy_duration_seconds

## when_to_use
- "Copy this file and verify the transfer"
- "Backup this data file with integrity checks"
- "Transfer data from source to destination safely"
- "Create a copy of this file and confirm it matches"
- "Copy data between folders with checksums"
