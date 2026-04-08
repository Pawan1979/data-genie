# Data Validation

## description
Compare source and target datasets to detect schema mismatches, row count discrepancies, and data integrity issues. Produces a reconciliation report with detailed differences flagged by severity.

## intent_keywords
validate data, reconciliation, compare data, data matching, validation check, integrity check, data comparison, quality validation, test data, data audit

## entry_point
validator.py :: run(params: dict, progress_callback=None) -> dict

## inputs
- input_path  (str, required): path to source data file
- output_path (str, required): folder to write output files
- target_path (str, optional): path to target data file for comparison
- sample_percent (integer, optional, default: 10): percentage of rows to sample for detailed comparison

## outputs
- summary      (str): markdown validation report
- output_files (list): absolute paths of [validation_results.xlsx, reconciliation_report.md]
- data         (dict): row_count_match, schema_match, data_discrepancies, quality_issues

## when_to_use
- "Validate that this data matches the source"
- "Compare two datasets and find differences"
- "Check data integrity after a migration"
- "Generate a reconciliation report for these files"
- "Validate that the copy matches the original"
