# Data Discovery

## description
Profile and explore data source structure by inferring schema, computing null rates, detecting data types, and generating quality metrics. Identifies column distributions and flags potential quality issues for data engineers to investigate.

## intent_keywords
discover, profile, schema, explore, analyse data, data structure, what columns, what tables, data quality, null check, row count, data inventory, source analysis, data profiling, understand dataset, data types, column analysis, quality assessment, data exploration

## entry_point
discovery.py :: run(params: dict, progress_callback=None) -> dict

## inputs
- input_path  (str, required): path to data file (CSV, Parquet, JSON, Delta)
- output_path (str, required): folder to write output files
- sample_size (integer, optional, default: 1000): number of rows to sample for profiling
- file_type (string, optional, auto-detect): enum: csv/parquet/json/delta/auto

## outputs
- summary      (str): markdown discovery report
- output_files (list): absolute paths of [discovery_report.xlsx, schema.json, report.md]
- data         (dict): structured schema, null rates, row count, quality flags, column stats

## when_to_use
- "Profile this CSV file and tell me about its structure"
- "I need to understand what columns are in my data source"
- "Can you discover the schema and check data quality?"
- "Explore this data file and flag any quality issues"
- "Generate a data inventory for this source"
