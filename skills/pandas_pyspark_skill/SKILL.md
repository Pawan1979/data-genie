# Pandas to PySpark Converter

## description
Convert pandas Python code to PySpark equivalents with auto-dependency installation, hierarchical folder support, and optional Spark testing. Automatically installs missing dependencies (astor, pandas), detects PySpark availability, and gracefully handles Databricks integration. Processes single files or entire folder hierarchies recursively.

## intent_keywords
convert pandas pyspark migration spark dataframe read write csv json parquet excel batch convert multiple files code transformation pandas migration databricks hierarchical folder processing auto-install dependencies

## entry_point
converter.py :: run(params: dict, progress_callback=None) -> dict

## tools
This skill has **2 integrated tools**:

### Tool 1: run() - Convert Pandas to PySpark

#### Inputs
- input_path (str, required): Path to Python file or folder containing pandas code
- output_path (str, required): Folder for converted code and reports
- path_mapping (dict, optional): Map old mount paths to new: {'/mnt/old': '/mnt/new'}
- auto_install_deps (bool, optional, default=True): Auto-install missing dependencies
- enable_spark_testing (bool, optional, default=True): Enable side-by-side Spark testing
- auto_consolidate (bool, optional, default=True): **Automatically consolidate converted files into converted_pyspark_files/ folder**

#### Outputs
- summary (str): Markdown report with operations, changes, and Spark status
- output_files (list): Converted code(s), discovery report, conversion summary, optional Spark test results
- data (dict): Metrics - operations_found, changes_made, conversion_confidence, spark_available, spark_tested, consolidated_folder, consolidated_file_count

---

### Tool 2: consolidate() - Organize Converted Files
```
converter.py :: consolidate(params: dict, progress_callback=None) -> dict
```

#### Inputs
- output_path (str, required): Path to existing converter output folder
- consolidate_mode (str, optional, default='files_only'): 
  - 'files_only': Only converted .py files
  - 'with_reports': .py files + conversion reports
  - 'clean_output': Complete package with all metadata

#### Outputs
- summary (str): Consolidation report with file listing
- output_files (list): Path to consolidated folder
- data (dict): Metrics - consolidated_files, reports_included, consolidation_mode, consolidated_folder

## when_to_use

### Tool 1: run() - Conversion
- "Convert my pandas script to Spark"
- "Migrate this Python code from pandas to pyspark"
- "Find all pandas operations in my code"
- "Batch convert all Python files in a folder"
- "Convert entire project hierarchy to Spark"
- "Help me refactor this pandas code for Databricks"
- "What pandas operations are in this codebase?"

### Tool 2: consolidate() - File Organization
- "Organize my converted PySpark files"
- "Create a clean copy of converted files"
- "Bundle converted code without reports"
- "Prepare converted files for delivery"
- "Create a clean output package"
- "Consolidate files from batch conversion"

## features

### Tool 1: run() - Conversion Features
✅ Auto-installs missing optional dependencies (no manual pip install needed)
✅ Processes single files or entire folder hierarchies (recursive)
✅ Detects PySpark/Databricks availability automatically
✅ Graceful fallback if Spark unavailable (conversion still works)
✅ Optional side-by-side testing (pandas vs Spark comparison)
✅ Comprehensive discovery and validation reports
✅ Support for Databricks path mapping (/mnt/ detection)
✅ AST-based conversion (preserves code formatting)
✅ Detailed metrics and confidence scoring
✅ **Auto-creates consolidated folder BY DEFAULT** (converted_pyspark_files/)
✅ Consolidation can be disabled with auto_consolidate=False
✅ Batch parallel processing (up to 4 workers)

### Tool 2: consolidate() - Organization Features
✅ Three consolidation modes (files_only, with_reports, clean_output)
✅ Works on existing converter output
✅ Organizes files into single clean folder
✅ Includes optional conversion reports
✅ Complete metadata packaging
✅ Ready-to-deliver output
