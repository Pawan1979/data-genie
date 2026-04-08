# Pandas to PySpark Converter

## description
Convert Pandas-based Python scripts or Jupyter notebooks to PySpark equivalents. Maps common Pandas patterns to DataFrame APIs and highlights code sections requiring manual review or optimization.

## intent_keywords
convert pandas, pyspark, migration, pandas to spark, refactor pandas, spark dataframes, databricks, distributed computing, pandas conversion, spark migration, code conversion, script migration

## entry_point
converter.py :: run(params: dict, progress_callback=None) -> dict

## inputs
- input_path  (str, required): path to Python script (.py) or Jupyter notebook (.ipynb)
- output_path (str, required): folder to write output files
- target_runtime (string, optional, default: "databricks", enum: databricks/emr/hdp): target PySpark runtime
- python_version (string, optional, default: "3.9"): target Python version

## outputs
- summary      (str): markdown conversion report
- output_files (list): absolute paths of [converted_script.py, conversion_notes.md, report.md]
- data         (dict): conversion_count, patterns_mapped, manual_review_items, warnings

## when_to_use
- "Convert this Pandas script to PySpark"
- "How do I migrate my Pandas notebook to Spark?"
- "I need to refactor this code to run on Databricks"
- "Help me convert Pandas operations to DataFrame API"
- "Generate a Spark version of my analysis script"
