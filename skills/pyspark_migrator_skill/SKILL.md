# PySpark Migrator

## description
Scan PySpark code for Databricks migration incompatibilities, detect deprecated APIs, and assess infrastructure refactoring risk. Generates a migration plan with detailed change recommendations and risk categories.

## intent_keywords
migrate pyspark, databricks migration, spark migration, pyspark upgrade, deprecated apis, migration assessment, spark refactoring, infrastructure migration, spark compatibility, migration planning

## entry_point
migrator.py :: run(params: dict, progress_callback=None) -> dict

## inputs
- input_path  (str, required): path to PySpark script or code file
- output_path (str, required): folder to write output files
- target_databricks_runtime (string, optional, default: "13.3 LTS"): target runtime version
- migration_mode (string, optional, default: "as_is", enum: as_is/optimised): assessment depth

## outputs
- summary      (str): markdown migration plan
- output_files (list): absolute paths of [migrated_script.py, migration_plan.md, risk_report.xlsx]
- data         (dict): incompatibility_count, risk_assessment, api_changes, migration_steps

## when_to_use
- "Assess this PySpark code for Databricks migration"
- "What deprecated APIs are in my Spark script?"
- "I need a migration plan for this cluster code"
- "Check if my PySpark code will work on Databricks"
- "Generate a compatibility report for our migration"
