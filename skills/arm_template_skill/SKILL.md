# ARM Template Analyser

## description
Parse Azure ARM template JSON files and extract a complete resource inventory with dependency mapping and risk detection. Produces Excel workbooks with sorted resource tables, risk flags by severity, and a markdown analysis report.

## intent_keywords
arm, azure, arm template, resource inventory, azure resources, infrastructure, iac, json template, arm analysis, resource group, azure deployment, arm json, extract resources, azure audit, resource extraction, arm file, template analysis, azure infrastructure, bicep

## entry_point
arm_analyzer.py :: run(params: dict, progress_callback=None) -> dict

## inputs
- input_path  (str, required): path to ARM template JSON file
- output_path (str, required): folder to write output files
- include_child_resources (boolean, optional, default: true): parse nested child resources
- min_risk_severity (string, optional, default: "LOW", enum: CRITICAL/HIGH/MEDIUM/LOW): minimum risk flag severity to report

## outputs
- summary      (str): markdown report for chat
- output_files (list): absolute paths of [inventory.xlsx, inventory_summary.json, report.md]
- data         (dict): structured result containing resource_count, resource_types, risk_flags, resources, dependencies, parameters, outputs

## when_to_use
- "Analyse my ARM template and extract resource inventory"
- "I need to audit what resources are deployed in this ARM JSON"
- "Can you parse this Azure infrastructure template and find risks?"
- "Generate an inventory of all resources in this ARM file"
- "Extract parameters and outputs from my deployment template"
