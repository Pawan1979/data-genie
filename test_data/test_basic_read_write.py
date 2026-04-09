"""
Test 1: Basic Read/Write Operations
Tests: pd.read_csv, pd.read_parquet, pd.read_json, pd.read_excel
       df.to_csv, df.to_parquet, df.to_json, df.to_excel
"""

import pandas as pd
import numpy as np

# ============================================================================
# TEST 1.1: CSV Read Operations
# ============================================================================

# Read CSV with default options
df_csv_basic = pd.read_csv('data/sales.csv')

# Read CSV with custom separator
df_csv_pipe = pd.read_csv('data/products.csv', sep='|', header=0)

# Read CSV with encoding
df_csv_encoded = pd.read_csv('data/international_data.csv', encoding='utf-8')

# Read CSV with quote character
df_csv_quoted = pd.read_csv('data/quoted_data.csv', quotechar='"')

# Read CSV with comment handling
df_csv_comments = pd.read_csv('data/config.csv', comment='#', header=0)

print("CSV files loaded successfully")
print(f"Sales shape: {df_csv_basic.shape}")
print(f"Products shape: {df_csv_pipe.shape}")


# ============================================================================
# TEST 1.2: Parquet Read Operations
# ============================================================================

# Read parquet file (basic)
df_parquet_basic = pd.read_parquet('data/inventory.parquet')

# Read parquet with specific columns
df_parquet_filtered = pd.read_parquet('data/orders.parquet', columns=['order_id', 'customer_id'])

print(f"Inventory shape: {df_parquet_basic.shape}")


# ============================================================================
# TEST 1.3: JSON Read Operations
# ============================================================================

# Read JSON (regular format)
df_json_regular = pd.read_json('data/customers.json')

# Read JSON lines format (one JSON object per line)
df_json_lines = pd.read_json('data/events.json', lines=True)

# Read JSON with orient parameter
df_json_split = pd.read_json('data/data.json', orient='split')

print(f"Customers JSON shape: {df_json_regular.shape}")
print(f"Events JSON shape: {df_json_lines.shape}")


# ============================================================================
# TEST 1.4: Excel Read Operations
# ============================================================================

# Read Excel file (basic)
df_excel_basic = pd.read_excel('data/report.xlsx', sheet_name='Sheet1')

# Read Excel with specific sheet
df_excel_sheet = pd.read_excel('data/report.xlsx', sheet_name='Sales')

# Read Excel with header specification
df_excel_header = pd.read_excel('data/data.xlsx', sheet_name=0, header=0)

print(f"Excel report shape: {df_excel_basic.shape}")


# ============================================================================
# TEST 1.5: CSV Write Operations
# ============================================================================

# Write CSV (basic)
df_csv_basic.to_csv('output/sales_processed.csv')

# Write CSV with custom separator
df_csv_pipe.to_csv('output/products_pipe.csv', sep='|', header=True)

# Write CSV with encoding
df_csv_encoded.to_csv('output/international_output.csv', encoding='utf-8')

# Write CSV without index
df_csv_basic.to_csv('output/sales_no_index.csv', index=False)

# Write CSV with custom line terminator
df_csv_basic.to_csv('output/sales_crlf.csv', lineterminator='\r\n')

print("CSV files written successfully")


# ============================================================================
# TEST 1.6: Parquet Write Operations
# ============================================================================

# Write parquet (basic)
df_parquet_basic.to_parquet('output/inventory_processed.parquet')

# Write parquet with compression
df_parquet_filtered.to_parquet('output/orders_compressed.parquet')

print("Parquet files written successfully")


# ============================================================================
# TEST 1.7: JSON Write Operations
# ============================================================================

# Write JSON (regular format)
df_json_regular.to_json('output/customers_output.json')

# Write JSON lines format
df_json_lines.to_json('output/events_lines.json', orient='records', lines=True)

print("JSON files written successfully")


# ============================================================================
# TEST 1.8: Excel Write Operations
# ============================================================================

# Write Excel (basic)
df_excel_basic.to_excel('output/report_output.xlsx', sheet_name='Sheet1')

# Write Excel with custom sheet name
df_excel_sheet.to_excel('output/sales_report.xlsx', sheet_name='Sales')

# Write Excel without index
df_excel_basic.to_excel('output/report_no_index.xlsx', index=False)

print("Excel files written successfully")


# ============================================================================
# TEST 1.9: Data Inspection (no conversion needed)
# ============================================================================

# Display data info
print("\n=== Data Summary ===")
print(df_csv_basic.info())
print(df_csv_basic.describe())
print(df_csv_basic.head())

# Check for null values
print(f"\nNull values: {df_csv_basic.isnull().sum()}")

# Display data types
print(f"\nData types:\n{df_csv_basic.dtypes}")
