"""
Test 5: Edge Cases and Special Scenarios
Tests: Comment preservation, formatting preservation, Databricks magic commands
Also tests: Error handling, large files, special characters, encoding
"""

import pandas as pd
import numpy as np

# ============================================================================
# TEST 5.1: Comments and Documentation Preservation
# ============================================================================

# This is a comment that should be preserved during conversion
# Multiple comments
# - Point 1
# - Point 2

# Single line comment before operation
customers = pd.read_csv('data/customers.csv')  # Inline comment

# Multi-line operation with comments
orders = pd.read_csv(
    'data/orders.csv',  # Order data file
    sep=',',            # Delimiter
    header=0            # Header row
)

# Comment after operation
products = pd.read_parquet('data/products.parquet')  # Product metadata

# Multiple operations with comments
"""
Block comment explaining the section
This describes what we're doing
"""
sales = pd.read_csv('data/sales.csv')

print("Comments and documentation preserved")


# ============================================================================
# TEST 5.2: Special Characters and Encoding
# ============================================================================

# Read with special characters in paths
special_data = pd.read_csv('data/special-characters.csv')
unicode_data = pd.read_csv('data/übung_data.csv', encoding='utf-8')
latin_data = pd.read_csv('data/données_françaises.csv', encoding='latin-1')

# Column names with special characters
data_with_special_cols = pd.read_csv('data/data-with-special-cols.csv')
# Columns might be named: 'First Name', '@id', '#count', 'price($)'

# Data with special characters
data_with_special_cols['First Name'] = data_with_special_cols['First Name'].str.upper()
data_with_special_cols['@id'] = data_with_special_cols['@id'].astype(str)

# Write with special characters
special_data.to_csv('output/special_chars_output.csv', encoding='utf-8')
unicode_data.to_csv('output/unicode_output.csv', encoding='utf-8')

print("Special characters and encoding handled")


# ============================================================================
# TEST 5.3: Large File Scenarios
# ============================================================================

# Reading large file in chunks
# Note: Simulated with smaller size, but syntax should support large files
large_file_chunks = []
for chunk in pd.read_csv('data/large_file.csv', chunksize=10000):
    # Process chunk
    chunk['processed'] = chunk['value'] * 2
    large_file_chunks.append(chunk)

# Concatenate all chunks
large_file_complete = pd.concat(large_file_chunks, ignore_index=True)

# Alternative: Read entire large file
large_file = pd.read_csv('data/huge_dataset.csv', dtype={'id': str, 'amount': float})

# Operations on large file
large_file_summary = large_file.groupby('category').agg({
    'amount': ['sum', 'mean', 'count']
})

# Write large file
large_file_complete.to_parquet('output/large_file_processed.parquet', compression='snappy')
large_file.to_csv('output/huge_dataset_processed.csv', index=False, chunksize=50000)

print("Large file scenarios handled")


# ============================================================================
# TEST 5.4: Databricks Magic Commands (Comments to be preserved)
# ============================================================================

# %sql SELECT * FROM my_table
# This magic command should be preserved as a comment during conversion

# %run /Workspace/setup
# Another magic command reference

# %sh ls -la /mnt/data
# Shell magic command reference

# %pip install package_name
# Pip install magic command

# MAGIC # This is a MAGIC comment from Databricks notebooks
# It should be preserved during conversion

print("Magic command placeholders defined")


# ============================================================================
# TEST 5.5: Error Handling and Try-Except Patterns
# ============================================================================

# Try-except blocks should be handled properly during conversion
try:
    problematic_data = pd.read_csv('data/nonexistent.csv')
except FileNotFoundError:
    print("File not found - using default data")
    problematic_data = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})

try:
    # Try to read with wrong encoding
    encoded_data = pd.read_csv('data/encoded.csv', encoding='ascii')
except UnicodeDecodeError:
    print("Encoding error - retrying with UTF-8")
    encoded_data = pd.read_csv('data/encoded.csv', encoding='utf-8')

# Error handling in transformations
try:
    result = problematic_data['value'].astype(int)
except ValueError:
    print("Type conversion failed - using default values")
    result = problematic_data['value'].fillna(0).astype(int)

print("Error handling patterns preserved")


# ============================================================================
# TEST 5.6: Formatting and Indentation Edge Cases
# ============================================================================

# Long line that exceeds normal formatting
very_long_variable_name_that_explains_the_purpose = pd.read_csv('data/input.csv', sep=',', header=0, encoding='utf-8', quotechar='"', comment='#', dtype={'id': str, 'amount': float})

# Multi-line operations with various indentation levels
result = (
    pd.read_csv('data/sales.csv')
    .merge(pd.read_csv('data/customers.csv'), on='customer_id')
    .groupby('region')
    .agg({'amount': 'sum', 'quantity': 'mean'})
    .reset_index()
    .sort_values('amount', ascending=False)
)

# Nested parentheses
nested_result = (
    customers
    .copy()
    .assign(
        year=lambda df: pd.to_datetime(df['date']).dt.year,
        month=lambda df: pd.to_datetime(df['date']).dt.month
    )
    .groupby(['year', 'month'])
    .size()
    .reset_index(name='count')
)

print("Formatting and indentation preserved")


# ============================================================================
# TEST 5.7: Null and NaN Handling
# ============================================================================

# Data with various null representations
data_with_nulls = pd.read_csv('data/nullable.csv', na_values=['NA', 'N/A', 'null', '', 'None'])

# Check nulls
null_counts = data_with_nulls.isnull().sum()
null_percentage = (data_with_nulls.isnull().sum() / len(data_with_nulls)) * 100

# Fill nulls with different strategies
filled_forward = data_with_nulls.fillna(method='ffill')  # Forward fill
filled_backward = data_with_nulls.fillna(method='bfill')  # Backward fill
filled_mean = data_with_nulls.fillna(data_with_nulls.mean())  # Fill with mean
filled_constant = data_with_nulls.fillna(0)  # Fill with constant

# Drop nulls
no_nulls = data_with_nulls.dropna()
partial_nulls = data_with_nulls.dropna(subset=['critical_column'])

print("Null handling preserved")


# ============================================================================
# TEST 5.8: Data Type Edge Cases
# ============================================================================

# Various data types
mixed_types = pd.read_csv('data/mixed_types.csv')

# Explicit type conversion
mixed_types['id'] = mixed_types['id'].astype(str)
mixed_types['amount'] = mixed_types['amount'].astype(float)
mixed_types['quantity'] = mixed_types['quantity'].astype(int)
mixed_types['is_active'] = mixed_types['is_active'].astype(bool)
mixed_types['date'] = pd.to_datetime(mixed_types['date'])

# Category type
mixed_types['category'] = mixed_types['category'].astype('category')
mixed_types['status'] = pd.Categorical(mixed_types['status'], categories=['pending', 'completed', 'failed'])

# Nullable integers (Int64 instead of int64)
nullable_int = mixed_types['quantity'].astype('Int64')

# String dtype
string_col = mixed_types['description'].astype('string')

print("Data type edge cases handled")


# ============================================================================
# TEST 5.9: Complex Lambda and Apply Operations
# ============================================================================

# Lambda functions with various complexity levels
simple_lambda = customers['amount'].apply(lambda x: x * 2)
conditional_lambda = customers['amount'].apply(lambda x: 'High' if x > 100 else 'Low')
nested_lambda = customers.apply(lambda row: f"{row['name']}: {row['amount']}", axis=1)

# Multi-condition lambda
complex_lambda = customers['amount'].apply(
    lambda x: 'Premium' if x > 500 else ('Standard' if x > 100 else 'Basic')
)

# Apply with additional parameters (not directly supported in Spark, needs custom handling)
def calculate_discount(amount, rate=0.1):
    return amount * rate

customers['discount'] = customers['amount'].apply(lambda x: calculate_discount(x, rate=0.15))

# String operations with lambda
customers['name_processed'] = customers['name'].apply(
    lambda x: x.strip().lower().replace(' ', '_') if pd.notna(x) else 'unknown'
)

print("Complex lambda and apply operations preserved")


# ============================================================================
# TEST 5.10: Output and Edge Case Results
# ============================================================================

# Save results with various options
result.to_csv('output/edge_case_result.csv', index=False, encoding='utf-8')
nested_result.to_parquet('output/nested_result.parquet', compression='gzip')
data_with_nulls.to_csv('output/nullable_preserved.csv', quotechar='"', escapechar='\\')

# Excel with special formatting (formatting will be lost in Spark, but file structure preserved)
with pd.ExcelWriter('output/edge_cases_multi.xlsx') as writer:
    result.to_excel(writer, sheet_name='Result')
    nested_result.to_excel(writer, sheet_name='Nested')
    data_with_nulls.to_excel(writer, sheet_name='Nulls')

print("\n=== Edge Cases Summary ===")
print("✓ Comments and documentation preserved")
print("✓ Special characters and encoding handled")
print("✓ Large file scenarios supported")
print("✓ Databricks magic commands preserved")
print("✓ Error handling patterns maintained")
print("✓ Formatting and indentation preserved")
print("✓ Null handling comprehensive")
print("✓ Data type conversions supported")
print("✓ Lambda and apply operations converted")
print("✓ Multi-format output generated")

print("\nEdge cases test completed successfully")
