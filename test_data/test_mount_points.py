"""
Test 3: Databricks Mount Points and Path Mapping
Tests: /mnt/ paths, /dbfs/mnt/ paths, path variable resolution
Also tests: Databricks magic commands (!, %, # MAGIC)
"""

import pandas as pd
import numpy as np

# ============================================================================
# TEST 3.1: Standard Databricks Mount Point Paths
# ============================================================================

# Read from standard mount points
customers_mnt = pd.read_csv('/mnt/data/customers.csv')
products_mnt = pd.read_csv('/mnt/data/products.csv')
orders_mnt = pd.read_csv('/mnt/data/orders.csv')

# Read from DBFS mount points
inventory_dbfs = pd.read_parquet('/dbfs/mnt/inventory/current.parquet')
archived_data = pd.read_parquet('/dbfs/mnt/archive/old_orders.parquet')

print("Mount point data loaded successfully")


# ============================================================================
# TEST 3.2: Path Variable Resolution
# ============================================================================

# Dynamic path construction
data_path = '/mnt/data'
output_path = '/mnt/output'
archive_path = '/dbfs/mnt/archive'

# Read with dynamic paths
df_dynamic = pd.read_csv(f'{data_path}/sales.csv')
products_dynamic = pd.read_parquet(f'{data_path}/products.parquet')
historical = pd.read_parquet(f'{archive_path}/history.parquet')

print("Dynamic path resolution completed")


# ============================================================================
# TEST 3.3: Path Mapping Scenarios
# ============================================================================

# Paths that need mapping (old → new)
# Original: /mnt/old_location/data
# Mapped to: /mnt/new_location/data

# Read from old paths (will be mapped during conversion)
old_data = pd.read_csv('/mnt/old_location/data/customers.csv')
old_products = pd.read_parquet('/mnt/old_location/data/products.parquet')

# Read from archive paths (will be mapped)
archived = pd.read_csv('/mnt/archive_old/sales.csv')

# Write to new paths
old_data.to_csv('/mnt/old_location/output/customers_processed.csv', index=False)
old_products.to_parquet('/mnt/old_location/output/products_processed.parquet')

print("Path mapping scenarios completed")


# ============================================================================
# TEST 3.4: Multiple Mount Point Operations
# ============================================================================

# Read from different mount points
source1 = pd.read_csv('/mnt/data/region1.csv')
source2 = pd.read_csv('/mnt/data/region2.csv')
source3 = pd.read_csv('/mnt/data/region3.csv')

# Combine data
combined = pd.concat([source1, source2, source3], ignore_index=True)

# Write to mount point
combined.to_parquet('/mnt/output/combined_regions.parquet')

# Write JSON to mount point
combined.to_json('/mnt/output/combined_regions.json', orient='records', lines=True)

print("Multiple mount point operations completed")


# ============================================================================
# TEST 3.5: Mount Point with Transformations
# ============================================================================

# Read from mount point
raw_sales = pd.read_csv('/mnt/raw/sales.csv')

# Transform
raw_sales['date'] = pd.to_datetime(raw_sales['date'])
raw_sales['amount'] = raw_sales['amount'].astype(float)
raw_sales['quantity'] = raw_sales['quantity'].astype(int)

# Aggregate
monthly_summary = raw_sales.groupby(raw_sales['date'].dt.to_period('M')).agg({
    'amount': ['sum', 'mean', 'count'],
    'quantity': 'sum'
})

# Write processed data to mount point
monthly_summary.to_parquet('/mnt/processed/monthly_summary.parquet')
raw_sales.to_csv('/mnt/processed/sales_cleaned.csv', index=False)

print("Mount point transformations completed")


# ============================================================================
# TEST 3.6: DBFS Direct Access Patterns
# ============================================================================

# Read directly from DBFS paths
dbfs_data = pd.read_parquet('/dbfs/data/analytics.parquet')
dbfs_archive = pd.read_csv('/dbfs/archive/historical.csv')

# Read from Databricks delta table location (as parquet)
delta_data = pd.read_parquet('/dbfs/user/hive/warehouse/my_table')

# Write to DBFS
dbfs_data.to_parquet('/dbfs/user/output/processed.parquet')

print("DBFS direct access completed")


# ============================================================================
# TEST 3.7: Nested Mount Point Paths
# ============================================================================

# Deeply nested paths
nested_source = pd.read_csv('/mnt/data/year/2025/month/04/day/09/sales.csv')
nested_product = pd.read_parquet('/mnt/data/year/2025/metadata/products.parquet')

# Merge nested data
merged_nested = nested_source.merge(nested_product, on='product_id')

# Write to nested mount path
merged_nested.to_csv('/mnt/output/year/2025/month/04/day/09/processed_sales.csv', index=False)

print("Nested mount point paths completed")


# ============================================================================
# TEST 3.8: Path Mapping with Filtering
# ============================================================================

# Read from old mount location
old_inventory = pd.read_parquet('/mnt/old_location/inventory.parquet')

# Filter data
active_items = old_inventory[old_inventory['status'] == 'active']
high_quantity = active_items[active_items['quantity'] > 100]

# Write to new mount location
high_quantity.to_csv('/mnt/old_location/output/high_quantity_items.csv', index=False)

# Write to archive
high_quantity.to_parquet('/mnt/archive_old/backup/high_quantity.parquet')

print("Path mapping with filtering completed")


# ============================================================================
# TEST 3.9: Mixed Mount Points in Single Pipeline
# ============================================================================

# Read from multiple mount types
standard_mount = pd.read_csv('/mnt/data/standard.csv')
dbfs_mount = pd.read_csv('/dbfs/mnt/dbfs_data.csv')

# Process both
standard_mount['source'] = 'standard'
dbfs_mount['source'] = 'dbfs'

# Combine
combined_data = pd.concat([standard_mount, dbfs_mount])

# Write results to both mount types
combined_data.to_csv('/mnt/output/combined.csv', index=False)
combined_data.to_parquet('/dbfs/mnt/output/combined.parquet')

print("Mixed mount points pipeline completed")


# ============================================================================
# TEST 3.10: Mount Point Path Summary and Output
# ============================================================================

print("\n=== Mount Point Summary ===")
print(f"Standard mount data shape: {customers_mnt.shape}")
print(f"DBFS mount data shape: {inventory_dbfs.shape}")
print(f"Dynamic path resolution: {df_dynamic.shape}")
print(f"Combined regions shape: {combined.shape}")
print(f"Processed data written to: /mnt/processed/")
print(f"DBFS output written to: /dbfs/user/output/")

print("\nAll mount point tests completed successfully")
