"""
Test 4: Complex Data Pipeline
Tests: Real-world data pipeline combining reads, transforms, joins, aggregations, writes
Simulates actual ETL/Analytics workflows
"""

import pandas as pd
import numpy as np

# ============================================================================
# TEST 4.1: Multi-Source ETL Pipeline
# ============================================================================

# Load data from multiple sources
customers = pd.read_csv('data/customers.csv')
orders = pd.read_parquet('data/orders.parquet')
order_items = pd.read_json('data/order_items.json', lines=True)
products = pd.read_excel('data/products.xlsx', sheet_name='Products')

print("Step 1: Data sources loaded")


# ============================================================================
# TEST 4.2: Data Cleaning and Preparation
# ============================================================================

# Clean customer data
customers['email'] = customers['email'].str.lower().str.strip()
customers['phone'] = customers['phone'].str.replace('-', '').str.replace(' ', '')
customers['created_date'] = pd.to_datetime(customers['created_date'])
customers = customers.dropna(subset=['customer_id'])

# Clean orders
orders['order_date'] = pd.to_datetime(orders['order_date'])
orders['amount'] = orders['amount'].astype(float)
orders = orders[orders['amount'] > 0]  # Remove invalid amounts
orders = orders.drop_duplicates(subset=['order_id'])

# Clean order items
order_items['quantity'] = order_items['quantity'].astype(int)
order_items['price'] = order_items['price'].astype(float)
order_items['line_total'] = order_items['quantity'] * order_items['price']

# Clean products
products['category'] = products['category'].fillna('Uncategorized')
products['price'] = products['price'].astype(float)

print("Step 2: Data cleaning completed")


# ============================================================================
# TEST 4.3: Complex Joins and Merges
# ============================================================================

# Join orders with customers
orders_with_customers = orders.merge(
    customers[['customer_id', 'name', 'email', 'segment']],
    on='customer_id',
    how='left'
)

# Join with order items
orders_detail = orders_with_customers.merge(
    order_items,
    on='order_id',
    how='left'
)

# Join with products
complete_orders = orders_detail.merge(
    products[['product_id', 'category', 'price']],
    on='product_id',
    how='left',
    suffixes=('_order', '_product')
)

print("Step 3: Complex joins completed")


# ============================================================================
# TEST 4.4: Feature Engineering
# ============================================================================

# Create new features
complete_orders['order_month'] = complete_orders['order_date'].dt.to_period('M')
complete_orders['order_quarter'] = complete_orders['order_date'].dt.to_period('Q')
complete_orders['order_year'] = complete_orders['order_date'].dt.year

# Customer level features
complete_orders['is_premium'] = complete_orders['segment'].isin(['Premium', 'VIP'])
complete_orders['order_value_category'] = pd.cut(
    complete_orders['amount'],
    bins=[0, 100, 500, 1000, float('inf')],
    labels=['Small', 'Medium', 'Large', 'XLarge']
)

# Product level features
complete_orders['is_high_margin'] = complete_orders['category'].isin(['Electronics', 'Premium'])

print("Step 4: Feature engineering completed")


# ============================================================================
# TEST 4.5: Aggregations and Summaries
# ============================================================================

# Customer-level summary
customer_summary = complete_orders.groupby('customer_id').agg({
    'order_id': 'count',
    'amount': ['sum', 'mean', 'max'],
    'line_total': 'sum',
    'is_premium': 'max',
    'name': 'first',
    'email': 'first'
}).round(2)

customer_summary.columns = ['order_count', 'total_spend', 'avg_order_value', 'max_order_value',
                           'total_items', 'is_premium', 'name', 'email']

# Category-level summary
category_summary = complete_orders.groupby('category').agg({
    'order_id': 'count',
    'amount': ['sum', 'mean'],
    'quantity': 'sum',
    'line_total': 'sum'
}).round(2)

category_summary.columns = ['order_count', 'total_revenue', 'avg_order', 'total_units', 'total_sales']

# Temporal summary
temporal_summary = complete_orders.groupby('order_month').agg({
    'order_id': 'count',
    'amount': ['sum', 'mean'],
    'customer_id': 'nunique'
}).round(2)

temporal_summary.columns = ['order_count', 'revenue', 'avg_order', 'unique_customers']

print("Step 5: Aggregations completed")


# ============================================================================
# TEST 4.6: Advanced Transformations
# ============================================================================

# Running totals by customer
complete_orders = complete_orders.sort_values(['customer_id', 'order_date'])
complete_orders['customer_cumulative_spend'] = complete_orders.groupby('customer_id')['amount'].cumsum()

# Rank orders by value within each month
complete_orders['rank_in_month'] = complete_orders.groupby('order_month')['amount'].rank(ascending=False)

# Percentile calculations
complete_orders['spend_percentile'] = complete_orders['amount'].rank(pct=True) * 100

# Rolling averages
complete_orders = complete_orders.sort_values('order_date')
complete_orders['moving_avg_amount'] = complete_orders['amount'].rolling(window=7, min_periods=1).mean()

print("Step 6: Advanced transformations completed")


# ============================================================================
# TEST 4.7: Data Quality Checks
# ============================================================================

# Quality metrics
null_check = complete_orders.isnull().sum()
duplicate_check = complete_orders.duplicated(subset=['order_id', 'product_id']).sum()

# Remove quality issues
quality_filtered = complete_orders.dropna(subset=['customer_id', 'order_id', 'amount'])
quality_filtered = quality_filtered.drop_duplicates(subset=['order_id', 'product_id'])

print(f"Step 7: Data quality checks - Nulls: {null_check.sum()}, Duplicates: {duplicate_check}")


# ============================================================================
# TEST 4.8: Pivot and Reshape Operations
# ============================================================================

# Pivot orders by category
category_pivot = complete_orders.pivot_table(
    values='amount',
    index='customer_id',
    columns='category',
    aggfunc='sum',
    fill_value=0
)

# Crosstab for category and segment
segment_category_crosstab = pd.crosstab(
    complete_orders['segment'],
    complete_orders['category'],
    values=complete_orders['amount'],
    aggfunc='sum'
)

# Pivot for temporal analysis
monthly_by_segment = complete_orders.pivot_table(
    values='amount',
    index='order_month',
    columns='segment',
    aggfunc='sum'
)

print("Step 8: Pivot operations completed")


# ============================================================================
# TEST 4.9: Output to Multiple Formats
# ============================================================================

# Save cleaned data
quality_filtered.to_csv('output/pipeline_complete_orders.csv', index=False)
quality_filtered.to_parquet('output/pipeline_complete_orders.parquet')

# Save summaries
customer_summary.to_csv('output/pipeline_customer_summary.csv')
category_summary.to_csv('output/pipeline_category_summary.csv')
temporal_summary.to_csv('output/pipeline_temporal_summary.csv')

# Save pivoted data
category_pivot.to_parquet('output/pipeline_category_pivot.parquet')
segment_category_crosstab.to_csv('output/pipeline_segment_category.csv')
monthly_by_segment.to_csv('output/pipeline_monthly_by_segment.csv')

# Save as Excel with multiple sheets
with pd.ExcelWriter('output/pipeline_summary.xlsx') as writer:
    customer_summary.to_excel(writer, sheet_name='Customers')
    category_summary.to_excel(writer, sheet_name='Categories')
    temporal_summary.to_excel(writer, sheet_name='Temporal')

print("Step 9: Output to multiple formats completed")


# ============================================================================
# TEST 4.10: Pipeline Summary and Metrics
# ============================================================================

# Generate summary statistics
summary_stats = {
    'total_orders': len(quality_filtered),
    'unique_customers': quality_filtered['customer_id'].nunique(),
    'total_revenue': quality_filtered['amount'].sum(),
    'avg_order_value': quality_filtered['amount'].mean(),
    'date_range': f"{quality_filtered['order_date'].min()} to {quality_filtered['order_date'].max()}",
    'categories': quality_filtered['category'].nunique(),
    'segments': quality_filtered['segment'].nunique()
}

print("\n=== Pipeline Summary ===")
for key, value in summary_stats.items():
    print(f"{key}: {value}")

print("\n=== Output Files Generated ===")
print("✓ pipeline_complete_orders.csv")
print("✓ pipeline_complete_orders.parquet")
print("✓ pipeline_customer_summary.csv")
print("✓ pipeline_category_summary.csv")
print("✓ pipeline_temporal_summary.csv")
print("✓ pipeline_category_pivot.parquet")
print("✓ pipeline_segment_category.csv")
print("✓ pipeline_monthly_by_segment.csv")
print("✓ pipeline_summary.xlsx")

print("\nComplex pipeline test completed successfully")
