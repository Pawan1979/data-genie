"""
Test 2: Advanced DataFrame Operations
Tests: groupby, merge, join, apply, aggregate functions
Also tests: lambda functions (for UDF detection)
"""

import pandas as pd
import numpy as np

# ============================================================================
# TEST 2.1: Load Sample Data
# ============================================================================

# Read base data
customers = pd.read_csv('data/customers.csv')
orders = pd.read_csv('data/orders.csv')
products = pd.read_csv('data/products.csv')
sales = pd.read_csv('data/sales.csv')

print("Data loaded successfully")


# ============================================================================
# TEST 2.2: GroupBy Operations
# ============================================================================

# Group by single column
orders_by_customer = orders.groupby('customer_id').size()

# Group by multiple columns
sales_by_region_product = sales.groupby(['region', 'product_id']).sum()

# Group by with aggregation
customer_stats = orders.groupby('customer_id').agg({
    'order_id': 'count',
    'total_amount': ['sum', 'mean', 'max']
})

# Group by with custom names
monthly_sales = sales.groupby(sales['date'].dt.to_period('M')).agg({
    'amount': 'sum',
    'quantity': 'sum'
})

print("GroupBy operations completed")


# ============================================================================
# TEST 2.3: Merge/Join Operations
# ============================================================================

# Inner join
orders_with_customers = orders.merge(customers, on='customer_id', how='inner')

# Left join
all_orders_data = orders.merge(customers, on='customer_id', how='left')

# Merge multiple DataFrames
complete_data = orders.merge(customers, on='customer_id') \
                     .merge(products, on='product_id') \
                     .merge(sales, on='order_id')

# Merge on different column names
merged_data = orders.merge(customers, left_on='cust_id', right_on='customer_id')

# Outer join
all_records = orders.merge(customers, on='customer_id', how='outer')

print("Merge operations completed")


# ============================================================================
# TEST 2.4: Apply and Transform Operations
# ============================================================================

# Apply custom function to column
def calculate_discount(amount):
    if amount > 100:
        return amount * 0.1
    else:
        return amount * 0.05

sales['discount'] = sales['amount'].apply(calculate_discount)

# Apply lambda function
sales['discounted_amount'] = sales['amount'].apply(lambda x: x * 0.9 if x > 100 else x * 0.95)

# Apply to entire row
orders['summary'] = orders.apply(lambda row: f"{row['order_id']}: {row['customer_id']}", axis=1)

# Transform operation (returns same shape)
orders['normalized_amount'] = orders['total_amount'].transform(lambda x: (x - x.mean()) / x.std())

# Apply with multiple conditions
products['category_group'] = products['category'].apply(
    lambda x: 'Electronics' if x in ['laptop', 'phone'] else 'Accessories'
)

print("Apply/Transform operations completed")


# ============================================================================
# TEST 2.5: Aggregate Operations
# ============================================================================

# Simple aggregation
total_sales = sales['amount'].sum()
avg_order = orders['total_amount'].mean()
max_amount = sales['amount'].max()
min_amount = sales['amount'].min()

# Named aggregation
summary_stats = orders.agg({
    'order_id': 'count',
    'total_amount': ['sum', 'mean', 'std', 'min', 'max']
}).round(2)

# Multiple aggregations
daily_stats = sales.groupby(sales['date'].dt.date).agg({
    'amount': ['sum', 'mean', 'count'],
    'quantity': ['sum', 'mean']
})

# Using agg with custom functions
custom_agg = customers.agg({
    'customer_id': 'count',
    'segment': lambda x: x.nunique()
})

print(f"Total sales: {total_sales}")
print(f"Average order: {avg_order}")


# ============================================================================
# TEST 2.6: String Operations
# ============================================================================

# String methods
customers['name_upper'] = customers['name'].str.upper()
customers['name_lower'] = customers['name'].str.lower()
customers['name_length'] = customers['name'].str.len()

# String contains
electronics = products[products['name'].str.contains('electronic', case=False)]

# String replace
products['name_clean'] = products['name'].str.replace('_', ' ')

# String split
customers[['first_name', 'last_name']] = customers['name'].str.split(' ', expand=True)

print("String operations completed")


# ============================================================================
# TEST 2.7: Data Cleaning Operations
# ============================================================================

# Fill null values
orders['notes'].fillna('No notes', inplace=True)

# Drop duplicates
unique_orders = orders.drop_duplicates(subset=['order_id'])

# Remove rows with null values
clean_data = sales.dropna()

# Replace values
orders['status'] = orders['status'].replace({'pending': 'Pending', 'completed': 'Completed'})

# Convert data types
orders['order_date'] = pd.to_datetime(orders['order_date'])

# Reset index
reindexed = orders.reset_index(drop=True)

print("Data cleaning completed")


# ============================================================================
# TEST 2.8: Sorting and Filtering
# ============================================================================

# Sort by single column
sorted_orders = orders.sort_values('total_amount', ascending=False)

# Sort by multiple columns
sorted_data = sales.sort_values(['date', 'amount'], ascending=[True, False])

# Filter with conditions
high_value_orders = orders[orders['total_amount'] > 500]

# Multiple conditions
premium_sales = sales[(sales['amount'] > 100) & (sales['quantity'] > 5)]

# Filter with isin
specific_regions = sales[sales['region'].isin(['North', 'South', 'East'])]

print("Sorting and filtering completed")


# ============================================================================
# TEST 2.9: Pivot Operations
# ============================================================================

# Pivot table
sales_pivot = sales.pivot_table(values='amount', index='region', columns='product_id', aggfunc='sum')

# Pivot with multiple aggregations
pivot_agg = orders.pivot_table(values='total_amount', index='customer_id', columns='status', aggfunc=['sum', 'count'])

# Crosstab
crosstab_data = pd.crosstab(sales['region'], sales['product_id'], values=sales['amount'], aggfunc='sum')

print("Pivot operations completed")


# ============================================================================
# TEST 2.10: Save Processed Data
# ============================================================================

# Save results
orders_with_customers.to_csv('output/orders_with_customers.csv', index=False)
complete_data.to_parquet('output/complete_data.parquet')
customer_stats.to_csv('output/customer_stats.csv')
sales_pivot.to_csv('output/sales_pivot.csv')

print("Results saved successfully")
