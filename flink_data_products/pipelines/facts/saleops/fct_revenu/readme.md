## Fact Table: saleops_fct_revenu

Status date:

Context:

-- Process file: 

## DDL and DML for fact table status

* DDL Features:
✅ Dimension Keys: time_key, product_key, customer_key (as requested)
✅ Degenerate Dimensions: order_id, line_item_id, sales_channel
✅ Additive Measures:
revenue_amount (net revenue)
gross_revenue_amount
discount_amount
quantity
unit_price
cost_amount
profit_amount
tax_amount
✅ Audit Fields: created_at, updated_at
✅ Proper Primary Key: Composite key with dimension keys + degenerate dimensions
✅ Optimized Distribution: Hash distributed across dimension keys into 6 buckets


* DML Features:
✅ Dimension Lookups: Joins with dim_time, dim_product, dim_customer to get surrogate keys
✅ Unknown Member Handling: Uses -1 as default for unknown dimension references
✅ Slowly Changing Dimensions: Supports SCD Type 2 with effective dates
✅ Calculated Measures:
Revenue = unit_price × quantity - discount
Profit = revenue - cost
✅ Data Quality Filters: Only completed transactions with valid quantities and prices
✅ Incremental Processing: Configurable time window (currently 30 days)

## Direct Dependencies found



## Tests

* Get source data from <> -> 
* Verify there is no duplicate in output table  ->