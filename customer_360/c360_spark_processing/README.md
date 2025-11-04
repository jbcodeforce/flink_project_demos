# Customer Analytics C360 Spark Processing

## Overview

This directory contains the Spark SQL processing pipeline for the **customer.analytics.C360** data product. The pipeline processes customer data from multiple domains (sales, loyalty, support, app usage) to create a comprehensive Customer 360 view.

This project uses `spark-sql` to run the Hive Metastore service in local mode and execute queries from the command line. Here is a quick summary of its capabilies:

### Basic commands

```sh
# Execute a sql within one session
spark-sql -f <filename.sql> 
# Execute one query as a string
spark-sql -e "SELECT * FROM orders;"
# Start interactive shell
spark-sql
```

```sql
SHOW DATABASES;

SHOW TABLES;
SHOW TABLES IN default;

DESCRIBE EXTENDED table_name;

SHOW CREATE TABLE table_name;
```

The current `metastore_db` exists but has **no persistent tables** because the pipeline uses:
```sql
CREATE OR REPLACE TEMPORARY VIEW src_customers AS ...
```

### Creating Persistent Tables

If you want tables that persist data in the metastore use one of the following options:

#### Option 1: Create Managed Tables
```sql
-- Load the pipeline views first
SOURCE sources/src_customers.sql;

-- Then persist to a table
CREATE TABLE customers_persistent AS
SELECT * FROM src_customers;

-- Verify it's in the metastore
SHOW TABLES;
```

#### Option 2: Create External Tables
```sql
CREATE EXTERNAL TABLE customers_external
USING PARQUET
LOCATION '/path/to/data'
AS SELECT * FROM src_customers;
```

#### Option 3: Save as Parquet
```sql
-- From spark-sql
CREATE TABLE customers_parquet
USING PARQUET
AS SELECT * FROM src_customers;
```

### Remove Metastore (reset everything)
```bash
# WARNING: This deletes all metadata!
rm -rf metastore_db/
rm -f derby.log

# Next spark-sql run will create fresh metastore
```

## Demonstration Architecture

The pipeline follows a layered data architecture with clear separation of concerns:

### Directory Structure
SQL files are organized into layers based on data processing stage:

```
sources/          → Load raw data
dimensions/       → Reference tables
intermediates/    → Transformations
facts/            → Aggregations
views/            → Final products
```

```
c360_spark_processing/
├── sources/          # Layer 1: Raw data ingestion from CSV files with deduplication logic
│   ├── src_customers.sql
│   ├── src_loyalty_program.sql
│   ├── src_products.sql
│   └── src_transactions.sql
├── dimensions/       # Layer 2: Dimensional data to prepare reusable dimensions
├── intermediates/    # Layer 3: Data transformation and enrichment
│   └── int_customer_transactions.sql
├── facts/           # Layer 4: Aggregated fact tables
│   └── fct_customer_360_profile.sql
├── views/           # Layer 5: Final consumable data products
│   └── customer_analytics_c360.sql
├── run_c360_pipeline.sh          # Main pipeline orchestration script
└── demo_queries.sql              # Business intelligence examples
```

### Auto-Discovery

The script automatically finds and executes all `.sql` files in each layer directory.

**Example:** Add a new source
```bash
# Just create the file - no script changes needed!
echo "CREATE OR REPLACE TEMPORARY VIEW src_new_data AS ..." > sources/src_new_data.sql

# Run the pipeline - your new file is automatically included
./run_c360_pipeline.sh
```

### Pipeline Execution Modes

The `run_c360_pipeline.sh` script automatically discovers and executes SQL files from subdirectories in the correct dependency order:

```
Sources → Dimensions → Intermediates → Facts → Views
```

It supports two execution modes:

1. **Single Session Mode** (Default, Recommended)
   - Executes all SQL layers in one Spark session
   - More efficient: Shares execution context and intermediate results
   - Better for production workloads
   ```bash
   ./run_c360_pipeline.sh
   ```

2. **Separate Sessions Mode**
   - Executes each layer in isolated Spark sessions
   - Better for debugging layer-specific issues
   - More resource-intensive
   ```bash
   ./run_c360_pipeline.sh --separate-sessions
   ```

[See the demonstration chapter](https://jbcodeforce.github.io/flink_project_demos/c360/spark_project/#demonstration)

### Conditional Execution

You can modify the script to skip certain layers:

```bash
# Edit run_c360_pipeline.sh to comment out layers:
LAYERS=(
    "Sources:sources"
    # "Dimensions:dimensions"     # Skip dimensions
    "Intermediates:intermediates"
    "Facts:facts"
    "Views:views"
)
```

### Validation
After pipeline execution, automatic validation queries run:
- 📊 Total Customers
- 💰 Total Revenue
- ⭐ Active Customers
- ⚠️ At Risk Customers
- 📝 Sample Records

### Best Practices

#### 1. Use Temporary Views
Always create `TEMPORARY VIEW` so views don't persist across sessions:
```sql
CREATE OR REPLACE TEMPORARY VIEW my_view AS ...
```

#### 2. Document Dependencies
Add comments to clarify what views your SQL depends on:
```sql
-- Dependencies: src_customers, src_transactions, src_products
```

#### 3. Handle NULL Values
Use `COALESCE` and `NULLIF` to handle missing data:
```sql
COALESCE(total_spent, 0.0) as total_spent,
SUM(quantity) / NULLIF(total_orders, 0) as avg_quantity
```

#### 4. Add Data Quality Checks
Include filters and quality flags:
```sql
WHERE customer_id IS NOT NULL 
  AND email IS NOT NULL
  AND email != ''
```

## Prerequisites

- Apache Spark 3.x installed with `spark-sql` command available
- Mock CSV data in `../c360_mock_data/` directory

## Quick Start

```bash
# Run the complete pipeline
./run_c360_pipeline.sh

# Or run with separate sessions for debugging
./run_c360_pipeline.sh --separate-sessions
```

This will:

1. Load all source data from CSV files (`sources/`)
2. Create intermediate transformation layers (`intermediates/`)
3. Build aggregated fact tables (`facts/`)
4. Generate the final Customer 360 data product (`views/`)
5. Run validation queries to confirm success


### Testing Individual Layers

Under c360_spark_processing folder:

```bash
# Test just the sources layer
spark-sql -f sources/src_customers.sql

# Test sources + intermediates
cat sources/*.sql intermediates/*.sql | spark-sql
```

### Business Intelligence Queries

Run demo business analysis queries:

#### In the same session as pipeline:

```sql
-- After running c360_consolidated_pipeline.sql, run:
SELECT * FROM customer_analytics_c360 LIMIT 10;
```

#### In a new session:
```bash
# First load the pipeline
spark-sql -f c360_consolidated_pipeline.sql

# Then run analysis queries  
spark-sql -f demo_queries.sql
```

## Integration Examples

### Connect to BI Tools
```sql
-- Query the data product directly
SELECT customer_id, customer_segment, loyalty_tier, 
       customer_health_score, total_spent
FROM customer_analytics_c360
WHERE customer_status = 'Active';
```

### ML Feature Extraction
```sql  
-- Extract features for churn prediction model
SELECT customer_id, recency_score, frequency_score, monetary_score,
       total_app_sessions, avg_satisfaction, churn_risk_flag
FROM customer_analytics_c360;
```

### Marketing Campaigns
```sql
-- Target high-value at-risk customers
SELECT customer_id, email, first_name, last_name
FROM customer_analytics_c360  
WHERE churn_risk_flag = 1 AND lifetime_value > 1000;
```
