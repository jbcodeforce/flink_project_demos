# Customer 360 Spark Implementation

The Spark implementation for the Customer 360 analytics uses the Kimball methodology of Sources → Intermediates → Facts → Views.

The demonstration supports a **Customer 360 (C360) data product** for a multi-channel retailer, following the data product architecture outlined in the [design document](./data_models.md). The project implements a complete data pipeline from mock data generation to final consumable analytics views.

The spark-sql creates a Hive metastore to contain the persistent view definitions and the database schema metadata.

As an analytical pipeline, the sources are processed at each spark session, on-demand, and there is no persistence of the computed results.

## Key Features

The implemented pipeline creates a comprehensive customer view for analytics, BI, and ML initiatives. The name of this data product is `customer_analytics_c360`.

### Data Pipeline Flow


```
┌─────────────────────────────────────────────────────────────┐
│                    run_c360_pipeline.sh                     │
│                                                             │
│  [--separate-sessions flag determines execution mode]       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
         ┌────────────────────────────────────┐
         │  Execution Mode Decision           │
         └────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
    ┌─────────────────┐             ┌─────────────────┐
    │ Single Session  │             │ Separate        │
    │ (Default)       │             │ Sessions        │
    └─────────────────┘             └─────────────────┘
              │                               │
              │                               │
    ┌─────────▼─────────┐         ┌──────────▼──────────┐
    │ Combine all SQL   │         │ Run each layer in   │
    │ into temp file    │         │ separate spark-sql  │
    │ → Single spark-sql│         │ sessions            │
    └─────────┬─────────┘         └──────────┬──────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
                    Execute Layers in Order
```

## Layer-by-Layer Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 1: SOURCES                                                │
│  Load raw data from CSV files                                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│    src_customers.sql                                             │
│     ../c360_mock_data/customer/customers.csv                     │
│     ↓                                                            │
│     customers_raw → src_customers (view)                         │
│                                                                  │
│    src_loyalty_program.sql                                       │
│     ../c360_mock_data/customer/loyalty_program.csv               │
│     ↓                                                            │
│     loyalty_program_raw → src_loyalty_program (view)             │
│                                                                  │
│    src_products.sql                                              │
│     ../c360_mock_data/products/products.csv                      │
│     ↓                                                            │
│     products_raw → src_products (view)                           │
│                                                                  │
│    src_transactions.sql                                          │
│     ../c360_mock_data/sales/transactions.csv                     │
│     ↓                                                            │
│     transactions_raw → src_transactions (view)                   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 2: DIMENSIONS                                             │
│  Reference/lookup tables (optional, currently empty)             │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  (No files yet - placeholder for future dimension tables)        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 3: INTERMEDIATES                                          │
│  Join and enrich data across sources                             │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   int_customer_transactions.sql                                  │
│     DEPENDS ON:                                                  │
│       - src_customers                                            │
│       - src_transactions                                         │
│       - src_products                                             │
│       - transaction_items_raw (loaded inline)                    │
│     ↓                                                            │
│     Combines customer + transaction + product data               │
│     ↓                                                            │
│     int_customer_transactions (view)                             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 4: FACTS                                                  │
│  Aggregate metrics and KPIs                                      │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   fct_customer_360_profile.sql                                   │
│     DEPENDS ON:                                                  │
│       - src_customers                                            │
│       - src_loyalty_program                                      │
│       - int_customer_transactions                                │
│       - support_tickets_raw (loaded inline)                      │
│       - app_usage_raw (loaded inline)                            │
│     ↓                                                            │
│     Aggregates all customer metrics:                             │
│       • Transaction summaries                                    │
│       • Support ticket metrics                                   │
│       • App usage statistics                                     │
│       • RFM scores (Recency, Frequency, Monetary)                │
│     ↓                                                            │
│     fct_customer_360_profile (view)                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 5: VIEWS                                                  │
│  Final consumable data products                                  │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   customer_analytics_c360.sql                                    │
│     DEPENDS ON:                                                  │
│       - fct_customer_360_profile                                 │
│     ↓                                                            │
│     Applies:                                                     │
│       • Data quality filters                                     │
│       • Business logic (customer health score)                   │
│       • Risk indicators (churn risk, satisfaction risk)          │
│       • Opportunity flags (upsell, cross-sell)                   │
│     ↓                                                            │
│     customer_analytics_c360 (view) FINAL DATA PRODUCT            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────┐
              │   Validation Queries      │
              │   • Total customers       │
              │   • Total revenue         │
              │   • Active customers      │
              │   • At-risk customers     │
              │   • Sample records        │
              └───────────────────────────┘
```

### Single Session vs Separate Sessions

#### Single Session Mode (Default)

```
┌─────────────────────────────────────────────┐
│  One Spark Session                          │
│  ┌────────────────────────────────────┐     │
│  │ Execute sources/*.sql              │     │
│  │ ↓                                  │     │
│  │ Execute dimensions/*.sql           │     │
│  │ ↓                                  │     │
│  │ Execute intermediates/*.sql        │     │
│  │ ↓                                  │     │
│  │ Execute facts/*.sql                │     │
│  │ ↓                                  │     │
│  │ Execute views/*.sql                │     │
│  └────────────────────────────────────┘     │
│                                             │
└─────────────────────────────────────────────┘
```

#### Separate Sessions Mode

```
┌─────────────────────────────────────────────┐
│  Session 1                                  │
│  └─ Execute sources/*.sql                   │
│     └─ Exit session                         │
└─────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Session 2                                  │
│  └─ Execute dimensions/*.sql                │
│     └─ Exit session                         │
└─────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Session 3                                  │
│  └─ Execute intermediates/*.sql             │
│     └─ Exit session                         │
└─────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Session 4                                  │
│  └─ Execute facts/*.sql                     │
│     └─ Exit session                         │
└─────────────────────────────────────────────┘
              ↓
┌─────────────────────────────────────────────┐
│  Session 5                                  │
│  └─ Execute views/*.sql                     │
│     └─ Exit session                         │
└─────────────────────────────────────────────┘
```

## Key Concepts

### View Persistence
- **Temporary Views**: Created with `CREATE OR REPLACE TEMPORARY VIEW`
- Only exist within the Spark session
- Perfect for pipeline transformations
- Don't pollute the metastore

### Execution Order
- Layers execute in dependency order: Sources → Dimensions → Intermediates → Facts → Views
- Within each layer, files execute alphabetically
- Use numeric prefixes to control order: `01_`, `02_`, etc.

### Error Handling
- Script exits on first error (`set -e`)
- In single session mode: Debug file preserved on error
- In separate sessions mode: Each layer isolated for easier debugging

### Auto-Discovery
- Script uses `find` to discover all `.sql` files
- No hard-coded file names
- Add/remove files freely - no script changes needed

---

**Ready to explore?** Run `./run_c360_pipeline.sh` and watch the data flow through the layers!



### Customer Health Scoring 

The following KPIs may be defined.

#### RFM Analysis & Health Scoring
- **Recency Score** (1-5): Based on last purchase date
- **Frequency Score** (1-5): Based on transaction count  
- **Monetary Score** (1-5): Based on total spending
- **Customer Health Score**: Composite metric for overall value

#### Customer Status Classification
- **Active**: Recent purchases (≤30 days)
- **At Risk**: Moderate inactivity (31-90 days)
- **Churned**: Extended inactivity (>90 days)
- **Never Purchased**: No transaction history

### Cross-Domain Integration

- **Demographics**: Age, generation, location, tenure
- **Loyalty Program**: Tier, points, lifetime value
- **Purchase Behavior**: Channels, frequency, preferences
- **Support Interaction**: Tickets, satisfaction, issues
- **Digital Engagement**: App usage, device types, engagement score

### Risk & Opportunity Identification
- **Churn Risk Flags**: Based on activity and satisfaction
- **Channel Expansion**: Single-channel customers with growth potential
- **Tier Upgrade**: High-value customers in lower tiers
- **App Adoption**: Active customers not using digital channels

## Components

- **4 Source files**: Raw data ingestion with enrichment
- **1 Intermediate file**: Customer-transaction integration layer
- **1 Fact table**: Complete customer profile with metrics
- **1 Final view**: Consumable data product

## Key SQL Files

### Sources
- `src_customers.sql` - Customer demographics with derived fields read from the raw_customers.
- `src_loyalty_program.sql` - Loyalty data with tier analysis read from loyalty_program_raw
- `src_transactions.sql` - Sales data with time-based enrichment read from transactions_raw
- `src_products.sql` - Product catalog with category groupings read from products_raw

### Intermediates  
- `int_customer_transactions.sql` - Joined customer-transaction data

### Facts
- `fct_customer_360_profile.sql` - Complete customer profile with metrics

### Views
- `customer_analytics_c360.sql` - Final data product for consumption


### Data Lineage

```
Raw CSV Files
    │
    ├── customers.csv ────────────────┐
    ├── loyalty_program.csv ──────────┤
    ├── products.csv ─────────────────┤
    ├── transactions.csv ─────────────┤
    ├── transaction_items.csv ────────┤
    ├── support_tickets.csv ──────────┤
    └── app_usage.csv ────────────────┤
                                      │
                                      ▼
                            Source Views (Layer 1)
    ┌───────────────────────────────────────────┐
    │ src_customers                             │
    │ src_loyalty_program                       │
    │ src_products                              │
    │ src_transactions                          │
    └───────────────────────────────────────────┘
                    │
                    ▼
            Intermediate Views (Layer 3)
    ┌───────────────────────────────────────────┐
    │ int_customer_transactions                 │
    │   (combines customers + transactions +    │
    │    products + transaction items)          │
    └───────────────────────────────────────────┘
                    │
                    ▼
                Fact Tables (Layer 4)
    ┌───────────────────────────────────────────┐
    │ fct_customer_360_profile                  │
    │   (aggregates all metrics:                │
    │    transactions, support, app usage,      │
    │    loyalty, RFM scores)                   │
    └───────────────────────────────────────────┘
                    │
                    ▼
            Final Data Product (Layer 5)
    ┌───────────────────────────────────────────┐
    │ customer_analytics_c360                   │
    │   (filtered, enriched, with business      │
    │    rules and data quality checks)         │
    └───────────────────────────────────────────┘
                    │
                    ▼
            Business Intelligence
    ┌───────────────────────────────────────────┐
    │ • Dashboards (Tableau, Power BI)          │
    │ • Reports (Customer health, churn)        │
    │ • ML Models (Churn prediction)            │
    │ • Operational queries (CRM, marketing)    │
    └───────────────────────────────────────────┘
```


## Demonstration

### Prerequisites

* Apache Spark 3.x installed with `spark-sql` command available: `brew install apache-spark`
* Mock CSV data in `../c360_mock_data/` directory

### Step by step execution

The following commands execute the pipeline step by step so we can understand the processing logic.

```bash
# 0. start the shell in the c360_spark_processing
spark-sql
# 1. Load source views
source sources/src_customers.sql;
select * from src_customers;
source sources/src_loyalty_program.sql;
select * from src_loyalty_program;
source sources/src_transactions.sql;
select * from src_transactions;
source sources/src_products.sql;
select * from src_products;

# 2. Verify tables
show tables;
customers_raw
loyalty_program_raw
products_raw
src_customers
src_loyalty_program
src_products
src_transactions
transactions_raw

# 3. Create intermediate views
source intermediates/int_customer_transactions.sql;
select * from int_customer_transactions;

# 4. Build fact tables
source facts/fct_customer_360_profile.sql;
select * from fct_customer_360_profile;

# 5. Create final data product view
source views/customer_analytics_c360.sql;

# 6. Query the data product
SELECT * FROM customer_analytics_c360 LIMIT 10;
```

The completion of this pipeline creates a `metastore_db` folder which is a Apache Spark's embedded Hive metastore database.

The Hive Metastore stores metadata about databases, tables, views, and schemas. There is also a Derby Database used as the embedded database engine.

### Expected Results

After successful execution, you'll see:

- **15 customers** processed from mock data
- **Pipeline Summary** showing customer distribution
- **Top 5 customers** by health score
- All customers currently show "Churned" status (expected - mock data is from May/June 2024)

Sample output:

```
Pipeline Summary: 15 total customers, 0 active, 15 at risk, 1.87 avg health score
Top Customer: Lisa Anderson (CUST009) - Platinum tier, $1019.98 spent, 2.67 health score
```

### Quick Execution

#### Run Complete Pipeline (Recommended)
```bash
cd c360_spark_processing
./run_c360_pipeline.sh
```

#### Run Consolidated SQL Directly
```bash
cd c360_spark_processing  
spark-sql -f c360_consolidated_pipeline.sql
```

## Usage Examples

From this customer analytics data product it is possible to extract some specific analytic metrics:

### Sample Customer 360 Profile

```sql
SELECT 
    customer_id,
    first_name || ' ' || last_name as customer_name,
    customer_segment,          -- Premium, Standard, Basic
    loyalty_tier,             -- Platinum, Gold, Silver, Bronze
    customer_status,          -- Active, At Risk, Churned
    total_transactions,       -- Purchase frequency
    total_spent,             -- Total lifetime spending
    customer_health_score,   -- Composite RFM score (1-5)
    churn_risk_flag,         -- Risk indicator
    app_engagement_score     -- Digital engagement metric
FROM customer_analytics_c360;
```

### Marketing Use Cases
```sql
-- Identify churn risk customers 
SELECT customer_id, first_name, last_name, customer_health_score
FROM customer_analytics_c360 
WHERE churn_risk_flag = 1 
  AND lifetime_value > 1000;

-- Segment customers by generation for targeted messaging
SELECT generation_segment, 
       COUNT(*) as customer_count,
       AVG(lifetime_value) as avg_ltv
FROM customer_analytics_c360 
GROUP BY generation_segment;
```

### Product Analysis
```sql
-- Analyze channel expansion opportunities
SELECT preferred_channel,
       COUNT(*) as customers,
       SUM(channel_expansion_opportunity) as expansion_opps
FROM customer_analytics_c360
GROUP BY preferred_channel;
```

### Finance Analysis  
```sql
-- Customer lifetime value by segment
SELECT value_segment,
       COUNT(*) as customers,
       AVG(lifetime_value) as avg_ltv,
       SUM(total_spent) as total_revenue
FROM customer_analytics_c360
GROUP BY value_segment;
```

## Data Quality

### Validation Rules
- Customer ID must be present and unique
- Email addresses must be non-null and non-empty
- Transaction amounts must be positive
- Dates must be valid and reasonable

### Data Completeness
- Customer data: 100% on key demographics
- Transaction data: 95%+ on purchase history
- Support data: Variable based on customer interaction
- App usage: Available for digital customers only

## Performance Considerations

### Optimization Strategies
- Partition by customer_id for efficient queries
- Index on frequently queried dimensions (customer_segment, loyalty_tier)
- Cache intermediate results for iterative analysis
- Use columnar storage (Parquet) for analytical workloads

### Scaling
- Designed to handle millions of customers
- Horizontal scaling via Spark cluster
- Incremental processing for large datasets
- Real-time streaming integration capability

## Integration Points

### Consuming Systems
- **BI Tools**: Tableau, Power BI, Looker
- **ML Platforms**: MLflow, PyTorsh, Notebook.

### API Access
```python
# Python example for programmatic access
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("C360Analysis").getOrCreate()
c360_df = spark.sql("SELECT * FROM customer_analytics_c360")
high_value_customers = c360_df.filter("customer_health_score >= 4.0")
```

