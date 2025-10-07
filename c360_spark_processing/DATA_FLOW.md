# Data Flow Architecture

## Pipeline Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    run_c360_pipeline.sh                      │
│                                                              │
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
│  📄 src_customers.sql                                            │
│     ../c360_mock_data/customer/customers.csv                     │
│     ↓                                                            │
│     customers_raw → src_customers (view)                         │
│                                                                  │
│  📄 src_loyalty_program.sql                                      │
│     ../c360_mock_data/customer/loyalty_program.csv               │
│     ↓                                                            │
│     loyalty_program_raw → src_loyalty_program (view)             │
│                                                                  │
│  📄 src_products.sql                                             │
│     ../c360_mock_data/products/products.csv                      │
│     ↓                                                            │
│     products_raw → src_products (view)                           │
│                                                                  │
│  📄 src_transactions.sql                                         │
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
│  📄 int_customer_transactions.sql                                │
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
│  📄 fct_customer_360_profile.sql                                 │
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
│  📄 customer_analytics_c360.sql                                  │
│     DEPENDS ON:                                                  │
│       - fct_customer_360_profile                                 │
│     ↓                                                            │
│     Applies:                                                     │
│       • Data quality filters                                     │
│       • Business logic (customer health score)                   │
│       • Risk indicators (churn risk, satisfaction risk)          │
│       • Opportunity flags (upsell, cross-sell)                   │
│     ↓                                                            │
│     customer_analytics_c360 (view) ✅ FINAL DATA PRODUCT         │
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

## Single Session vs Separate Sessions

### Single Session Mode (Default)

```
┌─────────────────────────────────────────────┐
│  One Spark Session                          │
│  ┌────────────────────────────────────┐    │
│  │ Execute sources/*.sql              │    │
│  │ ↓                                  │    │
│  │ Execute dimensions/*.sql           │    │
│  │ ↓                                  │    │
│  │ Execute intermediates/*.sql        │    │
│  │ ↓                                  │    │
│  │ Execute facts/*.sql                │    │
│  │ ↓                                  │    │
│  │ Execute views/*.sql                │    │
│  └────────────────────────────────────┘    │
│                                             │
│  ✅ Faster - shares execution context      │
│  ✅ More efficient memory usage            │
│  ✅ Views persist across layers            │
└─────────────────────────────────────────────┘
```

### Separate Sessions Mode

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

  ⚠️  Slower - multiple session startups
  ⚠️  More resource intensive
  ✅ Better isolation for debugging
  ✅ Easier to identify layer-specific issues
```

## Data Lineage

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
