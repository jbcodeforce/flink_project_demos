# Flink Processing Demonstration

The data model and pipeline design match the Spark Processing. See [the data model section.]()

## Pipeline Tables

This section lists all DDL (Data Definition Language) and DML (Data Manipulation Language) files organized by data layer.

### Raw Tables

| Table Name | DDL File | Insert | Data |
|---------------|-------|--------|------|
| customers_raw | ok | ok |  15 records |
| product_raw   | ok | ok | 36 rows |
| tx_raw        | ok | ok | 20 rows |
| transaction_items_raw | ok | ok | 25 rows |
| app_usage_raw | ok | ok | 20 rows |
| support_ticket_raw | ok | ok | 18 rows |
| loyalty_program_raw | ok | ok | 15 rows |

### Source Layer Tables

| Table Name | DDL File | DML File | Deployment | Data Visibility |
|-----------|----------|----------|------------|-----------------|
| `src_c360_customers` | `ddl.src_c360_customers.sql` | `dml.src_c360_customers.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |
| `src_c360_products` | `ddl.src_c360_products.sql` | `dml.src_c360_products.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |
| `src_c360_transactions` | `ddl.src_c360_transactions.sql` | `dml.src_c360_transactions.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |
| `src_c360_tx_items` | `ddl.src_c360_tx_items.sql` | `dml.src_c360_tx_items.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |
| `src_c360_app_usage` | `ddl.src_c360_app_usage.sql` | `dml.src_c360_app_usage.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |
| `src_c360_support_ticket` | `ddl.src_c360_support_ticket.sql` | `dml.src_c360_support_ticket.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |
| `src_c360_loyalty_program` | `ddl.src_c360_loyalty_program.sql` | `dml.src_c360_loyalty_program.sql` | Deploy first - no dependencies | Immediate after Kafka topic has data |

### Intermediate Layer Tables

| Table Name | DDL File | DML File | Deployment | Data Visibility |
|-----------|----------|----------|------------|-----------------|
| `int_c360_customer_transactions` | `ddl.int_c360_customer_transactions.sql` | `dml.int_c360_customer_transactions.sql` | okay |  15 records |

### Fact Layer Tables

| Table Name | DDL File | DML File | Deployment | Data Visibility |
|-----------|----------|----------|------------|-----------------|
| `c360_fct_customer_profile` | `ddl.c360_fct_customer_profile.sql` | `dml.c360_fct_customer_profile.sql` | Deploy after intermediate and source tables | After all upstream dependencies have data |

### View Table

| Table Name | DDL File | DML File | Deployment | Data Visibility |
|-----------|----------|----------|------------|-----------------|
| `c360_fct_customer_profile` | `ddl.c360_fct_customer_profile.sql` | `dml.c360_fct_customer_profile.sql` | Deploy after intermediate and source tables | After all upstream dependencies have data |

## Deployment Order

1. **Source Layer**: Deploy all source tables in parallel (no dependencies on each other)
2. **Intermediate Layer**: Deploy after source tables are running
3. **Fact Layer**: Deploy after intermediate and source tables are running

## Data Visibility Notes

- **DDL**: Creates table schema, Kafka topics, and metadata
- **DML**: Creates Flink SQL streaming jobs that continuously process data
- **Data Visibility**: Time from job start until data appears in the table depends on:
  - Upstream data availability
  - Kafka topic backlog
  - Flink checkpoint intervals
  - Processing watermarks and windowing logic

