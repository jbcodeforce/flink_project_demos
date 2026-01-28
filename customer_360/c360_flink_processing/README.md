# Flink Processing Demonstration

This is the Flink implementation of the customer 360 analytics data product.

The data model and pipeline design match the Spark Processing. See [the data model section.](https://jbcodeforce.github.io/flink_project_demos/c360/data_models/)

## How to use it for demonstration

* First create the source tables using terraform to simulate CDC topics and prepare some test data
  ```sh
  cd IaC
  mv terraform.tfvars.example terraform.tfvars
  # modify the variable definitions inside terraform.tfvars
  terraform init
  terraform plan
  terraform deploy
  ```

* Use the shift_left utility to deploy the solution. Be sure to export the environment variables
  ```sh
  source .env
  shift_left project validate-config

  shift_left table build-inventory

  shift_left pipeline build-all-metadata
  ```

* Validate the execution plan:
  ```sh
  shift_left pipeline build-execution-plan --product-name c360
  ```

* Deploy
  ```sh
  shift_left pipeline deploy --product-name c360
  ```

* The expected results look like:
  ```sh
  --------------------------------------------------------------------------------------------------------
  Table_name                                              | Status     | Pending Records | Num Records Out
  --------------------------------------------------------------------------------------------------------
  src_c360_support_ticket                                 | RUNNING    |               0 |              18
  src_c360_loyalty_program                                | RUNNING    |               0 |              15
  src_c360_customers                                      | RUNNING    |               0 |              16
  src_c360_app_usage                                      | RUNNING    |               0 |              20
  src_c360_transactions                                   | RUNNING    |               0 |              20
  src_c360_tx_items                                       | RUNNING    |               0 |               0
  src_c360_products                                       | RUNNING    |               0 |               0
  int_c360_customer_transactions                          | RUNNING    |               0 |               0
  c360_fct_customer_profile                               | RUNNING    |               0 |              27
  customer_analytics_c360                                 | RUNNING    |               0 |               3
  --------------------------------------------------------------------------------------------------------
  ```

* In the Flink Workspace a `select * from c360_fct_customer_profile` should give the c360 data analytic records,
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

