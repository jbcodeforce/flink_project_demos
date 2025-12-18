# Transaction Processing

This contribution is coming from Confluent's Mustafa Bhanpurawala contributions. It is organized as a project to build data product and being managed by shift_left tool. It was adapted to extend the domain model of the transaction processing.

## Domain Data Model

The transaction processing domain consists of three core source tables:

### Entities

#### customers
Customer master data with deduplication support (upsert mode).

| Column | Type | Description |
|--------|------|-------------|
| `account_number` | VARCHAR | Primary key - unique customer identifier |
| `customer_name` | VARCHAR | Full name of the customer |
| `email` | VARCHAR | Customer email address |
| `phone_number` | VARCHAR | Contact phone number |
| `date_of_birth` | TIMESTAMP(3) | Customer birth date |
| `city` | VARCHAR | Customer city location |
| `created_at` | TIMESTAMP_LTZ(3) | Record creation timestamp (watermark) |

#### transactions
Financial transaction records with deduplication support (upsert mode).

| Column | Type | Description |
|--------|------|-------------|
| `txn_id` | VARCHAR(36) | Primary key - unique transaction identifier |
| `account_number` | VARCHAR(255) | Foreign key to customer |
| `timestamp` | TIMESTAMP_LTZ(3) | Transaction timestamp (watermark) |
| `amount` | DECIMAL(10,2) | Transaction amount |
| `currency` | VARCHAR(5) | Currency code (e.g., USD) |
| `merchant` | VARCHAR(255) | Merchant name |
| `location` | VARCHAR(255) | Transaction location |
| `status` | VARCHAR(255) | Transaction status |
| `transaction_type` | VARCHAR(50) | Type of transaction |

#### discounts
Discount offerings by merchants (append-only mode).

| Column | Type | Description |
|--------|------|-------------|
| `city` | VARCHAR | City where discount applies |
| `merchant_name` | VARCHAR | Merchant offering the discount |
| `min_transaction_value` | DECIMAL(10,2) | Minimum transaction value to qualify |
| `discount_amount` | DECIMAL(10,2) | Discount amount |
| `timestamp` | TIMESTAMP_LTZ(3) | Discount record timestamp (watermark) |

### Relationships

```
src_tx_customers (1) ──────< src_tx_transactions (N)
       │                           │
       │ account_number            │ merchant, location
       │                           │
       └─────── city ──────────────┴──── src_tx_discounts
```

- **Customers → Transactions**: One customer can have many transactions (linked by `account_number`)
- **Discounts → Transactions**: Discounts apply to transactions based on matching `city` and `merchant_name`

## Set up to use shift_left for this project

Recall shift_left doc is [here.]()

1. Set a config.yaml for shift_left
1. Set environment variables
    ```
    export CCLOUD_ENV_ID=env-nknqp3
    export CCLOUD_ENV_NAME=j9r-env
    export CCLOUD_KAFKA_CLUSTER=j9r-kafka
    export CLOUD_REGION=us-west-2
    export CLOUD_PROVIDER=aws
    export CCLOUD_CONTEXT=....
    export CCLOUD_COMPUTE_POOL_ID=lfcp-xvrvmz


    export SL_KAFKA_API_KEY=...
    export SL_KAFKA_API_SECRET=...
    export SL_CONFLUENT_CLOUD_API_KEY=...
    export SL_CONFLUENT_CLOUD_API_SECRET=...
    export SL_FLINK_API_KEY=...
    export SL_FLINK_API_SECRET=...

    export SL_LLM_BASE_URL=http://localhost:11434/v1
    export SL_LLM_MODEL=qwen3-coder:30b
    export SL_LLM_API_KEY=ollama
    ```
1. Define the last env variables for the project See file([set_sl_env.sh](set_sl_env.sh))
1. Verify configuration:
    ```sh
    shift_left project validate-config
    ```

## Managing the project

* The project was created by using:
    ```sh
    shift_left project init tx_processing flink_project_demos
    ```

* Then tables are created using:
    ```sql
    shift_left table init src_customers $PIPELINE/sources  --product-name tx
    ```

* For each source table that uses Faker to generate data, the tests folder includes the needed SQL to create the faker.
* Tracking:

| Table | Purpose | Specials | Status |
| ----- | ------- | -------- | ------ |
| transactions_fakers | Generate synthetic data | Faker record generation in tests under src_tx_transactions | Deployed via make |
| src_tx_transactions | Specify watermark keep append mode | ddl in upsert and dml | ✅ DDL ✅ DML |
| customers_faker | Generate synthetic data | Faker record generation in tests under src_tx_customers | Gernerate records |
| src_tx_customers | Deduplicate customers | ddl in upsert and dml for dedup  | ✅ DDL ✅ DML |
| discounts_faker | Generate synthetic data | Faker record generation in tests under src_tx_customers | Gernerate records |
| src_tx_discounts | Deduplicate discounts | ddl in upsert and dml for dedup  | ✅ DDL ✅ DML |

## Preparing dimensions

### Flag transactions

The goal is to assess total withdraws higher than a threshold over a time period, or multiple withdraw in the same merchant within 2 hours.

* Build a dimension to flag the high withdraw account over the last 2 hours. As the window operation does not support update and retraction, we need to have the src_transction to be insert only.
    ```sh
    shift_left table init dim_flag_tx $PIPELINES/dimensions --product-name tx
    ```

* Prefer to separate the DDL and DML. The DDL has one FLAG and reason.