# Customer 360 Flink Implementation

The Flink implementation is using the same approach as the spark implementation. The sources of data are now becoming streams, and to support the Stream capability we use Apache Kafka.

This chapter proposes different approaches to implement the Flink SQL queries, one starting from a white page but using the [shift Left Data Engineer's recipes](https://jbcodeforce.github.io/shift_left_utils/recipes/#data-engineer-centric-use-cases). And one using AI to migrate from the Spark batch processing.

## Building the project

This section presents the commands executed to build the `c360_flink_processing` project, and all its content.

### Define the shift_left configuration

* Create a config.yaml from the template, and .gitignore it
* create a .env to export all the environment variables and .gitignore it
* source the .env
* Run: `shift_left project validate-config` -> should get "Config.yaml validated" message

### Create the target project

```sql
shift_left project init c360_flink_processing customer_360
```

SQL files are organized into layers based on data processing stage:

```sh
sources/          → Load raw data
dimensions/       → Reference tables
intermediates/    → Transformations
facts/            → Aggregations
views/            → Final products
```

### Add table foundations

* Create the **fct_customer_360_profile**
    ```sh
    shift_left table init fct_customer_360_profile $PIPELINES/facts --product-name c360
    ```

    It should build the following structure:
    ```sh
    └── c360
        └── fct_customer_360_profile
            ├── Makefile
            ├── sql-scripts
            │   ├── ddl.c360_fct_customer_360_profile.sql
            │   └── dml.c360_fct_customer_360_profile.sql
            ├── tests
            └── tracking.md
    ```

    The DDL is created by looking as the expected structure in the corresponding Spark fct_customer_360_profile.sql

The fact needs source tables to get the data from raw topics.

* Create  `src_loyalty_program` with the command:
    ```sql
    shift_left table init src_loyalty_program $PIPELINES/sources --product-name c360
    ```

* Other sources to create
    ```sh
    shift_left table init src_customers  $PIPELINES/sources --product-name c360
    shift_left table init src_app_usage  $PIPELINES/sources --product-name c360
    shift_left table init src_support_ticket  $PIPELINES/sources --product-name c360
    shift_left table init src_tx_items  $PIPELINES/sources --product-name c360
    ```
* Each dml may implement the deduplication pattern.
    ```sql
    insert into ..
    select 
    ...
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ticket_id 
                ORDER BY `$rowtime` DESC
            ) AS row_num
        FROM support_ticket_raw
    ) WHERE row_num = 1
    ```
* for each sources add the creation of the raw tables, and the insert statements to get some values into the raw kafka topics. Use the following prompt to the AI code assistant:
    ```txt
    create a flink sql to insert the same records from the @support_tickets.csv into a support_ticket_raw
    ```
### Add an intermediate table for the transaction

```sh
shift_left table init int_customer_transactions  $PIPELINES/intermediates --product-name c360
```

which leads to create a new src and raw
```sh
shift_left table init src_tx_items  $PIPELINES/sources --product-name c360
shift_left table init src_transactions  $PIPELINES/sources --product-name c360 
shift_left table init src_products  $PIPELINES/sources --product-name c360
```

One of the prompt is:
```
create a flink sql to insert the same records from the @transaction_items.csv into insert transaction_items_raw
```

## Shifting left from batch to real-time

As illustrated by the Spark project, the analytical data prduct is accessible after each batch pipeline execution, and even a REST API needs to run the spark job, and then cache the results for a certain time to deliver the data product via its interface.

As introduced by [the Spark to Flink SQL migration section](https://jbcodeforce.github.io/shift_left_utils/coding/llm_based_translation/#spark-sql-to-flink-sql) it is possible to migrate with a local LLM.

* Start Ollama server
    ```
    ollama serve
    ```

* Try a migration
    ```sh
    shift_left table migrate fct_customer_360_profile $SRC_FOLDER/facts/fct_customer_360_profile.sql $STAGING --source-type spark
    ```