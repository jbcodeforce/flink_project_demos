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

* Create the fct_customer_360_profile
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

## Shifting left from batch to real-time

As illustrated by the Spark project, the analytical data prduct is accessible after each batch pipeline execution, and even a REST API needs to run the spark job, and then cache the result for a certain time.

The goal is to move to close to real-time processing.