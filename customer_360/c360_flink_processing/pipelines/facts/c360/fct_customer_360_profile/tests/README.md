# Unit tests explanations

The `c360_fct_customer_profile` uses 5 input tables as sources and generates record with the ['No primary key found in the statement.'] primary keys

## DML analysis


The joins are unbounded leading the Flink state growth.

These JOINs will accumulate unlimited state:
```sql

```


## Real data analysis

Running source data analysis, from the env-nknqp3 environment:

| Table Name | # messages in topic | Information of interest |
|------------|------------|--------------|
| src_c360_loyalty_program |  |  |
| dim_c360_customer_transactions |  |  |
| src_c360_app_usage |  |  |
| src_c360_support_ticket |  |  |
| src_c360_customers |  |  |


## Unit tests creation and execution:

DDL -> 

| UT |   Inserts | Validation |
| --- | --- | --- |
| sql | ✅ | ✅  |

### Issues to address



### src_c360_loyalty_program

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from src_c360_loyalty_program  group by id, tenant_id
```


### dim_c360_customer_transactions

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from dim_c360_customer_transactions  group by id, tenant_id
```


### src_c360_app_usage

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from src_c360_app_usage  group by id, tenant_id
```


### src_c360_support_ticket

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from src_c360_support_ticket  group by id, tenant_id
```


### src_c360_customers

* Example of record in topic:

```json
# add an example here as json object from the kafka topic
```

Analyze **data skew** with

```sql
select id, tenant_id, count(*) as record_count from src_c360_customers  group by id, tenant_id
```

