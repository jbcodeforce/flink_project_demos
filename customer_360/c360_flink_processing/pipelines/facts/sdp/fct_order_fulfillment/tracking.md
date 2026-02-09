## Fact Table: c360_fct_order_fulfillment

Status date:

Context:

One row per shipment. Fulfillment snapshot with SLA and customer context for ops/BI.

## DDL and DML status

* DDL: ddl.c360_fct_order_fulfillment.sql
* DML: dml.c360_fct_order_fulfillment.sql

STATUS: MIGRATED

## Direct Dependencies found

dim_c360_order_fulfillment, src_c360_customers

## Tests

* Verify one row per shipment
* Verify sla_met and delay_days when delivery_status = delivered
