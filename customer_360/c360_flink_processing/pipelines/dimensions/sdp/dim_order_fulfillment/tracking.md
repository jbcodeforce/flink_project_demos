## Dimension Table: dim_c360_order_fulfillment

Status date:

Context:

Enriches shipment with transaction (order) data for fulfillment analytics.

## DDL and DML status

* DDL: ddl.int_c360_order_fulfillment.sql
* DML: dml.int_c360_order_fulfillment.sql

STATUS: MIGRATED

## Direct Dependencies found

src_c360_shipments, src_c360_transactions

## Tests

* Verify one row per shipment
* Verify transaction_id and customer_id populated when order exists
