## Dimension Table: dim_sdp_estimated_delivery

Status date:

Context:

Computes estimated delivery timestamp and time window per shipment using the latest tracking event. Uses UDF `estimate_delivery(event_timestamp, current_location, destination_address)` with destination as ROW(street, city, zipcode, state); returns ROW(estimated_delivery_ts, window_start, window_end).

## DDL and DML status

* DDL: ddl.int_sdp_estimated_delivery.sql
* DML: dml.int_sdp_estimated_delivery.sql

STATUS: NEW

## Direct Dependencies

src_sdp_tracking_events, src_sdp_shipments

## UDF contract

Register a scalar function `estimate_delivery` with:

* Arguments: event_timestamp TIMESTAMP(3), current_location STRING, destination_address ROW&lt;street STRING, city STRING, zipcode STRING, state STRING&gt;
* Return: ROW&lt;estimated_delivery_ts TIMESTAMP(3), window_start TIMESTAMP(3), window_end TIMESTAMP(3)&gt;

## Tests

* Verify one row per shipment (latest event)
* Verify estimated_delivery_ts and time_window_start/end populated when UDF is registered
