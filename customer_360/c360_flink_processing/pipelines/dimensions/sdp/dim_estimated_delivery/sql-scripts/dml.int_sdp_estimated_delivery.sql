-- Dimension: estimated delivery and time window. One row per shipment (latest tracking event).
-- Assumes UDF: estimate_delivery(event_timestamp, current_location, destination_address)
-- with destination_address as ROW<street STRING, city STRING, zipcode STRING, state STRING>
-- returning ROW<estimated_delivery_ts TIMESTAMP(3), window_start TIMESTAMP(3), window_end TIMESTAMP(3)>.
INSERT INTO dim_sdp_estimated_delivery
SELECT
    with_est.shipment_id,
    with_est.event_id,
    with_est.event_timestamp,
    with_est.current_location,
    with_est.est.estimated_delivery_ts,
    with_est.est.window_start AS time_window_start,
    with_est.est.window_end AS time_window_end
FROM (
    SELECT
        latest.shipment_id,
        latest.event_id,
        latest.event_timestamp,
        latest.location AS current_location,
        estimate_delivery(
            latest.event_timestamp,
            latest.location,
            ROW(latest.street, latest.city, latest.zipcode, latest.state)
        ) AS est
    FROM (
        SELECT
            e.shipment_id,
            e.event_id,
            e.event_timestamp,
            e.location,
            s.street,
            s.city,
            s.zipcode,
            s.state,
            ROW_NUMBER() OVER (
                PARTITION BY e.shipment_id
                ORDER BY e.event_timestamp DESC
            ) AS row_num
        FROM src_sdp_tracking_events e
        INNER JOIN src_sdp_shipments s ON e.shipment_id = s.shipment_id
    ) latest
    WHERE latest.row_num = 1
) AS with_est;
