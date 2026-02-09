INSERT INTO src_sdp_tracking_events
SELECT
    event_id,
    shipment_id,
    event_timestamp,
    event_type,
    location,
    description,
    carrier_status
FROM tracking_events_raw
where (carrier_status <> 'cancelled' or carrier_status <> 'delivered')
 and location is not null;
