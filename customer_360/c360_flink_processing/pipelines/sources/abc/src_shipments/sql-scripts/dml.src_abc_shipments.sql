INSERT INTO src_abc_shipments
SELECT
    shipment_id,
    transaction_id,
    tracking_number,
    carrier,
    service_level,
    origin_location,
    destination_address.street AS street,
    destination_address.city AS city,
    destination_address.zipcode AS zipcode,
    destination_address.state AS state,
    weight_kg,
    dimensions,
    target_ship_date,
    shipment_status,
    shipping_cost
FROM shipments_raw where shipment_status = 'delivered';