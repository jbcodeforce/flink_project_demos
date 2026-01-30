INSERT INTO src_c360_shipments
SELECT
    shipment_id,
    transaction_id,
    tracking_number,
    carrier,
    service_level,
    origin_location,
    destination_address,
    weight_kg,
    dimensions,
    ship_date,
    estimated_delivery,
    actual_delivery,
    delivery_status,
    shipping_cost
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM shipments_raw
)
WHERE row_num = 1;
