-- Dimension: order fulfillment (shipment + order). One row per shipment.
-- Enriches shipment with transaction (customer_id, order amount, channel) for downstream fact.
INSERT INTO dim_c360_order_fulfillment
SELECT
    s.shipment_id,
    s.transaction_id,
    t.customer_id,
    t.transaction_date,
    t.channel,
    t.channel_group,
    t.total_amount,
    s.tracking_number,
    s.carrier,
    s.service_level,
    s.origin_location,
    s.destination_address,
    s.weight_kg,
    s.dimensions,
    s.ship_date,
    s.estimated_delivery,
    s.actual_delivery,
    s.delivery_status,
    s.shipping_cost
FROM src_c360_shipments s
LEFT JOIN src_c360_transactions t ON s.transaction_id = t.transaction_id;
