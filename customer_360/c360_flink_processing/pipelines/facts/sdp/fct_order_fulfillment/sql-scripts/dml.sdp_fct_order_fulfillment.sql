-- Fact: order fulfillment snapshot. One row per shipment.
-- Data owner: Operations / Logistics.
INSERT INTO sdp_fct_order_fulfillment
SELECT
    d.shipment_id,
    d.transaction_id,
    d.customer_id,
    d.transaction_date,
    d.channel,
    d.channel_group,
    d.total_amount,
    d.tracking_number,
    d.carrier,
    d.service_level,
    d.origin_location,
    d.destination_address,
    d.ship_date,
    d.estimated_delivery,
    d.actual_delivery,
    d.delivery_status,
    d.shipping_cost,
    d.weight_kg,
    CASE
        WHEN d.delivery_status <> 'delivered' THEN CAST(NULL AS INT)
        WHEN d.actual_delivery IS NULL THEN CAST(NULL AS INT)
        WHEN d.actual_delivery <= d.estimated_delivery THEN 1
        ELSE 0
    END as sla_met,
    CASE
        WHEN d.delivery_status = 'delivered' AND d.actual_delivery IS NOT NULL AND d.estimated_delivery IS NOT NULL AND d.actual_delivery > d.estimated_delivery
        THEN TIMESTAMPDIFF(DAY, CAST(d.estimated_delivery AS TIMESTAMP(3)), CAST(d.actual_delivery AS TIMESTAMP(3)))
        ELSE CAST(0 AS BIGINT)
    END as delay_days,
    c.customer_segment,
    c.city,
    c.state,
    c.country,
    `$rowtime` as snapshot_at
FROM dim_sdp_order_fulfillment d
LEFT JOIN src_c360_customers c ON d.customer_id = c.customer_id;
