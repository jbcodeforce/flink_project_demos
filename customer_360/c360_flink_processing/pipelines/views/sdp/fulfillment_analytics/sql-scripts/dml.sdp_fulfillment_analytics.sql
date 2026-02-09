-- View: Fulfillment Analytics sdp Data Product
-- Description: Consumable view for order fulfillment and delivery experience analytics
-- Purpose: Real-time fulfillment health, SLA, carrier performance for ops and CX
-- Data Owner: Operations / Logistics
-- Dependencies: sdp_fct_order_fulfillment
INSERT INTO sdp_fulfillment_analytics
SELECT
    shipment_id,
    transaction_id,
    customer_id,
    transaction_date,
    channel,
    channel_group,
    total_amount,
    tracking_number,
    carrier,
    service_level,
    origin_location,
    destination_address as destination_region,
    ship_date,
    estimated_delivery,
    actual_delivery,
    delivery_status,
    shipping_cost,
    weight_kg,
    sla_met,
    delay_days,
    CASE
        WHEN delivery_status <> 'delivered' THEN 'In Transit'
        WHEN sla_met = 1 THEN 'On Time'
        WHEN sla_met = 0 AND delay_days > 0 THEN 'Late'
        ELSE 'Unknown'
    END as sla_status,
    customer_segment,
    city,
    state,
    country,
    snapshot_at,
    `$rowtime` as view_created_at
FROM sdp_fct_order_fulfillment
WHERE shipment_id IS NOT NULL;
