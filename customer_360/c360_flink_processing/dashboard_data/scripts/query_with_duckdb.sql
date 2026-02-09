-- Example DuckDB queries over dashboard parquet files.
-- Run from dashboard_data/ or pass paths to the parquet directory.
-- Example: duckdb -c ".read scripts/query_with_duckdb.sql"
-- Or: duckdb -c "SELECT * FROM 'parquet/customer_analytics_c360.parquet' LIMIT 5"

-- Attach parquet directory (relative to current working directory).
-- If running from dashboard_data/: parquet/customer_analytics_c360.parquet
-- If running from scripts/: ../parquet/customer_analytics_c360.parquet

-- Create views for stable names in dashboards or BI tools.
-- Adjust the path prefix if your cwd differs (e.g. '../parquet/' when run from scripts/).
CREATE OR REPLACE VIEW customer_analytics_c360 AS
SELECT * FROM read_parquet('parquet/customer_analytics_c360.parquet');

CREATE OR REPLACE VIEW sdp_fulfillment_analytics AS
SELECT * FROM read_parquet('parquet/sdp_fulfillment_analytics.parquet');

-- Example: customer health summary
SELECT
    customer_status,
    loyalty_tier,
    COUNT(*) AS customers,
    ROUND(AVG(customer_health_score), 2) AS avg_health_score,
    ROUND(SUM(total_spent), 2) AS total_spent
FROM customer_analytics_c360
GROUP BY customer_status, loyalty_tier
ORDER BY customers DESC;

-- Example: fulfillment SLA by carrier
SELECT
    carrier,
    sla_status,
    COUNT(*) AS shipments,
    ROUND(AVG(delay_days), 1) AS avg_delay_days,
    ROUND(AVG(shipping_cost), 2) AS avg_shipping_cost
FROM sdp_fulfillment_analytics
GROUP BY carrier, sla_status
ORDER BY carrier, shipments DESC;
