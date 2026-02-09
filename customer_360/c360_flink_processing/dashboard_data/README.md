# Dashboard data (Parquet + DuckDB)

This folder holds parquet snapshots of the two analytics views for building a dashboard with DuckDB as the query engine. It is a uv project; use `uv sync` and `uv run` for commands.

## Layout

- **parquet/** – Parquet files: `customer_analytics_c360.parquet`, `sdp_fulfillment_analytics.parquet`
- **iceberg/** – Optional Iceberg table metadata (for future use with object storage or time-travel)
- **scripts/** – Example DuckDB queries (`query_with_duckdb.sql`)
- **src/dashboard_data/** – Python package; CLI entry point `export-views-to-parquet`

## Generate parquet (Option A: sample data)

From `dashboard_data/`:

```bash
cd customer_360/c360_flink_processing/dashboard_data
uv sync
uv run export-views-to-parquet
```

Output goes to `parquet/` by default. Options: `--customer-rows N`, `--fulfillment-rows N`, `-o /path/to/parquet`.

## Query with DuckDB

From `dashboard_data/` (so paths like `parquet/...` resolve):

```bash
duckdb -c "SELECT * FROM read_parquet('parquet/customer_analytics_c360.parquet') LIMIT 5"
duckdb -c "SELECT * FROM read_parquet('parquet/sdp_fulfillment_analytics.parquet') LIMIT 5"
```

Example views and aggregations are in `scripts/query_with_duckdb.sql`. Run with:

```bash
cd dashboard_data
duckdb < scripts/query_with_duckdb.sql
```

Or open a DuckDB shell and run the SQL there (paths in the script assume current directory is `dashboard_data`).

## Export from live Kafka (Option B)

When the Flink pipelines are deployed and the view topics have data, you can export from Kafka to the same parquet files:

1. Use a Kafka consumer (e.g. the project's `kafka_consumer` with Schema Registry) to read the topics for `customer_analytics_c360` and `sdp_fulfillment_analytics` (Avro).
2. Decode Avro, convert to a DataFrame or table, then write to `parquet/customer_analytics_c360.parquet` and `parquet/sdp_fulfillment_analytics.parquet` (e.g. via DuckDB `COPY ... TO ... (FORMAT PARQUET)` or pandas/pyarrow).

Topic names and Schema Registry settings come from your Confluent/Flink deployment. The view DDLs are under `pipelines/views/` (c360 and sdp).

## Optional: Iceberg

Parquet is sufficient for local dashboards. If you later use object storage (S3/Confluent) or want time-travel, you can add an Iceberg layer in `iceberg/` and point DuckDB at the Iceberg table (DuckDB Iceberg extension: `INSTALL iceberg; LOAD iceberg;`). See DuckDB docs for Iceberg and your catalog (e.g. REST catalog).
