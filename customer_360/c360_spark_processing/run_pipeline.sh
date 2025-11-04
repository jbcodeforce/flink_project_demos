#!/bin/bash

# Get the absolute path to the project root
PROJECT_ROOT="/Users/jerome/Documents/Code/flink_project_demos"
DASHBOARD_DATA="$PROJECT_ROOT/c360_dashboard/data"

# Clean up any existing export data
rm -rf "$DASHBOARD_DATA/export"
rm -f "$DASHBOARD_DATA/customer_analytics_c360.csv"

# Run the Spark SQL pipeline
cd "$PROJECT_ROOT/c360_spark_processing"
spark-sql -f c360_consolidated_pipeline.sql

# Wait for the export to complete
sleep 2

# Find the exported part file and rename it
export_file=$(find "$DASHBOARD_DATA/export" -name "*.csv" -o -name "part-*")
if [ -n "$export_file" ]; then
    mv "$export_file" "$DASHBOARD_DATA/customer_analytics_c360.csv"
    rm -rf "$DASHBOARD_DATA/export"
fi