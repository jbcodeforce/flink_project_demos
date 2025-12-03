#!/bin/bash

# KSQL to Flink SQL Migration Script
# Uses shift_left CLI to migrate KSQL files to Flink SQL

set -e

# Default staging directory - can be overridden by environment variable
STAGING="${STAGING:-./staging}"

# Function to migrate a single KSQL file to Flink SQL
migrate_ksql() {
    local table_name="$1"
    local ksql_file="$2"

    if [[ -z "$table_name" ]]; then
        echo "Error: Table name is required"
        usage
        return 1
    fi

    if [[ -z "$ksql_file" ]]; then
        echo "Error: KSQL file path is required"
        usage
        return 1
    fi

    if [[ ! -f "$ksql_file" ]]; then
        echo "Error: KSQL file not found: $ksql_file"
        return 1
    fi

    echo "Migrating KSQL to Flink SQL..."
    echo "  Table:   $table_name"
    echo "  Source:  $ksql_file"
    echo "  Staging: $STAGING"
    echo ""

    shift_left table migrate "$table_name" "$ksql_file" "$STAGING" --source-type ksql
}

usage() {
    cat << EOF
Usage: $0 <table_name> <ksql_file>

Migrate a KSQL file to Flink SQL using the shift_left CLI.

Arguments:
  table_name    Name of the target table
  ksql_file     Path to the KSQL source file

Environment Variables:
  STAGING       Output directory for migrated files (default: ./flink_ref)

Examples:
  $0 all_songs sources/routing/merge.ksql
  $0 pageviews_count sources/aggregations/count_pageviews.ksql
  STAGING=/tmp/output $0 filtered_clicks sources/routing/filtering.ksql

EOF
}

# Main entry point
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        usage
        exit 0
    fi

    if [[ $# -lt 2 ]]; then
        echo "Error: Missing required arguments"
        usage
        exit 1
    fi

    migrate_ksql "$1" "$2"
fi

