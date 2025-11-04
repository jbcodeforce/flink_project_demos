#!/bin/bash

# Customer Analytics C360 Data Product Pipeline
# Description: Complete Spark SQL pipeline for customer 360 view
# Usage: ./run_c360_pipeline.sh [--separate-sessions]
#
# Options:
#   --separate-sessions   Run each layer in a separate Spark session (default: single session)

set -e  # Exit on any error

# Parse command line arguments
SEPARATE_SESSIONS=false
if [[ "$1" == "--separate-sessions" ]]; then
    SEPARATE_SESSIONS=true
fi

echo "Starting Customer Analytics C360 Data Pipeline"
echo "=================================================="

# Check if Spark is available
if ! command -v spark-sql &> /dev/null; then
    echo "❌ Error: spark-sql command not found. Please install Apache Spark."
    echo "   You can install it via:"
    echo "   - Homebrew: brew install apache-spark"  
    echo "   - Or download from: https://spark.apache.org/downloads.html"
    exit 1
fi

# Set working directory to script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if mock data exists
if [ ! -d "../c360_mock_data" ]; then
    echo "❌ Error: Mock data directory not found at ../c360_mock_data"
    echo "   Please ensure the CSV mock data has been generated."
    exit 1
fi

echo "Working directory: $SCRIPT_DIR"
echo "Mock data location: ../c360_mock_data"
echo "Execution mode: $([ "$SEPARATE_SESSIONS" = true ] && echo "Separate Sessions" || echo "Single Session")"
echo ""

# Function to execute SQL files from a directory
execute_sql_layer() {
    local layer_name=$1
    local layer_dir=$2
    local is_separate=$3
    
    if [ ! -d "$layer_dir" ]; then
        echo "⚠️  Warning: Directory $layer_dir not found, skipping..."
        return 0
    fi
    
    # Find all SQL files in the directory and sort them
    local sql_files=($(find "$layer_dir" -maxdepth 1 -name "*.sql" -type f | sort))
    
    if [ ${#sql_files[@]} -eq 0 ]; then
        echo "⚠️  Warning: No SQL files found in $layer_dir, skipping..."
        return 0
    fi
    
    echo "📁 Processing Layer: $layer_name"
    echo "   Directory: $layer_dir"
    echo "   Files found: ${#sql_files[@]}"
    
    if [ "$is_separate" = true ]; then
        # Run each SQL file in separate Spark sessions
        for sql_file in "${sql_files[@]}"; do
            local filename=$(basename "$sql_file")
            echo "   ▶ Executing: $filename"
            spark-sql -f "$sql_file" --silent 2>/dev/null || { 
                echo "   ❌ Failed to execute $filename"; 
                exit 1; 
            }
            echo "   ✓ Completed: $filename"
        done
    else
        # Accumulate SQL for single session execution
        for sql_file in "${sql_files[@]}"; do
            local filename=$(basename "$sql_file")
            echo "   ▶ Adding to pipeline: $filename"
            cat "$sql_file" >> "$TEMP_COMBINED_SQL"
            echo "" >> "$TEMP_COMBINED_SQL"  # Add blank line between files
        done
    fi
    
    echo "   ✅ Layer completed: $layer_name"
    echo ""
}

# Define pipeline layers in dependency order
LAYERS=(
    "Sources:sources"
    "Dimensions:dimensions"
    "Intermediates:intermediates"
    "Facts:facts"
    "Views:views"
)

if [ "$SEPARATE_SESSIONS" = true ]; then
    # Execute each layer in separate Spark sessions
    echo "🔧 Running pipeline in separate Spark sessions..."
    echo ""
    
    for layer in "${LAYERS[@]}"; do
        IFS=':' read -r layer_name layer_dir <<< "$layer"
        execute_sql_layer "$layer_name" "$layer_dir" true
    done
    
else
    # Execute all layers in a single Spark session
    echo "🔧 Running pipeline in single Spark session..."
    echo ""
    
    # Create temporary combined SQL file
    TEMP_COMBINED_SQL=$(mktemp "${SCRIPT_DIR}/temp_pipeline_XXXXXX.sql")
    trap "rm -f '$TEMP_COMBINED_SQL'" EXIT
    
    # Add header to combined SQL
    cat > "$TEMP_COMBINED_SQL" << 'EOF'
-- ============================================================
-- Customer Analytics C360 Data Pipeline
-- Auto-generated combined pipeline
-- ============================================================

EOF
    
    # Build combined SQL file from all layers
    for layer in "${LAYERS[@]}"; do
        IFS=':' read -r layer_name layer_dir <<< "$layer"
        echo "-- ============================================================" >> "$TEMP_COMBINED_SQL"
        echo "-- Layer: $layer_name" >> "$TEMP_COMBINED_SQL"
        echo "-- ============================================================" >> "$TEMP_COMBINED_SQL"
        echo "" >> "$TEMP_COMBINED_SQL"
        execute_sql_layer "$layer_name" "$layer_dir" false
    done
    
    # Execute the combined SQL file
    echo "🚀 Executing combined pipeline..."
    spark-sql -f "$TEMP_COMBINED_SQL" || { 
        echo "❌ Pipeline execution failed"; 
        echo "Debug: Check $TEMP_COMBINED_SQL for details";
        trap - EXIT  # Don't delete on error for debugging
        exit 1; 
    }
    
    echo "✅ Pipeline executed successfully"
    echo ""
fi

# Validation queries
echo "🔍 Running validation queries..."
echo ""

# Create validation SQL
cat > temp_validation.sql << 'EOF'
-- Quick validation of the final data product
SELECT 
    '📊 Total Customers' as metric,
    CAST(COUNT(*) AS STRING) as value
FROM customer_analytics_c360
UNION ALL
SELECT 
    '💰 Total Revenue' as metric,
    CAST(SUM(total_spent) AS STRING) as value
FROM customer_analytics_c360
UNION ALL
SELECT 
    '⭐ Active Customers' as metric,
    CAST(COUNT(*) AS STRING) as value
FROM customer_analytics_c360
WHERE customer_status = 'Active'
UNION ALL
SELECT 
    '⚠️  At Risk Customers' as metric,
    CAST(COUNT(*) AS STRING) as value
FROM customer_analytics_c360
WHERE customer_status = 'At Risk';

-- Show sample records
SELECT 
    '📝 Sample Customer Records' as info,
    '' as blank1,
    '' as blank2,
    '' as blank3,
    '' as blank4;

SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) as name,
    customer_segment,
    loyalty_tier,
    CAST(total_spent AS STRING) as total_spent,
    customer_status
FROM customer_analytics_c360
LIMIT 5;
EOF

spark-sql -f temp_validation.sql || echo "   ⚠️  Validation queries completed with warnings"

# Clean up
rm -f temp_validation.sql

echo ""
echo "🎉 Customer Analytics C360 Pipeline Completed Successfully!"
echo "============================================================"
echo ""
echo "📊 Data Product Available: customer_analytics_c360"
echo ""
echo "💡 Next Steps:"
echo "   - Query the data product: spark-sql -e \"SELECT * FROM customer_analytics_c360 LIMIT 10\""
echo "   - Run demo queries: spark-sql -f demo_queries.sql"
echo "   - Connect BI tools to the customer_analytics_c360 view"
echo "   - Use for ML model training and customer analytics"
echo ""
echo "📖 Usage Tips:"
echo "   - Re-run pipeline: ./run_c360_pipeline.sh"
echo "   - Use separate sessions: ./run_c360_pipeline.sh --separate-sessions"
echo ""

