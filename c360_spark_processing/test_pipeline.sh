#!/bin/bash

# Test script for C360 pipeline - validates CSV loading and basic queries
set -e

echo "🧪 Testing Customer Analytics C360 Pipeline..."
echo "=============================================="

# Check if Spark is available
if ! command -v spark-sql &> /dev/null; then
    echo "❌ Error: spark-sql command not found."
    exit 1
fi

# Set working directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "📂 Working directory: $SCRIPT_DIR"

# Run all tests in a single Spark session
echo ""
echo "🔧 Running comprehensive tests..."
spark-sql -e "
-- Test 1: Load customer data
CREATE OR REPLACE TEMPORARY VIEW customers_raw
USING CSV
OPTIONS (
  path '../c360_mock_data/customer/customers.csv',
  header 'true',
  inferSchema 'true'
);

-- Test 2: Load transaction data  
CREATE OR REPLACE TEMPORARY VIEW transactions_raw
USING CSV
OPTIONS (
  path '../c360_mock_data/sales/transactions.csv',
  header 'true',
  inferSchema 'true'
);

-- Test 3: Validate data counts
SELECT 'Customer Count:' as test, COUNT(*) as count FROM customers_raw
UNION ALL
SELECT 'Transaction Count:' as test, COUNT(*) as count FROM transactions_raw;

-- Test 4: Sample data
SELECT 'Sample Customers:' as section, customer_id, first_name, last_name FROM customers_raw LIMIT 3;

-- Test 5: Data relationships
SELECT 
    'Top Customers by Spending:' as section,
    c.customer_id,
    c.first_name,
    COUNT(t.transaction_id) as transaction_count,
    COALESCE(SUM(t.total_amount), 0) as total_spent
FROM customers_raw c
LEFT JOIN transactions_raw t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.first_name
ORDER BY total_spent DESC
LIMIT 5;
" --silent

echo "✅ All tests completed successfully"

echo ""
echo "🎉 Basic tests passed! Pipeline should work correctly."
echo "💡 Run ./run_c360_pipeline.sh to execute the full pipeline"
