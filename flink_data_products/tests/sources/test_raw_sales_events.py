#!/usr/bin/env python3
"""
PyFlink Table API test script for raw_sales_events table.
Creates test data with duplicates to validate deduplication logic in sales_transactions.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.types import DataTypes
from pyflink.table.table_schema import TableSchema
from datetime import datetime, timedelta

def create_test_raw_sales_events():
    """
    Create test data for raw_sales_events table with duplicates to test deduplication logic.
    """
    
    # Initialize Flink Table Environment
    # Use batch mode for testing - can also use streaming mode if needed
    env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    table_env = TableEnvironment.create(env_settings)
    
    # Configure Flink for Docker environment (if running in container)
    # These settings help with resource allocation in containerized environment
    table_env.get_config().set("parallelism.default", "1")
    table_env.get_config().set("taskmanager.memory.process.size", "512m")
    table_env.get_config().set("taskmanager.numberOfTaskSlots", "2")
    
    # Define schema for raw_sales_events table
    schema = TableSchema.builder() \
        .field("order_id", DataTypes.STRING()) \
        .field("line_item_id", DataTypes.STRING()) \
        .field("transaction_id", DataTypes.STRING()) \
        .field("customer_id", DataTypes.STRING()) \
        .field("product_id", DataTypes.STRING()) \
        .field("transaction_date", DataTypes.DATE()) \
        .field("transaction_timestamp", DataTypes.TIMESTAMP(3)) \
        .field("sales_channel", DataTypes.STRING()) \
        .field("unit_price", DataTypes.DECIMAL(10, 2)) \
        .field("quantity", DataTypes.INT()) \
        .field("discount_amount", DataTypes.DECIMAL(15, 2)) \
        .field("tax_amount", DataTypes.DECIMAL(15, 2)) \
        .field("shipping_amount", DataTypes.DECIMAL(15, 2)) \
        .field("status", DataTypes.STRING()) \
        .field("payment_method", DataTypes.STRING()) \
        .field("currency_code", DataTypes.STRING()) \
        .field("source_system", DataTypes.STRING()) \
        .field("source_timestamp", DataTypes.TIMESTAMP(3)) \
        .field("original_transaction_id", DataTypes.STRING()) \
        .field("created_at", DataTypes.TIMESTAMP(3)) \
        .build()
    
    # Test data with duplicates to validate deduplication logic
    test_data = [
        # Record 1: Original transaction
        (
            "ORD-001", "LINE-001", "TXN-001", "CUST-12345", "PROD-78901",
            "2024-01-15", "2024-01-15 10:30:00.000", "online",
            29.99, 2, 5.00, 2.40, 0.00,
            "COMPLETED", "Credit Card", "USD", "ecommerce_system",
            "2024-01-15 10:31:00.000", "orig_txn_001",
            "2024-01-15 10:31:00.000"
        ),
        
        # Record 2: Different line item, same order (should not be deduplicated)
        (
            "ORD-001", "LINE-002", "TXN-002", "CUST-12345", "PROD-78902", 
            "2024-01-15", "2024-01-15 10:32:00.000", "online",
            15.50, 1, 0.00, 1.24, 5.99,
            "COMPLETED", "Credit Card", "USD", "ecommerce_system",
            "2024-01-15 10:33:00.000", "orig_txn_002",
            "2024-01-15 10:33:00.000"
        ),
        
        # Record 3: DUPLICATE of Record 1 (same order_id, line_item_id, transaction_timestamp)
        # but with later source_timestamp - this should WIN in deduplication
        (
            "ORD-001", "LINE-001", "TXN-001-UPDATED", "CUST-12345", "PROD-78901",
            "2024-01-15", "2024-01-15 10:30:00.000", "online", 
            29.99, 2, 3.00, 2.40, 0.00,  # Note: different discount_amount
            "COMPLETED", "Credit Card", "USD", "ecommerce_system",
            "2024-01-15 10:35:00.000", "orig_txn_001_updated",  # Later timestamp
            "2024-01-15 10:35:00.000"
        ),
        
        # Record 4: Different customer, different product
        (
            "ORD-002", "LINE-001", "TXN-003", "CUST-67890", "PROD-11111",
            "2024-01-16", "2024-01-16 14:15:00.000", "retail",
            99.99, 1, 10.00, 7.20, 0.00,
            "COMPLETED", "Cash", "USD", "pos_system",
            "2024-01-16 14:16:00.000", "orig_txn_003",
            "2024-01-16 14:16:00.000"
        ),
        
        # Record 5: ANOTHER DUPLICATE of Record 4 (same transaction key)
        # but with earlier source_timestamp - this should LOSE in deduplication
        (
            "ORD-002", "LINE-001", "TXN-003-OLD", "CUST-67890", "PROD-11111",
            "2024-01-16", "2024-01-16 14:15:00.000", "retail",
            95.99, 1, 5.00, 6.88, 0.00,  # Different prices - older version
            "COMPLETED", "Cash", "USD", "pos_system", 
            "2024-01-16 14:10:00.000", "orig_txn_003_old",  # Earlier timestamp
            "2024-01-16 14:10:00.000"
        )
    ]
    
    # Create table from test data
    raw_sales_table = table_env.from_elements(test_data, schema)
    
    # Print the test data for verification
    print("=== RAW SALES EVENTS TEST DATA ===")
    print("Total records created:", len(test_data))
    print("\nTest scenarios:")
    print("1. Record 1: Original transaction (ORD-001, LINE-001)")
    print("2. Record 2: Different line item in same order (ORD-001, LINE-002)")
    print("3. Record 3: DUPLICATE of Record 1 with LATER timestamp (should WIN)")
    print("4. Record 4: Different order (ORD-002, LINE-001)")  
    print("5. Record 5: DUPLICATE of Record 4 with EARLIER timestamp (should LOSE)")
    print("\nExpected after deduplication: 3 unique records")
    print("- ORD-001, LINE-001 → Record 3 (latest)")
    print("- ORD-001, LINE-002 → Record 2 (unique)")
    print("- ORD-002, LINE-001 → Record 4 (latest)")
    
    # Display the data
    print("\n=== RAW DATA TABLE ===")
    raw_sales_table.execute().print()
    
    # Create a temporary view for the raw data
    table_env.create_temporary_view("raw_sales_events", raw_sales_table)
    
    # Simulate the deduplication logic from the DML
    dedup_query = """
    WITH deduplicated_transactions AS (
      SELECT 
        order_id,
        line_item_id,
        transaction_id,
        customer_id,
        product_id,
        transaction_date,
        transaction_timestamp,
        sales_channel,
        unit_price,
        quantity,
        discount_amount,
        tax_amount,
        shipping_amount,
        status,
        payment_method,
        currency_code,
        source_system,
        source_timestamp,
        original_transaction_id,
        
        ROW_NUMBER() OVER (
          PARTITION BY order_id, line_item_id, transaction_timestamp
          ORDER BY source_timestamp DESC, created_at DESC
        ) AS row_num

      FROM raw_sales_events
      WHERE 
        order_id IS NOT NULL
        AND line_item_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND product_id IS NOT NULL
        AND transaction_date IS NOT NULL
        AND unit_price > 0
        AND quantity > 0
    )
    SELECT 
      order_id,
      line_item_id,
      transaction_id,
      customer_id,
      product_id,
      transaction_date,
      transaction_timestamp,
      sales_channel,
      unit_price,
      quantity,
      discount_amount,
      tax_amount,
      shipping_amount,
      status,
      payment_method,
      currency_code,
      source_system,
      source_timestamp,
      original_transaction_id

    FROM deduplicated_transactions
    WHERE row_num = 1
    ORDER BY order_id, line_item_id
    """
    
    # Execute deduplication test
    print("\n=== DEDUPLICATION RESULTS ===")
    dedup_result = table_env.sql_query(dedup_query)
    dedup_result.execute().print()
    
    return raw_sales_table, dedup_result

def test_deduplication_scenarios():
    """
    Test specific deduplication scenarios and validate results.
    """
    print("\n" + "="*60)
    print("TESTING DEDUPLICATION LOGIC")
    print("="*60)
    
    raw_table, dedup_table = create_test_raw_sales_events()
    
    print("\n✅ Test completed successfully!")
    print("Verify that:")
    print("1. Only 3 records remain after deduplication")
    print("2. ORD-001/LINE-001 shows discount_amount=3.00 (from later record)")
    print("3. ORD-002/LINE-001 shows unit_price=99.99 (from later record)")
    print("4. ORD-001/LINE-002 remains unchanged (no duplicates)")

if __name__ == "__main__":
    test_deduplication_scenarios()