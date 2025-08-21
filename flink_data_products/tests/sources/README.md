# Sales Events Test Data Generator

## Overview
This directory contains PyFlink Table API scripts to generate test data for validating deduplication logic in the sales pipeline.

## Test Script: `test_raw_sales_events.py`

### Purpose
Creates 5 test records for `raw_sales_events` table with intentional duplicates to validate the deduplication logic implemented in `src_saleops_sales_transactions`.

### Test Scenarios
1. **Record 1**: Original transaction (`ORD-001`, `LINE-001`)
2. **Record 2**: Different line item in same order (`ORD-001`, `LINE-002`) 
3. **Record 3**: **DUPLICATE** of Record 1 with **LATER** source timestamp (should WIN)
4. **Record 4**: Different order (`ORD-002`, `LINE-001`)
5. **Record 5**: **DUPLICATE** of Record 4 with **EARLIER** source timestamp (should LOSE)

### Expected Results
After deduplication: **3 unique records**
- `ORD-001`, `LINE-001` → Record 3 (latest timestamp wins)
- `ORD-001`, `LINE-002` → Record 2 (no duplicates)  
- `ORD-002`, `LINE-001` → Record 4 (latest timestamp wins)

## Running the Test

### Option 1: Docker Compose (Recommended)

#### Prerequisites
- Docker and Docker Compose installed
- Navigate to the tests directory: `cd /Users/jerome/Code/flink_project_demos/flink_data_products/tests`

#### Quick Start
```bash
# Use the helper script (easiest way)
./run_tests.sh

# Then run the test
docker-compose exec pyflink-runner python sources/test_raw_sales_events.py
```

#### Manual Setup
```bash
# Start the Flink cluster
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
docker-compose logs -f pyflink-runner

# Run the test
docker-compose exec pyflink-runner python sources/test_raw_sales_events.py

# Stop when done
docker-compose down
```

#### Monitor Flink Cluster
- **Flink Web UI**: http://localhost:8081
- **View Logs**: `docker-compose logs -f jobmanager taskmanager`
- **Container Status**: `docker-compose ps`

### Option 2: Local PyFlink (Alternative)

#### Prerequisites
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install PyFlink locally
uv pip install apache-flink==1.18.1 pyflink==1.18.1
```

#### Execute Test
```bash
uv run python sources/test_raw_sales_events.py
```

### Validation
The script will:
1. Display all 5 raw records
2. Apply deduplication logic 
3. Show final 3 deduplicated records
4. Verify deduplication rules worked correctly

## Deduplication Logic Tested
- **Partition Key**: `(order_id, line_item_id, transaction_timestamp)`
- **Ordering**: `source_timestamp DESC, created_at DESC` (latest wins)
- **Result**: Only one record per partition key combination