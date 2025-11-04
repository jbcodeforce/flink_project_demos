# C360 Customer Analytics Dashboard

A simple dashboard built with Streamlit and DuckDB to visualize Customer 360 analytics data. This dashboard demonstrates the end result of the data products created by our batch and streaming pipelines.

## Pipeline Flow

1. **Data Processing (Apache Spark)**
   - Raw data is processed using Spark SQL
   - Creates enriched customer analytics view
   - Exports results to CSV for dashboard consumption

2. **Data Storage (DuckDB)**
   - CSV data is loaded into DuckDB
   - Provides fast analytics queries for the dashboard

3. **Data Visualization (Streamlit)**
   - Interactive dashboard showing key customer metrics
   - Real-time filtering and exploration capabilities
   - Direct connection to DuckDB for performance

## Features

- Real-time customer analytics visualization
- Key customer metrics and KPIs
- Interactive data exploration
- Direct integration with processed C360 data

## Setup

1. Install uv (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Create virtual environment and install dependencies:
```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

3. Run the Spark pipeline to process data:
```bash
cd ../c360_spark_processing
./run_pipeline.sh
cd ../c360_dashboard
```

4. Run the dashboard:
```bash
cd src/c360_dashboard
streamlit run dashboard.py
```


## Dashboard Components

- Customer Overview
  - Total customers
  - Average lifetime value
  - Active vs churned ratio
  - Customer health distribution

- Customer Segmentation
  - Status distribution
  - Loyalty tier breakdown
  - Health score analysis

- Customer Details
  - Individual customer profiles
  - Transaction history
  - Engagement metrics
  - Support interaction history

## Data Sources

The dashboard visualizes the customer_analytics_c360 data product which combines:
- Customer profile information
- Transaction history
- Loyalty program data
- Support interactions
- App usage metrics
- Customer health indicators