import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Page config
st.set_page_config(
    page_title="C360 Customer Analytics",
    page_icon="📊",
    layout="wide"
)

# Initialize connection to DuckDB
@st.cache_resource
def init_connection():
    # Get the directory where this script is located
    current_dir = Path(__file__).parent.parent.parent
    db_path = current_dir / "data" / "c360_analytics.duckdb"
    csv_path = current_dir / "data" / "customer_analytics_c360.csv"
    
    # Create connection
    conn = duckdb.connect(database=str(db_path), read_only=False)
    
    # Create table from CSV if it exists
    if csv_path.exists():
        conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_analytics_c360 AS 
            SELECT * FROM read_csv_auto(?, header=true, sample_size=-1);
        """, [str(csv_path)])
        
    return conn

conn = init_connection()

# Load data
@st.cache_data
def load_customer_data():
    try:
        return conn.execute("""
            SELECT *
            FROM customer_analytics_c360
        """).df()
    except Exception as e:
        st.error(f"""
            Error: Could not load customer analytics data.
            Make sure to run the Spark pipeline first:
            
            cd c360_spark_processing
            ./run_pipeline.sh
            
            Original error: {str(e)}
        """)
        return None

# Main dashboard
st.title("📊 C360 Customer Analytics Dashboard")

# Load data
df = load_customer_data()

if df is not None:
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Customers", len(df))
    with col2:
        st.metric("Avg Lifetime Value", f"${df['lifetime_value'].mean():,.2f}")
    with col3:
        st.metric("Total Orders", df['total_transactions'].sum())
    with col4:
        st.metric("Active Customers", len(df[df['customer_status'] == 'Active']))

    # Customer segments visualization
    st.subheader("Customer Segments by Lifetime Value")
    fig = px.histogram(df, x="lifetime_value", nbins=50,
                      title="Distribution of Customer Lifetime Value")
    st.plotly_chart(fig, use_container_width=True)

    # Customer Status Distribution
    st.subheader("Customer Status Distribution")
    status_counts = df['customer_status'].value_counts()
    fig = px.pie(values=status_counts.values, names=status_counts.index,
                 title="Customer Status Distribution")
    st.plotly_chart(fig, use_container_width=True)

    # Customer Health Score Distribution
    st.subheader("Customer Health Score Distribution")
    fig = px.histogram(df, x="customer_health_score", nbins=30,
                      title="Distribution of Customer Health Scores")
    st.plotly_chart(fig, use_container_width=True)

    # Customer Details
    st.subheader("Customer Details")
    selected_columns = ['customer_id', 'first_name', 'last_name', 'customer_status', 
                       'loyalty_tier', 'total_spent', 'customer_health_score']
    st.dataframe(df[selected_columns].sort_values('customer_health_score', ascending=False),
                use_container_width=True)

    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        
        # Customer Status filter
        status_filter = st.multiselect(
            "Customer Status",
            options=sorted(df['customer_status'].unique()),
            default=sorted(df['customer_status'].unique())
        )
        
        # Loyalty Tier filter
        loyalty_filter = st.multiselect(
            "Loyalty Tier",
            options=sorted(df['loyalty_tier'].unique()),
            default=sorted(df['loyalty_tier'].unique())
        )
        
        # Health Score Range
        health_score_range = st.slider(
            "Health Score Range",
            min_value=float(df['customer_health_score'].min()),
            max_value=float(df['customer_health_score'].max()),
            value=(float(df['customer_health_score'].min()), 
                  float(df['customer_health_score'].max()))
        )