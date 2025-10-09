import duckdb
import pandas as pd
from pathlib import Path

def init_database():
    """Initialize DuckDB database with C360 schema and sample data"""
    # Create database connection
    db_path = Path('data/c360_analytics.duckdb')
    conn = duckdb.connect(database=str(db_path))
    
    # Create tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            signup_date DATE
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            transaction_date TIMESTAMP,
            total_amount DECIMAL(10,2),
            payment_method VARCHAR,
            status VARCHAR,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transaction_items (
            item_id VARCHAR PRIMARY KEY,
            transaction_id VARCHAR,
            product_id VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_price DECIMAL(10,2),
            FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_program (
            customer_id VARCHAR PRIMARY KEY,
            membership_level VARCHAR,
            points_balance INTEGER,
            join_date DATE,
            last_points_update DATE,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # Load data from CSV files
    try:
        mock_data_dir = Path('../../c360_mock_data')
        
        # Load customers
        customers_df = pd.read_csv(mock_data_dir / 'customer/customers.csv')
        conn.execute("INSERT INTO customers SELECT * FROM customers_df")
        
        # Load transactions
        transactions_df = pd.read_csv(mock_data_dir / 'sales/transactions.csv')
        conn.execute("INSERT INTO transactions SELECT * FROM transactions_df")
        
        # Load transaction items
        items_df = pd.read_csv(mock_data_dir / 'sales/transaction_items.csv')
        conn.execute("INSERT INTO transaction_items SELECT * FROM items_df")
        
        # Load loyalty data
        loyalty_df = pd.read_csv(mock_data_dir / 'customer/loyalty_program.csv')
        conn.execute("INSERT INTO loyalty_program SELECT * FROM loyalty_df")
        
        # Create analytics view
        conn.execute("""
            CREATE OR REPLACE VIEW customer_analytics AS
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name as customer_name,
                c.email,
                c.city,
                c.state,
                c.country,
                COUNT(DISTINCT t.transaction_id) as total_transactions,
                SUM(t.total_amount) as total_spent,
                MAX(t.transaction_date) as last_transaction_date,
                l.membership_level,
                l.points_balance
            FROM customers c
            LEFT JOIN transactions t ON c.customer_id = t.customer_id
            LEFT JOIN loyalty_program l ON c.customer_id = l.customer_id
            GROUP BY 
                c.customer_id, c.first_name, c.last_name, c.email,
                c.city, c.state, c.country,
                l.membership_level, l.points_balance
        """)
        
        print("Database initialized successfully!")
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        print("Make sure the mock data CSV files exist in the correct location")
    
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()