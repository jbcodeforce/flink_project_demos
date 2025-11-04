import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
from pathlib import Path

def generate_customer_data(num_customers=1000):
    np.random.seed(42)
    
    # Generate basic customer data
    customer_ids = [str(uuid.uuid4()) for _ in range(num_customers)]
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'James', 'Emma']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    
    customers = pd.DataFrame({
        'customer_id': customer_ids,
        'first_name': np.random.choice(first_names, num_customers),
        'last_name': np.random.choice(last_names, num_customers),
        'email': [f"{fn.lower()}.{ln.lower()}@example.com" for fn, ln in zip(
            np.random.choice(first_names, num_customers),
            np.random.choice(last_names, num_customers))],
        'phone': [f"+1-555-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}" 
                 for _ in range(num_customers)],
        'address': [f"{np.random.randint(100, 9999)} Main St" for _ in range(num_customers)],
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], num_customers),
        'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], num_customers),
        'country': 'USA',
        'signup_date': [datetime.now() - timedelta(days=np.random.randint(1, 1000)) 
                       for _ in range(num_customers)]
    })
    
    return customers, customer_ids

def generate_transaction_data(customer_ids, num_transactions=5000):
    transaction_ids = [str(uuid.uuid4()) for _ in range(num_transactions)]
    
    transactions = pd.DataFrame({
        'transaction_id': transaction_ids,
        'customer_id': np.random.choice(customer_ids, num_transactions),
        'transaction_date': [datetime.now() - timedelta(days=np.random.randint(1, 365)) 
                           for _ in range(num_transactions)],
        'total_amount': np.random.uniform(10, 1000, num_transactions).round(2),
        'payment_method': np.random.choice(['credit_card', 'debit_card', 'paypal'], num_transactions),
        'status': np.random.choice(['completed', 'pending', 'failed'], num_transactions, 
                                 p=[0.95, 0.03, 0.02])
    })
    
    return transactions, transaction_ids

def generate_transaction_items(transaction_ids, num_items=10000):
    product_ids = ['P001', 'P002', 'P003', 'P004', 'P005']
    product_prices = {
        'P001': 999.99,  # Laptop
        'P002': 699.99,  # Smartphone
        'P003': 199.99,  # Headphones
        'P004': 499.99,  # Tablet
        'P005': 299.99   # Smartwatch
    }
    
    # Generate random product IDs
    selected_products = np.random.choice(product_ids, num_items)
    
    items = pd.DataFrame({
        'item_id': [str(uuid.uuid4()) for _ in range(num_items)],
        'transaction_id': np.random.choice(transaction_ids, num_items),
        'product_id': selected_products,
        'quantity': np.random.randint(1, 5, num_items),
        'unit_price': [product_prices[p] for p in selected_products]
    })
    
    items['total_price'] = items['quantity'] * items['unit_price']
    return items

def generate_loyalty_data(customer_ids):
    loyalty = pd.DataFrame({
        'customer_id': customer_ids,
        'membership_level': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], len(customer_ids),
                                           p=[0.4, 0.3, 0.2, 0.1]),
        'points_balance': np.random.randint(0, 50000, len(customer_ids)),
        'join_date': [datetime.now() - timedelta(days=np.random.randint(1, 500)) 
                     for _ in range(len(customer_ids))],
        'last_points_update': [datetime.now() - timedelta(days=np.random.randint(1, 30)) 
                             for _ in range(len(customer_ids))]
    })
    
    return loyalty

def main():
    # Create mock data directory structure
    mock_data_dir = Path('../../c360_mock_data')
    (mock_data_dir / 'customer').mkdir(parents=True, exist_ok=True)
    (mock_data_dir / 'sales').mkdir(parents=True, exist_ok=True)
    
    # Generate data
    customers, customer_ids = generate_customer_data()
    transactions, transaction_ids = generate_transaction_data(customer_ids)
    items = generate_transaction_items(transaction_ids)
    loyalty = generate_loyalty_data(customer_ids)
    
    # Save data
    customers.to_csv(mock_data_dir / 'customer/customers.csv', index=False)
    transactions.to_csv(mock_data_dir / 'sales/transactions.csv', index=False)
    items.to_csv(mock_data_dir / 'sales/transaction_items.csv', index=False)
    loyalty.to_csv(mock_data_dir / 'customer/loyalty_program.csv', index=False)
    
    print("Mock data generated successfully!")

if __name__ == "__main__":
    main()