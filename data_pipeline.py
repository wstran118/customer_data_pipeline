import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Create output directory
if not os.path.exists('grocery_data'):
    os.makedirs('grocery_data')

def generate_mock_customers(n=100):
    """Generate mock customer data"""
    customer_ids = [f'CUST_{i:05d}' for i in range(1, n+1)]
    first_names = ['John', 'Jane', 'Mike', 'Sarah', 'Emma', 'David', 'Lisa', 'Chris', 'Amy', 'Tom']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
    
    customers = {
        'customer_id': customer_ids,
        'first_name': [random.choice(first_names) for _ in range(n)],
        'last_name': [random.choice(last_names) for _ in range(n)],
        'email': [f"{fn.lower()}.{ln.lower()}@example.com" for fn, ln in zip(
            [random.choice(first_names) for _ in range(n)], 
            [random.choice(last_names) for _ in range(n)]
        )],
        'city': [random.choice(cities) for _ in range(n)],
        'join_date': [(datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825))).strftime('%Y-%m-%d') for _ in range(n)]
    }
    return pd.DataFrame(customers)

def generate_mock_purchases(n=1000):
    """Generate mock purchase data"""
    customer_ids = [f'CUST_{i:05d}' for i in range(1, 101)]
    products = ['Milk', 'Bread', 'Eggs', 'Cheese', 'Yogurt', 'Apples', 'Bananas', 'Chicken', 'Pasta', 'Rice']
    stores = ['Store_A', 'Store_B', 'Store_C']
    
    purchases = {
        'purchase_id': [f'PUR_{i:06d}' for i in range(1, n+1)],
        'customer_id': [random.choice(customer_ids) for _ in range(n)],
        'product': [random.choice(products) for _ in range(n)],
        'quantity': [random.randint(1, 5) for _ in range(n)],
        'price': [round(random.uniform(1.99, 9.99), 2) for _ in range(n)],
        'store_id': [random.choice(stores) for _ in range(n)],
        'purchase_date': [(datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(n)]
    }
    return pd.DataFrame(purchases)

def clean_customer_data(df):
    """Clean customer data"""
    # Remove duplicate emails
    df = df.drop_duplicates(subset='email', keep='first')
    
    # Validate email format
    df['email'] = df['email'].str.lower()
    df = df[df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$')]
    
    # Convert join_date to datetime
    df['join_date'] = pd.to_datetime(df['join_date'])
    
    return df

def clean_purchase_data(df):
    """Clean purchase data"""
    # Remove negative or zero quantities and prices
    df = df[(df['quantity'] > 0) & (df['price'] > 0)]
    
    # Convert purchase_date to datetime
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    
    # Calculate total amount
    df['total_amount'] = df['quantity'] * df['price']
    
    return df

def transform_data(customers_df, purchases_df):
    """Transform and aggregate data"""
    # Merge customer and purchase data
    merged_df = purchases_df.merge(customers_df, on='customer_id', how='left')
    
    # Calculate customer metrics
    customer_summary = merged_df.groupby('customer_id').agg({
        'total_amount': ['sum', 'count'],
        'purchase_date': 'max',
        'city': 'first'
    }).reset_index()
    
    customer_summary.columns = ['customer_id', 'total_spent', 'purchase_count', 'last_purchase', 'city']
    
    # Calculate customer tenure
    customer_summary = customer_summary.merge(
        customers_df[['customer_id', 'join_date']],
        on='customer_id'
    )
    customer_summary['tenure_days'] = (pd.to_datetime('2025-06-27') - customer_summary['join_date']).dt.days
    
    return customer_summary

def save_data(df, filename):
    """Save dataframe to CSV"""
    df.to_csv(f'grocery_data/{filename}', index=False)

def main():
    # Generate mock data
    customers = generate_mock_customers(100)
    purchases = generate_mock_purchases(1000)
    
    # Clean data
    cleaned_customers = clean_customer_data(customers)
    cleaned_purchases = clean_purchase_data(purchases)
    
    # Transform data
    customer_summary = transform_data(cleaned_customers, cleaned_purchases)
    
    # Save data
    save_data(cleaned_customers, 'customers.csv')
    save_data(cleaned_purchases, 'purchases.csv')
    save_data(customer_summary, 'customer_summary.csv')
    
    print("Data pipeline completed successfully!")
    print(f"Processed {len(cleaned_customers)} customers and {len(cleaned_purchases)} purchases")
    print(f"Generated customer summary with {len(customer_summary)} records")

if __name__ == "__main__":
    main()