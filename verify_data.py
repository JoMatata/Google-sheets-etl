"""
Verification script to check if data was loaded correctly
"""

import psycopg2
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

def verify_postgresql():
    """Check PostgreSQL data"""
    print("\n" + "="*50)
    print("POSTGRESQL VERIFICATION")
    print("="*50)
    
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DATABASE'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = conn.cursor()
    
    # Count rows
    cursor.execute("SELECT COUNT(*) FROM sales_data;")
    count = cursor.fetchone()[0]
    print(f"\nTotal rows: {count}")
    
    # Show first 5 rows
    cursor.execute("SELECT * FROM sales_data LIMIT 5;")
    rows = cursor.fetchall()
    print("\nFirst 5 rows:")
    for row in rows:
        print(row)
    
    cursor.close()
    conn.close()

def verify_mongodb():
    """Check MongoDB data"""
    print("\n" + "="*50)
    print("MONGODB VERIFICATION")
    print("="*50)
    
    client = MongoClient(os.getenv('MONGODB_URI'))
    db = client[os.getenv('MONGODB_DATABASE')]
    collection = db['sales_data']
    
    # Count documents
    count = collection.count_documents({})
    print(f"\nTotal documents: {count}")
    
    # Show first 5 documents
    print("\nFirst 5 documents:")
    for doc in collection.find().limit(5):
        print(doc)
    
    client.close()

if __name__ == "__main__":
    verify_postgresql()
    verify_mongodb()

