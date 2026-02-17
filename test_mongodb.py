from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("Testing MongoDB connection...")
    print(f"Connection string: {os.getenv('MONGODB_URI')[:50]}...")  # Show first 50 chars only
    
    client = MongoClient(os.getenv('MONGODB_URI'))
    
    # Test the connection
    client.admin.command('ping')
    print(" MongoDB connection successful!")
    
    # List databases
    print(f"Available databases: {client.list_database_names()}")
    
    client.close()
    
except Exception as e:
    print(f" MongoDB connection failed: {str(e)}")