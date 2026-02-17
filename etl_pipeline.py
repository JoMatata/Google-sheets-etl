"""
ETL PIPELINE: Google sheets to Database
This script extracts data from Google Sheets, transforms it and loads it into postgreSQL and MongoDB databases
"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import psycopg2
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timezone

# Loading environment variables from .env file
load_dotenv()

#Configure logging to track my ETL process
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

#EXTRACT - Connecting to google sheets
def extract_from_google_sheets():
    """
    Extract data from Google Sheets.

    Returns:
        pandas.DataFrame: Extracted data from the sheet
    """
    try:
        logger.info("Starting data extraction from google sheets...")
        #Define the scopes for google sheets and Drive API
        scope=[
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        #Load credentials from the JSON file
        credentials= ServiceAccountCredentials.from_json_keyfile_name(
            os.getenv('GOOGLE_CREDENTIALS_FILE'),
            scope
        )

        #Authorize the client
        client = gspread.authorize(credentials)

        #Open spreadsheet using ID
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        sheet = client.open_by_key(spreadsheet_id).sheet1 #Helps usto get the first sheet

        #Get all records as a list of dictionaries
        data = sheet.get_all_records()

        #Convert to pandas DataFrame to make it easier to manipulate
        df = pd.DataFrame(data)

        logger.info(f"Succesfully extracted {len(df)} rows from Google Sheets")
        logger.info(f"Columns found: {list(df.columns)}")

        return df

    except Exception as e:
        logger.error(f"Error extracting data from Google Sheets: {str(e)}")
        raise

#TRANSFORM - Cleaning and Selecting Columns
def transform_data(df):
    """
    Transform and clean the data.
    Selects required columns and handles data validation.
    
    Args:
        df (pandas.DataFrame): Raw data from Google Sheets
        
    Returns:
        pandas.DataFrame: Transformed data
    """
    try:
        logger.info("Starting data transformation...")
        
        # Define required columns
        required_columns = [
            'id',
            'quantity',
            'product_name',
            'total_amount',
            'payment_method',
            'customer_type'
        ]
        
        # Check if all required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Select only required columns
        df_transformed = df[required_columns].copy()
        
        # Data cleaning and validation
        # Remove rows where id is null or empty
        initial_rows = len(df_transformed)
        df_transformed = df_transformed.dropna(subset=['id'])
        df_transformed = df_transformed[df_transformed['id'].astype(str).str.strip() != '']
        rows_after_cleaning = len(df_transformed)
        
        if initial_rows != rows_after_cleaning:
            logger.warning(f"Removed {initial_rows - rows_after_cleaning} rows with null/empty IDs")
        
        # Convert data types for consistency
        # Keep ID as string (it's a UUID)
        df_transformed['id'] = df_transformed['id'].astype(str).str.strip()
        
        # Convert numeric columns
        df_transformed['quantity'] = pd.to_numeric(df_transformed['quantity'], errors='coerce')
        df_transformed['total_amount'] = pd.to_numeric(df_transformed['total_amount'], errors='coerce')
        
        # Fill any remaining NaN values in numeric columns with 0
        df_transformed['quantity'] = df_transformed['quantity'].fillna(0)
        df_transformed['total_amount'] = df_transformed['total_amount'].fillna(0)
        
        # Clean string columns (remove extra spaces)
        df_transformed['product_name'] = df_transformed['product_name'].astype(str).str.strip()
        df_transformed['payment_method'] = df_transformed['payment_method'].astype(str).str.strip()
        df_transformed['customer_type'] = df_transformed['customer_type'].astype(str).str.strip()
        
        # Remove any potential empty strings and replace with NULL-equivalent
        df_transformed['product_name'] = df_transformed['product_name'].replace('', 'Unknown')
        df_transformed['payment_method'] = df_transformed['payment_method'].replace('', 'Unknown')
        df_transformed['customer_type'] = df_transformed['customer_type'].replace('', 'Unknown')
        
        logger.info(f"Transformation complete. Final dataset: {len(df_transformed)} rows")
        logger.info(f"Data types after transformation:\n{df_transformed.dtypes}")
        
        return df_transformed
    
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

#Loading to PostgreSQL 
def load_to_postgresql(df):
    """
    Load data into PostgreSQL database using bulk insert.
    Creates table if it doesn't exist and inserts data efficiently.
    
    Args:
        df (pandas.DataFrame): Transformed data to load
    """
    conn = None
    cursor = None
    
    try:
        logger.info("Connecting to PostgreSQL...")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            database=os.getenv('POSTGRES_DATABASE'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cursor = conn.cursor()
        
        # Drop existing table if it exists (for fresh start)
        cursor.execute("DROP TABLE IF EXISTS sales_data;")
        logger.info("Dropped existing table (if any)")
        
        # Create table with VARCHAR id (for UUIDs)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sales_data (
            id VARCHAR(255) PRIMARY KEY,
            quantity NUMERIC,
            product_name VARCHAR(255),
            total_amount NUMERIC,
            payment_method VARCHAR(100),
            customer_type VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Table 'sales_data' created successfully")
        
        # Prepare data for bulk insert
        logger.info(f"Preparing to insert {len(df)} rows...")
        
        # Convert DataFrame to list of tuples for bulk insert
        data_tuples = []
        for index, row in df.iterrows():
            data_tuples.append((
                str(row['id']),
                float(row['quantity']),
                str(row['product_name']),
                float(row['total_amount']),
                str(row['payment_method']),
                str(row['customer_type'])
            ))
        
        # Use execute_values for fast bulk insert (much faster than executemany)
        from psycopg2.extras import execute_values
        
        insert_query = """
        INSERT INTO sales_data 
            (id, quantity, product_name, total_amount, payment_method, customer_type)
        VALUES %s
        ON CONFLICT (id) 
        DO UPDATE SET
            quantity = EXCLUDED.quantity,
            product_name = EXCLUDED.product_name,
            total_amount = EXCLUDED.total_amount,
            payment_method = EXCLUDED.payment_method,
            customer_type = EXCLUDED.customer_type;
        """
        
        # Insert in batches of 1000 for better performance and progress tracking
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            execute_values(cursor, insert_query, batch)
            total_inserted += len(batch)
            logger.info(f"Progress: {total_inserted}/{len(df)} rows inserted ({(total_inserted/len(df)*100):.1f}%)")
        
        conn.commit()
        logger.info(f"Successfully loaded {total_inserted} rows to PostgreSQL")
        
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    
    except Exception as e:
        logger.error(f"Error loading to PostgreSQL: {str(e)}")
        raise
    
    finally:
        # Clean up connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed")

#Load to MongoDB
def load_to_mongodb(df):
    """
    Load data into MongoDB database.
    Creates collection if it doesn't exist and inserts documents.
    
    Args:
        df (pandas.DataFrame): Transformed data to load
    """
    client = None
    
    try:
        logger.info("Connecting to MongoDB...")
        
        # Connect to MongoDB
        mongodb_uri = os.getenv('MONGODB_URI')
        client = MongoClient(mongodb_uri)
        
        # Select database and collection
        db_name = os.getenv('MONGODB_DATABASE')
        db = client[db_name]
        collection = db['sales_data']
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Add metadata to each document
        for record in records:
            record['inserted_at'] = datetime.now(timezone.utc)
            # Keep id as string (UUID)
            record['id'] = str(record['id'])
            record['quantity'] = float(record['quantity'])
            record['total_amount'] = float(record['total_amount'])
        
        # Clear existing data (for fresh load)
        deleted_count = collection.delete_many({}).deleted_count
        if deleted_count > 0:
            logger.info(f"Cleared {deleted_count} existing documents from MongoDB collection")
        
        # Insert all documents
        result = collection.insert_many(records)
        logger.info(f"Successfully loaded {len(result.inserted_ids)} documents to MongoDB")
        
        # Create index on id field for better query performance
        collection.create_index('id', unique=True)
        logger.info("Created unique index on 'id' field")
        
    except Exception as e:
        logger.error(f"Error loading to MongoDB: {str(e)}")
        raise
    
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")

#MAIN ETL PIPELINE FUNCTION
def run_etl_pipeline():
    """
    Main ETL pipeline orchestrator.
    Coordinates the Extract, Transform, and Load steps.
    """
    try:
        logger.info("=" * 50)
        logger.info("ETL PIPELINE STARTED")
        logger.info("=" * 50)
        
        # Step 1: Extract
        df_raw = extract_from_google_sheets()
        logger.info(f"Preview of raw data:\n{df_raw.head()}")
        
        # Step 2: Transform
        df_transformed = transform_data(df_raw)
        logger.info(f"Preview of transformed data:\n{df_transformed.head()}")
        
        # Step 3: Load to PostgreSQL
        load_to_postgresql(df_transformed)
        
        # Step 4: Load to MongoDB
        load_to_mongodb(df_transformed)
        
        logger.info("=" * 50)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error("=" * 50)
        logger.error(f"ETL PIPELINE FAILED: {str(e)}")
        logger.error("=" * 50)
        raise

# Entry point
if __name__ == "__main__":
    run_etl_pipeline()





        

