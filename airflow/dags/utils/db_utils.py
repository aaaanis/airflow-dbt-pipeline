import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def get_warehouse_connection():
    """Create a connection to the data warehouse."""
    try:
        conn = psycopg2.connect(
            host="warehouse",
            port=5432,
            database="warehouse",
            user="warehouse",
            password="warehouse"
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to warehouse database: {e}")
        raise

def load_csv_to_table(csv_file_path, table_name, schema="raw"):
    """Load data from a CSV file into a specified table in the warehouse."""
    try:
        # Read CSV file into pandas DataFrame
        df = pd.read_csv(csv_file_path)
        
        # Get column names and data types
        columns = df.columns.tolist()
        
        # Connect to the warehouse
        conn = get_warehouse_connection()
        cursor = conn.cursor()
        
        # Truncate table if it exists
        cursor.execute(f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE;")
        
        # Prepare data for insertion
        tuples = [tuple(row) for row in df.to_numpy()]
        
        # Define the SQL statement
        query = f"""
            INSERT INTO {schema}.{table_name} ({', '.join(columns)})
            VALUES %s
        """
        
        # Execute the SQL statement with the data
        execute_values(cursor, query, tuples)
        
        # Commit the transaction
        conn.commit()
        
        logger.info(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error loading data from {csv_file_path} to {schema}.{table_name}: {e}")
        raise 