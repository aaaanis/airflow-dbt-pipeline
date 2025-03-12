import json
import logging
import requests
import pandas as pd
from typing import Dict, List, Any, Optional
from .db_utils import get_warehouse_connection

logger = logging.getLogger(__name__)

def fetch_api_data(url: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Fetch data from an API endpoint.
    
    Args:
        url: The API endpoint URL
        headers: Optional headers for the API request
        params: Optional parameters for the API request
        
    Returns:
        The JSON response from the API
    """
    try:
        logger.info(f"Fetching data from API: {url}")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        logger.info(f"Successfully fetched data from API: {url}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {url}, Error: {e}")
        raise

def process_weather_data(data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Process weather data from API response.
    
    Args:
        data: The weather data JSON
        
    Returns:
        A pandas DataFrame with the processed data
    """
    try:
        # Extract weather data from the response
        weather_data = data.get('weather_data', [])
        
        if not weather_data:
            logger.warning("No weather data found in the API response")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(weather_data)
        
        # Perform any necessary data transformations
        # For example, convert temperature from Fahrenheit to Celsius
        # df['temperature'] = (df['temperature'] - 32) * 5/9
        
        logger.info(f"Successfully processed {len(df)} weather data records")
        return df
    except Exception as e:
        logger.error(f"Error processing weather data: {e}")
        raise

def save_api_data_to_json(data: Dict[str, Any], file_path: str) -> None:
    """
    Save API data to a JSON file.
    
    Args:
        data: The data to save
        file_path: The path to save the file to
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Successfully saved API data to {file_path}")
    except Exception as e:
        logger.error(f"Error saving API data to {file_path}: {e}")
        raise

def load_weather_data_to_warehouse(df: pd.DataFrame, table_name: str = "weather_data", schema: str = "raw") -> None:
    """
    Load weather data to the data warehouse.
    
    Args:
        df: The DataFrame containing the weather data
        table_name: The name of the table to load the data to
        schema: The schema to load the data to
    """
    try:
        if df.empty:
            logger.warning("No data to load to warehouse")
            return
        
        # Connect to the warehouse
        conn = get_warehouse_connection()
        cursor = conn.cursor()
        
        # Truncate table if it exists
        cursor.execute(f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE;")
        
        # Prepare data for insertion
        columns = df.columns.tolist()
        tuples = [tuple(row) for row in df.to_numpy()]
        
        # Define the SQL statement
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"""
            INSERT INTO {schema}.{table_name} ({', '.join(columns)})
            VALUES ({placeholders})
        """
        
        # Execute the SQL statement with the data
        cursor.executemany(query, tuples)
        
        # Commit the transaction
        conn.commit()
        
        logger.info(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error loading weather data to warehouse: {e}")
        raise 