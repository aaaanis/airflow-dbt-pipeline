import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from utils.api_utils import fetch_api_data, process_weather_data, save_api_data_to_json, load_weather_data_to_warehouse

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'extract_api_data',
    default_args=default_args,
    description='Extract data from various API sources',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['extract', 'api', 'data_engineering_showcase'],
)

# In a real-world scenario, we would fetch data from an actual API endpoint.
# For this showcase, we'll use a mock API endpoint
# In production, replace this with a real API URL
mock_api_url = 'https://api.example.com/weather'

# Define the directory where we'll store the fetched API data
data_dir = '/opt/airflow/data'

# Define PythonOperator tasks

# Define a task to fetch weather data from the API
def fetch_weather_data(**kwargs):
    # In a real-world scenario, we would fetch from an actual API
    # For this showcase, we'll just read the mock data file
    mock_data_path = os.path.join(data_dir, 'api_response.json')
    
    try:
        with open(mock_data_path, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        # If the mock data file doesn't exist, generate a simple response
        return {
            "weather_data": [
                {
                    "location": "New York",
                    "country": "USA",
                    "date": "2023-01-15",
                    "temperature": 32.5,
                    "humidity": 65,
                    "weather_condition": "Cloudy"
                }
            ]
        }

# Task to process the weather data
def process_and_load_weather_data(**kwargs):
    ti = kwargs['ti']
    weather_data = ti.xcom_pull(task_ids='fetch_weather_data_task')
    
    # Process the weather data
    weather_df = process_weather_data(weather_data)
    
    # Load the data to the warehouse
    load_weather_data_to_warehouse(weather_df)
    
    return f"Processed and loaded {len(weather_df)} weather records"

# Define the tasks
fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data_task',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

process_weather_data_task = PythonOperator(
    task_id='process_weather_data_task',
    python_callable=process_and_load_weather_data,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
fetch_weather_data_task >> process_weather_data_task 