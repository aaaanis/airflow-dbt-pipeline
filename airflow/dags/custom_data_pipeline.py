"""
Custom data pipeline that demonstrates the use of custom sensors and config files.
"""

from datetime import datetime, timedelta
import os
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import custom plugins
from plugins.custom_sensors import FileContentSensor, DataFreshnessTimeSensor
from utils.config_reader import get_data_source_config
from utils.db_utils import load_csv_to_table

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
    'custom_data_pipeline',
    default_args=default_args,
    description='Custom data pipeline using plugins and config',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['custom', 'data_engineering_showcase'],
)

# Define a function to get the last modified time of a file
def get_file_timestamp(context, file_path=None):
    """Get the last modified time of a file."""
    if file_path is None:
        # Get the file path from the configuration
        file_config = get_data_source_config('file', 'customer_data')
        file_path = file_config.get('path') if file_config else '/opt/airflow/data/customers.csv'
    
    try:
        return os.path.getmtime(file_path)
    except Exception as e:
        context['task_instance'].log.error(f"Error getting file timestamp: {e}")
        # Return a very old timestamp to indicate that the data is stale
        return 0


# Create a task to check if the customer data is fresh
check_customer_data_freshness = DataFreshnessTimeSensor(
    task_id='check_customer_data_freshness',
    data_timestamp_callable=get_file_timestamp,
    max_age_in_seconds=86400,  # 24 hours
    poke_interval=60,  # Check every minute
    timeout=3600,  # Timeout after 1 hour
    mode='poke',
    dag=dag,
)

# Create a task to check if the orders file contains completed orders
check_orders_completed = FileContentSensor(
    task_id='check_orders_completed',
    filepath='/opt/airflow/data/orders.csv',
    content_pattern='Completed',
    poke_interval=60,  # Check every minute
    timeout=3600,  # Timeout after 1 hour
    mode='poke',
    dag=dag,
)

# Define a task to load customer data
def load_customers_with_config(**kwargs):
    """Load customer data using configuration."""
    # Get configuration for customer data
    file_config = get_data_source_config('file', 'customer_data')
    
    if not file_config:
        raise ValueError("Customer data configuration not found")
    
    # Load data
    file_path = file_config.get('path')
    load_csv_to_table(file_path, 'customers')
    
    return f"Loaded customer data from {file_path}"

# Create a task to load customer data
load_customers_task = PythonOperator(
    task_id='load_customers_task',
    python_callable=load_customers_with_config,
    provide_context=True,
    dag=dag,
)

# Define a task to load order data
def load_orders_with_config(**kwargs):
    """Load order data using configuration."""
    # Get configuration for order data
    file_config = get_data_source_config('file', 'order_data')
    
    if not file_config:
        raise ValueError("Order data configuration not found")
    
    # Load data
    file_path = file_config.get('path')
    load_csv_to_table(file_path, 'orders')
    
    return f"Loaded order data from {file_path}"

# Create a task to load order data
load_orders_task = PythonOperator(
    task_id='load_orders_task',
    python_callable=load_orders_with_config,
    provide_context=True,
    dag=dag,
)

# Create a task to run dbt
run_dbt_task = BashOperator(
    task_id='run_dbt_task',
    bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --models staging.stg_customers staging.stg_orders",
    dag=dag,
)

# Define task dependencies
check_customer_data_freshness >> load_customers_task
check_orders_completed >> load_orders_task
[load_customers_task, load_orders_task] >> run_dbt_task 