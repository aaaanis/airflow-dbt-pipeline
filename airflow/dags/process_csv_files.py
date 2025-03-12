import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

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
    'process_csv_files',
    default_args=default_args,
    description='Process CSV files and load to data warehouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['extract', 'csv', 'data_engineering_showcase'],
)

# Define file paths
data_dir = '/opt/airflow/data'
customers_file = os.path.join(data_dir, 'customers.csv')
orders_file = os.path.join(data_dir, 'orders.csv')
products_file = os.path.join(data_dir, 'products.csv')

# Define PythonOperator tasks

def load_customers_data(**kwargs):
    """Load customers data from CSV to the warehouse."""
    load_csv_to_table(customers_file, 'customers')
    return "Loaded customers data"

def load_orders_data(**kwargs):
    """Load orders data from CSV to the warehouse."""
    load_csv_to_table(orders_file, 'orders')
    return "Loaded orders data"

def load_products_data(**kwargs):
    """Load products data from CSV to the warehouse."""
    load_csv_to_table(products_file, 'products')
    return "Loaded products data"

# Create a task group for data loading
with TaskGroup(group_id='load_csv_data', dag=dag) as load_data_group:
    # Define tasks
    load_customers_task = PythonOperator(
        task_id='load_customers_data',
        python_callable=load_customers_data,
        provide_context=True,
    )

    load_orders_task = PythonOperator(
        task_id='load_orders_data',
        python_callable=load_orders_data,
        provide_context=True,
    )

    load_products_task = PythonOperator(
        task_id='load_products_data',
        python_callable=load_products_data,
        provide_context=True,
    )

    # Define task dependencies within the group
    # We can load these tables in parallel since they don't depend on each other
    [load_customers_task, load_orders_task, load_products_task]

# The TaskGroup itself is the only task in the main DAG
load_data_group 