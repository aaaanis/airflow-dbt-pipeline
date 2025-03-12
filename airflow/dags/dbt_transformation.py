from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define paths
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# Define the DAG
dag = DAG(
    'dbt_transformation',
    default_args=default_args,
    description='Run dbt transformations on the data warehouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transform', 'dbt', 'data_engineering_showcase'],
)

# Define a function to run dbt commands
def run_dbt_command(command):
    """Helper function to create dbt command with common arguments."""
    return f"cd {DBT_PROJECT_DIR} && dbt {command} --profiles-dir {DBT_PROFILES_DIR} --target dev"

# Create a task to check dbt dependencies
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=run_dbt_command('deps'),
    dag=dag,
)

# Create a task to run dbt debug (checks configuration)
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command=run_dbt_command('debug'),
    dag=dag,
)

# Create a task to run dbt run (executes models)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=run_dbt_command('run'),
    dag=dag,
)

# Create a task to run dbt test (tests models)
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=run_dbt_command('test'),
    dag=dag,
)

# Create a task to generate dbt documentation
dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=run_dbt_command('docs generate'),
    dag=dag,
)

# Define task dependencies
dbt_deps >> dbt_debug >> dbt_run >> dbt_test >> dbt_docs_generate 