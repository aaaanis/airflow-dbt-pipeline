"""
Monitoring Pipeline DAG

This DAG implements the monitoring components in the data engineering pipeline.
It handles operations and integrates monitoring at each stage of the data pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.plugins.operators.monitoring_operators import (
    UpdateDataFreshnessOperator,
    UpdatePipelineStatusOperator,
    CheckDataFreshnessOperator
)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'monitoring_pipeline',
    default_args=default_args,
    description='Production monitoring integration for the data pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['monitoring', 'production'],
)

# Pipeline start
start = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Update pipeline status to running
update_pipeline_start = UpdatePipelineStatusOperator(
    task_id='update_pipeline_start',
    pipeline_name='data_pipeline',
    status=True,
    dag=dag,
)

# Data extraction
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='echo "Extracting data from source..." && sleep 5',
    dag=dag,
)

# Data transformation
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='echo "Transforming data..." && sleep 10',
    dag=dag,
)

# Load data to dim_customers
load_customers = BashOperator(
    task_id='load_customers',
    bash_command='echo "Loading customer data..." && sleep 3',
    dag=dag,
)

# Update dim_customers freshness
update_customers_freshness = UpdateDataFreshnessOperator(
    task_id='update_customers_freshness',
    table_name='dim_customers',
    dag=dag,
)

# Load data to dim_products
load_products = BashOperator(
    task_id='load_products',
    bash_command='echo "Loading product data..." && sleep 3',
    dag=dag,
)

# Update dim_products freshness
update_products_freshness = UpdateDataFreshnessOperator(
    task_id='update_products_freshness',
    table_name='dim_products',
    dag=dag,
)

# Load data to fact_orders
load_orders = BashOperator(
    task_id='load_orders',
    bash_command='echo "Loading order data..." && sleep 3',
    dag=dag,
)

# Update fact_orders freshness
update_orders_freshness = UpdateDataFreshnessOperator(
    task_id='update_orders_freshness',
    table_name='fact_orders',
    dag=dag,
)

# Check data freshness for all tables
check_all_freshness = CheckDataFreshnessOperator(
    task_id='check_all_freshness',
    dag=dag,
)

# Occasionally failing task (realistic error handling scenario)
def handle_potential_failure(**context):
    """
    Handles potential failure scenarios that may occur in production.
    In real pipelines, tasks may fail for various reasons.
    """
    import random
    
    # Simulate periodic failures that happen in production environments
    if random.random() < 0.2:
        raise Exception("Pipeline encountered an error that needs attention!")
    
    return "Task completed successfully!"

error_handling_task = PythonOperator(
    task_id='error_handling_task',
    python_callable=handle_potential_failure,
    dag=dag,
)

# Handle success
success_handler = UpdatePipelineStatusOperator(
    task_id='success_handler',
    pipeline_name='data_pipeline',
    status=True,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# Handle failure
failure_handler = UpdatePipelineStatusOperator(
    task_id='failure_handler',
    pipeline_name='data_pipeline',
    status=False,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Pipeline end
end = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# Define the workflow
start >> update_pipeline_start >> extract_data >> transform_data

transform_data >> load_customers >> update_customers_freshness
transform_data >> load_products >> update_products_freshness
transform_data >> load_orders >> update_orders_freshness

[update_customers_freshness, update_products_freshness, update_orders_freshness] >> check_all_freshness

check_all_freshness >> error_handling_task >> [success_handler, failure_handler] >> end 