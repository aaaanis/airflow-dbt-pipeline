from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    'full_etl_pipeline',
    default_args=default_args,
    description='Orchestrate the entire ETL process',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['pipeline', 'etl', 'data_engineering_showcase'],
)

# Create a task to mark the start of the pipeline
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Create a task to mark the end of the pipeline
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Create a task to mark the start of the extraction phase
start_extraction = DummyOperator(
    task_id='start_extraction',
    dag=dag,
)

# Create a task to mark the end of the extraction phase
end_extraction = DummyOperator(
    task_id='end_extraction',
    dag=dag,
)

# Create a task to trigger the CSV processing DAG
trigger_process_csv = TriggerDagRunOperator(
    task_id='trigger_process_csv',
    trigger_dag_id='process_csv_files',
    wait_for_completion=True,
    dag=dag,
)

# Create a task to trigger the API extraction DAG
trigger_extract_api = TriggerDagRunOperator(
    task_id='trigger_extract_api',
    trigger_dag_id='extract_api_data',
    wait_for_completion=True,
    dag=dag,
)

# Create a task to trigger the dbt transformation DAG
trigger_dbt_transformation = TriggerDagRunOperator(
    task_id='trigger_dbt_transformation',
    trigger_dag_id='dbt_transformation',
    wait_for_completion=True,
    dag=dag,
)

# Define task dependencies
start_pipeline >> start_extraction

start_extraction >> [trigger_process_csv, trigger_extract_api] >> end_extraction

end_extraction >> trigger_dbt_transformation >> end_pipeline 