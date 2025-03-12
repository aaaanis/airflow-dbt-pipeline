# Using the Data Engineering Showcase

This document provides detailed instructions on how to use the Data Engineering Showcase project, which demonstrates integration between Apache Airflow and dbt for a complete data pipeline.

## Prerequisites

Before you begin, ensure you have the following installed:
- Docker and Docker Compose
- Python 3.9+
- Git

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/data-engineering-showcase.git
cd data-engineering-showcase
```

### 2. Start the Environment

Start the Docker containers:

```bash
docker-compose up -d
```

This command will start:
- PostgreSQL database (for Airflow)
- PostgreSQL data warehouse
- Airflow webserver
- Airflow scheduler

### 3. Access Airflow UI

Open your browser and navigate to [http://localhost:8080](http://localhost:8080)

Default credentials:
- Username: `airflow`
- Password: `airflow`

## Running the Pipeline

### Option 1: Using Airflow UI

1. In the Airflow UI, navigate to the DAGs page
2. Enable the `full_etl_pipeline` DAG
3. Trigger the DAG manually by clicking the "Play" button

### Option 2: Using Command Line

```bash
docker-compose exec airflow-scheduler airflow dags trigger full_etl_pipeline
```

## Pipeline Steps

The full pipeline consists of the following steps:

1. **Extract Data from CSV Files**: Processes CSV files and loads them into the raw schema of the data warehouse
2. **Extract Data from API**: Fetches weather data from API and loads it into the raw schema
3. **Run dbt Transformations**: Transforms raw data into staging, intermediate, and mart models

## Working with dbt

### Run dbt Manually

```bash
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."
```

### Generate dbt Documentation

```bash
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir ."
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs serve --profiles-dir ."
```

Then navigate to [http://localhost:8080](http://localhost:8080) to view the documentation.

## Examining the Data

### Connect to the Data Warehouse

```bash
docker-compose exec warehouse psql -U warehouse -d warehouse
```

### Example Queries

```sql
-- View the raw data
SELECT * FROM raw.customers LIMIT 5;
SELECT * FROM raw.orders LIMIT 5;
SELECT * FROM raw.products LIMIT 5;
SELECT * FROM raw.weather_data LIMIT 5;

-- View the transformed data
SELECT * FROM mart.dim_customers LIMIT 5;
SELECT * FROM mart.dim_products LIMIT 5;
SELECT * FROM mart.dim_weather_locations LIMIT 5;
SELECT * FROM mart.fct_customer_orders LIMIT 5;
```

## Extending the Pipeline

### Adding New Data Sources

1. Create a new CSV file in the `data/` directory or set up a new API endpoint
2. Create a new Airflow DAG in the `airflow/dags/` directory to process the new data source
3. Update the `full_etl_pipeline` DAG to include the new DAG

### Adding New dbt Models

1. Create a new SQL file in one of the dbt model directories:
   - `dbt/models/staging/` for staging models
   - `dbt/models/intermediate/` for intermediate models
   - `dbt/models/mart/` for mart models
2. Update the corresponding schema.yml file
3. Run dbt to test the new model

## Monitoring and Troubleshooting

### Viewing Airflow Logs

```bash
docker-compose logs -f airflow-scheduler
```

### Viewing dbt Logs

```bash
docker-compose exec airflow-scheduler bash -c "cat /opt/airflow/dbt/logs/dbt.log"
```

## Shutting Down

```bash
docker-compose down
```

To remove all data and start fresh:

```bash
docker-compose down -v
``` 