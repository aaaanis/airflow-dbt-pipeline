# Data Engineering: Airflow & dbt Integration

> **Production-ready data engineering pipeline combining Apache Airflow and dbt**
>
> **Zero-to-production data platform with best practices for real-world scenarios**
>
> **Cross-database compatibility for PostgreSQL, Snowflake, and BigQuery**

This repository demonstrates a production-ready data engineering pipeline that integrates Apache Airflow for workflow orchestration and dbt (data build tool) for data transformations.

## Overview

This project showcases an end-to-end data pipeline that:

1. Extracts data from multiple sources (APIs, CSV files, and databases)
2. Orchestrates the ETL workflow with Apache Airflow
3. Performs data transformations using dbt
4. Loads the transformed data into a data warehouse
5. Includes data quality checks throughout the pipeline
6. Provides comprehensive monitoring and alerting

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)

The pipeline follows this workflow:
- **Extraction**: Airflow DAGs extract data from various sources
- **Loading**: Raw data is loaded into the data warehouse
- **Transformation**: dbt models transform the raw data into analytics-ready tables
- **Validation**: Tests are performed to ensure data quality and integrity
- **Monitoring**: Prometheus and Grafana track pipeline health and performance
- **Visualization**: Transformed data is ready for analytics and dashboarding

## Project Structure

```
.
├── airflow/               # Airflow configuration and DAGs
│   ├── dags/              # DAG definitions
│   ├── plugins/           # Custom plugins
│   └── config/            # Airflow configuration files
├── dbt/                   # dbt project for data transformations
│   ├── models/            # dbt models
│   ├── tests/             # dbt tests
│   └── macros/            # dbt macros
├── monitoring/            # Monitoring configuration
│   ├── prometheus.yml     # Prometheus configuration
│   ├── alert_rules.yml    # Alerting rules
│   ├── grafana/           # Grafana dashboards and provisioning
│   └── healthcheck.py     # Health check service
├── data/                  # Sample data files
├── scripts/               # Utility scripts
├── docs/                  # Documentation
└── docker/                # Docker configuration
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Access to a data warehouse (Snowflake, BigQuery, etc.)

### Installation

1. Clone this repository:
```bash
git clone https://github.com/aaaanis/data-engineering-airflow-dbt.git
cd data-engineering-airflow-dbt
```

2. Start the environment:
```bash
docker-compose up -d
```

3. Access Airflow UI at http://localhost:8080
4. Access Grafana dashboard at http://localhost:3000 (default credentials: admin/admin)
5. Access Prometheus at http://localhost:9090

### Usage

- Airflow DAGs will run on the configured schedule
- dbt models can be run via Airflow or manually
- Monitoring dashboards display real-time pipeline health

## Features

- **Modular Design**: Each component is modular and can be extended
- **Idempotent Pipelines**: All processes can be safely rerun
- **Error Handling**: Comprehensive error handling and notifications
- **Monitoring**: Full observability with Prometheus and Grafana
- **Alerting**: Configurable alerts for pipeline failures and data quality issues
- **Health Checks**: Production-ready API endpoints for service health and data freshness
- **Testing**: Automated tests for data quality and pipeline integrity
- **Documentation**: Auto-generated documentation for data lineage

## Airflow DAGs

The project includes several production DAGs:
- `extract_api_data`: Extracts data from various APIs
- `process_csv_files`: Processes CSV files from a specified location
- `dbt_transformation`: Triggers dbt models for data transformation
- `full_etl_pipeline`: Orchestrates the entire ETL process
- `monitoring_pipeline`: Manages monitoring metrics and alerts

## dbt Models

The dbt project includes:
- Staging models for initial data processing
- Intermediate models for business logic
- Dimension and fact models for analytics
- Testing and documentation

## Monitoring

The production monitoring stack includes:

### Prometheus
- Metrics collection from all components
- Alerting rules for pipeline failures
- Integration with StatsD for Airflow metrics

### Grafana
- Production dashboards for Airflow performance
- dbt transformation metrics dashboard
- Data freshness monitoring dashboard

### Health Service
- RESTful API endpoints for service health
- Data freshness checks with configurable thresholds
- Pipeline status monitoring
- Metrics collection

### Alerts
- Email notifications for critical issues
- Configurable alert thresholds
- Integration with notification services
- Proactive detection of data quality issues

## License

This project is licensed under the MIT License - see the LICENSE file for details. 