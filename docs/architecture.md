# Data Engineering Showcase Architecture

## Overview

This document describes the architecture of our data engineering showcase, which integrates Apache Airflow for workflow orchestration and dbt (data build tool) for data transformations.

## Components

The architecture consists of the following main components:

1. **Apache Airflow**: Orchestrates the entire ETL pipeline
2. **PostgreSQL**: Serves as our data warehouse
3. **dbt**: Handles data transformations and modeling

## Data Flow

The data flows through the system as follows:

1. **Data Extraction**:
   - CSV files are processed using Python scripts
   - API data is fetched using HTTP requests

2. **Data Loading**:
   - Raw data is loaded into the data warehouse's raw schema

3. **Data Transformation**:
   - dbt models transform the raw data into:
     - Staging models (cleaned data)
     - Intermediate models (business logic)
     - Mart models (dimension and fact tables)

4. **Data Validation**:
   - dbt tests ensure data quality throughout the pipeline

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Data Engineering Showcase                      │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                               Data Sources                               │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│  CSV Files      │  APIs           │  Databases      │  Other Sources   │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Apache Airflow DAGs                            │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│ extract_api_data│ process_csv_files│dbt_transformation│full_etl_pipeline│
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Data Warehouse (PostgreSQL)                    │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│    Raw Data     │                  │                 │                  │
└─────────────────┘                  │                 │                  │
                                      │                 │                  │
┌─────────────────┐                   │                 │                  │
│     dbt         │                   │                 │                  │
├─────────────────┼─────────────────┬┼─────────────────┼─────────────────┤
│  Staging Models │ Intermediate Models│   Mart Models  │      Tests      │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                             Analytics Layer                              │
├─────────────────┬─────────────────┬─────────────────┬─────────────────┤
│   Dashboards    │    Reports      │  ML Models      │  Data Apps      │
└─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

## Data Model

Our data model consists of:

1. **Staging Layer**:
   - Cleaned and standardized raw data
   - One-to-one mapping with source tables
   - Implemented as views

2. **Intermediate Layer**:
   - Business logic and transformations
   - Joins and aggregations
   - Implemented as views

3. **Mart Layer**:
   - Star schema with fact and dimension tables
   - Analytics-ready data
   - Implemented as tables

## Technologies Used

- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Data warehouse
- **dbt**: Data transformation
- **Python**: Data processing
- **Docker**: Containerization 