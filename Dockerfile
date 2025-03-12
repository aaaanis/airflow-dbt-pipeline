FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy dbt project
COPY dbt/ ${AIRFLOW_HOME}/dbt/

# Set working directory
WORKDIR ${AIRFLOW_HOME}

# Set environment variables
ENV PYTHONPATH=${AIRFLOW_HOME} 