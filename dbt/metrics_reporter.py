#!/usr/bin/env python3
"""
DBT Metrics Reporter
Collects and reports dbt run metrics to Prometheus via Pushgateway
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime
from prometheus_client import Gauge, push_to_gateway


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('dbt_metrics_reporter')

# Define Prometheus metrics
DBT_MODEL_RUN_TIME = Gauge(
    'dbt_model_run_time',
    'Time taken to run a DBT model in seconds',
    ['model_name']
)

DBT_MODEL_ROWS_AFFECTED = Gauge(
    'dbt_model_rows_affected',
    'Number of rows affected by a DBT model run',
    ['model_name']
)

DBT_MODEL_LAST_UPDATED = Gauge(
    'dbt_model_last_updated',
    'Timestamp when a DBT model was last updated',
    ['model_name']
)

DBT_MODEL_FAILURES = Gauge(
    'dbt_model_failures',
    'Count of DBT model run failures',
    ['model_name']
)


def parse_dbt_run_results(run_results_path):
    """
    Parse dbt run results from the run_results.json file
    """
    logger.info(f"Parsing DBT run results from {run_results_path}")
    
    try:
        with open(run_results_path, 'r') as f:
            results = json.load(f)
        
        return results
    except Exception as e:
        logger.error(f"Error parsing DBT run results: {str(e)}")
        return None


def report_metrics(results, pushgateway_url):
    """
    Report metrics to Prometheus Pushgateway
    """
    if not results:
        logger.error("No results to report")
        return
    
    try:
        run_results = results.get('results', [])
        logger.info(f"Processing {len(run_results)} model results")
        
        for result in run_results:
            # Extract model information
            model_name = result.get('unique_id', '').replace('model.', '')
            status = result.get('status')
            timing = result.get('execution_time', 0)
            
            # Set model run time
            DBT_MODEL_RUN_TIME.labels(model_name=model_name).set(timing)
            
            # Set model last updated time
            DBT_MODEL_LAST_UPDATED.labels(model_name=model_name).set(time.time())
            
            # Set model failure count
            if status == 'error':
                DBT_MODEL_FAILURES.labels(model_name=model_name).inc()
            
            # Try to extract rows affected from the adapter response
            adapter_response = result.get('adapter_response', {})
            rows_affected = adapter_response.get('rows_affected', 0)
            if rows_affected:
                DBT_MODEL_ROWS_AFFECTED.labels(model_name=model_name).set(rows_affected)
        
        # Push metrics to Pushgateway
        push_to_gateway(pushgateway_url, job='dbt_metrics', registry=None)
        logger.info(f"Successfully pushed metrics to {pushgateway_url}")
    
    except Exception as e:
        logger.error(f"Error reporting metrics: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='Report DBT metrics to Prometheus')
    parser.add_argument('--run-results', required=True,
                        help='Path to dbt run_results.json file')
    parser.add_argument('--pushgateway', default='localhost:9091',
                        help='Prometheus Pushgateway URL (default: localhost:9091)')
    
    args = parser.parse_args()
    
    results = parse_dbt_run_results(args.run_results)
    if results:
        report_metrics(results, args.pushgateway)


if __name__ == '__main__':
    main() 