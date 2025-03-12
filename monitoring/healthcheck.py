#!/usr/bin/env python3
"""
Health Check Service for Airflow & dbt pipeline
Provides endpoints for monitoring data freshness and pipeline status
"""

import os
import json
import time
import logging
import datetime
from flask import Flask, jsonify, request
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('healthcheck')

# Initialize Flask app
app = Flask(__name__)

# Define Prometheus metrics
HEALTH_CHECK_STATUS = Gauge(
    'healthcheck_status',
    'Status of health check (1=healthy, 0=unhealthy)',
    ['check_name']
)

DATA_FRESHNESS = Gauge(
    'data_freshness_seconds',
    'Data freshness in seconds',
    ['table_name']
)

PIPELINE_STATUS = Gauge(
    'pipeline_status',
    'Pipeline status (1=success, 0=failure)',
    ['pipeline_name']
)

# Configuration
FRESHNESS_THRESHOLDS = {
    'dim_customers': 86400,    # 1 day
    'dim_products': 86400,     # 1 day
    'fact_orders': 3600,       # 1 hour
    'dim_weather_locations': 86400  # 1 day
}

PIPELINES = [
    'customer_pipeline',
    'product_pipeline',
    'order_pipeline',
    'weather_pipeline'
]

# Database state tracking
# In production environments, this would query actual database timestamps
LAST_UPDATE_TIMES = {}
PIPELINE_STATUSES = {}


@app.route('/metrics', methods=['GET'])
def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


@app.route('/health', methods=['GET'])
def health():
    """Overall health check endpoint"""
    try:
        # Check data freshness
        freshness_checks = check_data_freshness()
        
        # Check pipeline status
        pipeline_checks = check_pipeline_status()
        
        # Combine results
        all_checks = freshness_checks + pipeline_checks
        overall_status = all(check["status"] for check in all_checks)
        
        # Update Prometheus metric
        HEALTH_CHECK_STATUS.labels(check_name="overall").set(1 if overall_status else 0)
        
        response = {
            "status": "healthy" if overall_status else "unhealthy",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "checks": all_checks
        }
        
        status_code = 200 if overall_status else 503
        return jsonify(response), status_code
    
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        HEALTH_CHECK_STATUS.labels(check_name="overall").set(0)
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }), 500


@app.route('/health/freshness', methods=['GET'])
def freshness():
    """Data freshness health check endpoint"""
    checks = check_data_freshness()
    overall_status = all(check["status"] for check in checks)
    
    response = {
        "status": "healthy" if overall_status else "unhealthy",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "checks": checks
    }
    
    status_code = 200 if overall_status else 503
    return jsonify(response), status_code


@app.route('/health/pipelines', methods=['GET'])
def pipelines():
    """Pipeline status health check endpoint"""
    checks = check_pipeline_status()
    overall_status = all(check["status"] for check in checks)
    
    response = {
        "status": "healthy" if overall_status else "unhealthy",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "checks": checks
    }
    
    status_code = 200 if overall_status else 503
    return jsonify(response), status_code


@app.route('/update/freshness', methods=['POST'])
def update_freshness():
    """Update table freshness timestamp"""
    data = request.json
    table_name = data.get('table_name')
    
    if not table_name:
        return jsonify({"error": "Missing table_name parameter"}), 400
    
    LAST_UPDATE_TIMES[table_name] = time.time()
    DATA_FRESHNESS.labels(table_name=table_name).set(0)
    
    return jsonify({
        "status": "success",
        "table_name": table_name,
        "update_time": datetime.datetime.utcnow().isoformat()
    })


@app.route('/update/pipeline', methods=['POST'])
def update_pipeline():
    """Update pipeline status"""
    data = request.json
    pipeline_name = data.get('pipeline_name')
    status = data.get('status', True)
    
    if not pipeline_name:
        return jsonify({"error": "Missing pipeline_name parameter"}), 400
    
    PIPELINE_STATUSES[pipeline_name] = bool(status)
    PIPELINE_STATUS.labels(pipeline_name=pipeline_name).set(1 if status else 0)
    
    return jsonify({
        "status": "success",
        "pipeline_name": pipeline_name,
        "pipeline_status": PIPELINE_STATUSES[pipeline_name],
        "update_time": datetime.datetime.utcnow().isoformat()
    })


def check_data_freshness():
    """Check freshness of all tables"""
    now = time.time()
    results = []
    
    for table_name, threshold in FRESHNESS_THRESHOLDS.items():
        last_update = LAST_UPDATE_TIMES.get(table_name, 0)
        freshness = now - last_update
        DATA_FRESHNESS.labels(table_name=table_name).set(freshness)
        
        status = freshness <= threshold
        HEALTH_CHECK_STATUS.labels(check_name=f"freshness_{table_name}").set(1 if status else 0)
        
        results.append({
            "name": f"freshness_{table_name}",
            "status": status,
            "message": f"Table {table_name} last updated {int(freshness)} seconds ago",
            "threshold": threshold,
            "value": int(freshness)
        })
    
    return results


def check_pipeline_status():
    """Check status of all pipelines"""
    results = []
    
    for pipeline_name in PIPELINES:
        status = PIPELINE_STATUSES.get(pipeline_name, True)  # Default to healthy initially
        PIPELINE_STATUS.labels(pipeline_name=pipeline_name).set(1 if status else 0)
        HEALTH_CHECK_STATUS.labels(check_name=f"pipeline_{pipeline_name}").set(1 if status else 0)
        
        results.append({
            "name": f"pipeline_{pipeline_name}",
            "status": status,
            "message": f"Pipeline {pipeline_name} is {'healthy' if status else 'unhealthy'}"
        })
    
    return results


# Initialize the service state
def initialize_service_state():
    """Initialize service state for first deployment"""
    now = time.time()
    
    # Set all tables with initial timestamps
    for table_name in FRESHNESS_THRESHOLDS.keys():
        LAST_UPDATE_TIMES[table_name] = now
        DATA_FRESHNESS.labels(table_name=table_name).set(0)
    
    # Set all pipelines as healthy initially
    for pipeline_name in PIPELINES:
        PIPELINE_STATUSES[pipeline_name] = True
        PIPELINE_STATUS.labels(pipeline_name=pipeline_name).set(1)
    
    logger.info("Service state initialized")


if __name__ == '__main__':
    initialize_service_state()
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port) 