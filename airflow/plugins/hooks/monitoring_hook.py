"""
Monitoring hook for Airflow to send metrics to our monitoring system.
"""
import json
import logging
import requests
from airflow.hooks.base import BaseHook


class MonitoringHook(BaseHook):
    """
    Hook for sending metrics to the monitoring system.
    
    This hook can:
    1. Update data freshness timestamps
    2. Update pipeline status
    3. Send metrics to Prometheus Pushgateway
    """
    
    def __init__(self, healthcheck_conn_id='health_service_default'):
        """
        Initialize the hook with connection id.
        
        :param healthcheck_conn_id: Airflow connection ID for the health service
        """
        super().__init__()
        self.healthcheck_conn_id = healthcheck_conn_id
        self.base_url = None
        self._get_conn_params()
    
    def _get_conn_params(self):
        """
        Get connection parameters for the health service.
        """
        try:
            conn = self.get_connection(self.healthcheck_conn_id)
            self.base_url = f"http://{conn.host}:{conn.port}"
        except Exception as e:
            # Fallback to default if connection doesn't exist
            logging.warning(f"Could not find connection '{self.healthcheck_conn_id}', using default: {str(e)}")
            self.base_url = "http://health-service:8080"
    
    def update_data_freshness(self, table_name):
        """
        Update the freshness timestamp for a table.
        
        :param table_name: Name of the table to update
        :return: Response from the health service
        """
        endpoint = f"{self.base_url}/update/freshness"
        data = {"table_name": table_name}
        
        try:
            response = requests.post(
                endpoint,
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            if response.status_code != 200:
                logging.error(f"Failed to update data freshness for {table_name}: {response.text}")
            else:
                logging.info(f"Successfully updated freshness for {table_name}")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logging.error(f"Error updating data freshness: {str(e)}")
            return None
    
    def update_pipeline_status(self, pipeline_name, status=True):
        """
        Update the status of a pipeline.
        
        :param pipeline_name: Name of the pipeline to update
        :param status: True for success, False for failure
        :return: Response from the health service
        """
        endpoint = f"{self.base_url}/update/pipeline"
        data = {
            "pipeline_name": pipeline_name,
            "status": status
        }
        
        try:
            response = requests.post(
                endpoint,
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            if response.status_code != 200:
                logging.error(f"Failed to update pipeline status for {pipeline_name}: {response.text}")
            else:
                logging.info(f"Successfully updated status for {pipeline_name} to {status}")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logging.error(f"Error updating pipeline status: {str(e)}")
            return None
    
    def check_health(self):
        """
        Check the overall health of the system.
        
        :return: Health status response or None on error
        """
        endpoint = f"{self.base_url}/health"
        
        try:
            response = requests.get(endpoint, timeout=5)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logging.error(f"Error checking health: {str(e)}")
            return None
    
    def check_data_freshness(self, table_name=None):
        """
        Check freshness status for all tables or a specific table.
        
        :param table_name: Optional, specific table to check
        :return: Freshness data or None on error
        """
        endpoint = f"{self.base_url}/health/freshness"
        
        try:
            response = requests.get(endpoint, timeout=5)
            if response.status_code != 200:
                logging.error(f"Failed to check data freshness: {response.text}")
                return None
            
            data = response.json()
            if table_name:
                # Filter for specific table if provided
                for check in data.get('checks', []):
                    if check.get('name') == f"freshness_{table_name}":
                        return check
                return None
            return data
        except Exception as e:
            logging.error(f"Error checking data freshness: {str(e)}")
            return None 