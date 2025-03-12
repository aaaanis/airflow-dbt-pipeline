"""
Custom operators for monitoring integration in Airflow.
"""
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins.hooks.monitoring_hook import MonitoringHook


class UpdateDataFreshnessOperator(BaseOperator):
    """
    Operator to update data freshness in the monitoring system.
    """
    
    @apply_defaults
    def __init__(self, table_name, healthcheck_conn_id='health_service_default', *args, **kwargs):
        """
        Initialize the operator.
        
        :param table_name: Name of the table to update freshness for
        :param healthcheck_conn_id: Connection ID for the health service
        """
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.healthcheck_conn_id = healthcheck_conn_id
    
    def execute(self, context):
        """
        Execute the operator.
        
        :param context: Airflow context
        :return: Response from update operation
        """
        hook = MonitoringHook(healthcheck_conn_id=self.healthcheck_conn_id)
        self.log.info(f"Updating data freshness for table: {self.table_name}")
        response = hook.update_data_freshness(self.table_name)
        
        if response:
            self.log.info(f"Successfully updated data freshness for {self.table_name}")
        else:
            self.log.warning(f"Failed to update data freshness for {self.table_name}")
        
        return response


class UpdatePipelineStatusOperator(BaseOperator):
    """
    Operator to update pipeline status in the monitoring system.
    """
    
    @apply_defaults
    def __init__(
        self, 
        pipeline_name, 
        status=True, 
        healthcheck_conn_id='health_service_default', 
        *args, 
        **kwargs
    ):
        """
        Initialize the operator.
        
        :param pipeline_name: Name of the pipeline to update status for
        :param status: Status to set (True=success, False=failure)
        :param healthcheck_conn_id: Connection ID for the health service
        """
        super().__init__(*args, **kwargs)
        self.pipeline_name = pipeline_name
        self.status = status
        self.healthcheck_conn_id = healthcheck_conn_id
    
    def execute(self, context):
        """
        Execute the operator.
        
        :param context: Airflow context
        :return: Response from update operation
        """
        hook = MonitoringHook(healthcheck_conn_id=self.healthcheck_conn_id)
        status_str = "success" if self.status else "failure"
        
        self.log.info(f"Setting pipeline status for {self.pipeline_name} to {status_str}")
        response = hook.update_pipeline_status(self.pipeline_name, self.status)
        
        if response:
            self.log.info(f"Successfully updated pipeline status for {self.pipeline_name}")
        else:
            self.log.warning(f"Failed to update pipeline status for {self.pipeline_name}")
        
        return response


class CheckDataFreshnessOperator(BaseOperator):
    """
    Operator to check data freshness and fail if not within threshold.
    """
    
    @apply_defaults
    def __init__(
        self, 
        table_name=None, 
        max_age_seconds=None,
        healthcheck_conn_id='health_service_default', 
        *args, 
        **kwargs
    ):
        """
        Initialize the operator.
        
        :param table_name: Name of the table to check freshness for (None for all)
        :param max_age_seconds: Maximum allowed age in seconds (None to use default)
        :param healthcheck_conn_id: Connection ID for the health service
        """
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.max_age_seconds = max_age_seconds
        self.healthcheck_conn_id = healthcheck_conn_id
    
    def execute(self, context):
        """
        Execute the operator.
        
        :param context: Airflow context
        :return: Freshness data
        """
        hook = MonitoringHook(healthcheck_conn_id=self.healthcheck_conn_id)
        self.log.info(f"Checking data freshness for {self.table_name or 'all tables'}")
        
        freshness_data = hook.check_data_freshness(self.table_name)
        
        if not freshness_data:
            self.log.error(f"Failed to get freshness data for {self.table_name or 'all tables'}")
            raise Exception(f"Failed to get freshness data for {self.table_name or 'all tables'}")
        
        # If checking a specific table
        if self.table_name:
            status = freshness_data.get('status', False)
            value = freshness_data.get('value', 0)
            threshold = freshness_data.get('threshold', 0)
            
            # Override threshold if provided
            if self.max_age_seconds is not None:
                threshold = self.max_age_seconds
                status = value <= threshold
            
            msg = f"Table {self.table_name} last updated {value} seconds ago (threshold: {threshold}s)"
            
            if not status:
                self.log.error(f"Data freshness check failed: {msg}")
                raise Exception(f"Data freshness check failed: {msg}")
            
            self.log.info(f"Data freshness check passed: {msg}")
            return freshness_data
        
        # If checking all tables
        all_checks = freshness_data.get('checks', [])
        failed_checks = [c for c in all_checks if not c.get('status', False)]
        
        if failed_checks:
            fail_msg = ", ".join([
                f"{c.get('name')}: {c.get('value')}s (threshold: {c.get('threshold')}s)" 
                for c in failed_checks
            ])
            self.log.error(f"Data freshness checks failed: {fail_msg}")
            raise Exception(f"Data freshness checks failed: {fail_msg}")
        
        self.log.info(f"All data freshness checks passed ({len(all_checks)} tables)")
        return freshness_data 