"""
Custom sensors for Apache Airflow.
These sensors can be used to detect specific conditions in external systems.
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import os
import time
from typing import Any, Dict, Optional


class FileContentSensor(BaseSensorOperator):
    """
    Sensor that waits for specific content to appear in a file.
    
    This is useful for detecting when a data file has been populated with required data
    or when a process has written an expected output to a log file.
    
    Attributes:
        filepath: Path to the file to check
        content_pattern: String or pattern to look for in the file
        ignore_case: Whether to ignore case when searching for the pattern
    """
    
    @apply_defaults
    def __init__(
        self,
        *,
        filepath: str,
        content_pattern: str,
        ignore_case: bool = True,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.filepath = filepath
        self.content_pattern = content_pattern
        self.ignore_case = ignore_case
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """
        Function executed in each poke interval to check condition.
        
        Args:
            context: Airflow context containing task instance, dag, etc.
            
        Returns:
            True if the condition is met, False otherwise
        """
        self.log.info(f"Poking for content '{self.content_pattern}' in file '{self.filepath}'")
        
        # Check if file exists
        if not os.path.isfile(self.filepath):
            self.log.info(f"File '{self.filepath}' does not exist yet")
            return False
        
        # Read file content
        try:
            with open(self.filepath, 'r') as file:
                content = file.read()
                
            # Search for pattern
            if self.ignore_case:
                found = self.content_pattern.lower() in content.lower()
            else:
                found = self.content_pattern in content
                
            if found:
                self.log.info(f"Content '{self.content_pattern}' found in file '{self.filepath}'")
                return True
            else:
                self.log.info(f"Content '{self.content_pattern}' not found in file '{self.filepath}'")
                return False
                
        except Exception as e:
            self.log.error(f"Error reading file '{self.filepath}': {str(e)}")
            return False


class DataFreshnessTimeSensor(BaseSensorOperator):
    """
    Sensor that checks if data in a file or database is fresh enough.
    
    This is useful for ensuring that data has been updated recently before processing it.
    
    Attributes:
        data_timestamp_callable: Function that returns the timestamp of the data
        max_age_in_seconds: Maximum allowed age of the data in seconds
    """
    
    @apply_defaults
    def __init__(
        self,
        *,
        data_timestamp_callable: callable,
        max_age_in_seconds: int,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.data_timestamp_callable = data_timestamp_callable
        self.max_age_in_seconds = max_age_in_seconds
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """
        Function executed in each poke interval to check condition.
        
        Args:
            context: Airflow context containing task instance, dag, etc.
            
        Returns:
            True if the condition is met, False otherwise
        """
        try:
            # Get data timestamp
            data_timestamp = self.data_timestamp_callable(context)
            current_timestamp = time.time()
            
            # Calculate age
            age_in_seconds = current_timestamp - data_timestamp
            
            self.log.info(f"Data age: {age_in_seconds} seconds, Max allowed: {self.max_age_in_seconds} seconds")
            
            # Check if data is fresh enough
            if age_in_seconds <= self.max_age_in_seconds:
                self.log.info("Data is fresh enough")
                return True
            else:
                self.log.info("Data is too old")
                return False
                
        except Exception as e:
            self.log.error(f"Error checking data freshness: {str(e)}")
            return False 