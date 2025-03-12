"""
Utility for reading configuration files.
"""

import json
import os
import logging
from typing import Dict, List, Any, Optional, Union

logger = logging.getLogger(__name__)

def read_config_file(config_path: str) -> Dict[str, Any]:
    """
    Read a JSON configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        The configuration as a dictionary
        
    Raises:
        FileNotFoundError: If the configuration file does not exist
        json.JSONDecodeError: If the configuration file is not valid JSON
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Successfully loaded configuration from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file {config_path}: {e}")
        raise

def get_api_config(config: Dict[str, Any], api_name: str) -> Optional[Dict[str, Any]]:
    """
    Get configuration for a specific API.
    
    Args:
        config: Configuration dictionary
        api_name: Name of the API
        
    Returns:
        API configuration or None if not found
    """
    api_sources = config.get('api_sources', [])
    for api in api_sources:
        if api.get('name') == api_name:
            return api
    
    logger.warning(f"API configuration not found for {api_name}")
    return None

def get_file_config(config: Dict[str, Any], file_name: str) -> Optional[Dict[str, Any]]:
    """
    Get configuration for a specific file source.
    
    Args:
        config: Configuration dictionary
        file_name: Name of the file source
        
    Returns:
        File configuration or None if not found
    """
    file_sources = config.get('file_sources', [])
    for file_source in file_sources:
        if file_source.get('name') == file_name:
            return file_source
    
    logger.warning(f"File configuration not found for {file_name}")
    return None

def get_database_config(config: Dict[str, Any], db_name: str) -> Optional[Dict[str, Any]]:
    """
    Get configuration for a specific database.
    
    Args:
        config: Configuration dictionary
        db_name: Name of the database
        
    Returns:
        Database configuration or None if not found
    """
    db_sources = config.get('database_sources', [])
    for db in db_sources:
        if db.get('name') == db_name:
            # Resolve environment variables for sensitive information
            if 'user_env_var' in db:
                db['user'] = os.environ.get(db['user_env_var'], 'warehouse')  # Default value as fallback
                del db['user_env_var']
            
            if 'password_env_var' in db:
                db['password'] = os.environ.get(db['password_env_var'], 'warehouse')  # Default value as fallback
                del db['password_env_var']
                
            return db
    
    logger.warning(f"Database configuration not found for {db_name}")
    return None

def get_data_source_config(
    source_type: str, 
    source_name: str, 
    config_path: str = '/opt/airflow/config/data_sources.json'
) -> Optional[Dict[str, Any]]:
    """
    Get configuration for a specific data source.
    
    Args:
        source_type: Type of data source ('api', 'file', or 'database')
        source_name: Name of the data source
        config_path: Path to the configuration file
        
    Returns:
        Data source configuration or None if not found
    """
    try:
        config = read_config_file(config_path)
        
        if source_type == 'api':
            return get_api_config(config, source_name)
        elif source_type == 'file':
            return get_file_config(config, source_name)
        elif source_type == 'database':
            return get_database_config(config, source_name)
        else:
            logger.warning(f"Unknown source type: {source_type}")
            return None
    
    except Exception as e:
        logger.error(f"Error getting data source configuration: {e}")
        return None 