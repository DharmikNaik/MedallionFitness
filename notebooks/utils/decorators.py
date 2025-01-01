# Databricks notebook source
# MAGIC %run ../config/config

# COMMAND ----------

import time
import functools
from typing import Any, Callable, TypeVar, cast
from datetime import datetime
import logging

F = TypeVar('F', bound=Callable[..., Any])

# COMMAND ----------

config = Config()
logger = config.logger

# COMMAND ----------

def log_execution(func: F) -> F:
    """
    Logs the execution of a function with start and end times.
    
    Args:
        func: The function to be decorated
        
    Returns:
        The wrapped function with execution logging
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Starting '{func.__name__}' at {timestamp}")
        execution_start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_duration_secs = time.time() - execution_start_time
            end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Completed '{func.__name__}' at {end_timestamp} in {execution_duration_secs:.2f} seconds")
            
            return result
            
        except Exception as e:
            execution_duration_secs = time.time() - execution_start_time
            logger.error(f"Error in '{func.__name__}': {str(e)}, failed after executing for {execution_duration_secs:.2f} seconds")
            raise
            
    return cast(F, wrapper)
