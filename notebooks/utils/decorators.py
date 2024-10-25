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
        
        try:
            result = func(*args, **kwargs)
            
            end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Completed '{func.__name__}' at {end_timestamp}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in '{func.__name__}': {str(e)}")
            raise
            
    return cast(F, wrapper)

# COMMAND ----------

def measure_performance(func: F) -> F:
    """
    Measures and logs the execution time of a function.
    
    Args:
        func: The function to be decorated
        
    Returns:
        The wrapped function with timing measurement
        
    Example:
        @measure_performance
        def process_data():
            # Some processing
            pass
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:

        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            logger.info(f"Function '{func.__name__}' executed in {execution_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Function '{func.__name__}' failed after {execution_time:.2f} seconds")
            raise
            
    return wrapper
