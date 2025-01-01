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

# COMMAND ----------


def retry(
    max_attempts: int = 3,
    delay: int = 10,
    backoff_factor: float = 2,
) -> Callable[[F], F]:
    """
    A decorator that retries a function execution when it fails with an exception.
    Implements exponential backoff between retry attempts.

    Args:
        max_attempts (int, optional): Maximum number of execution attempts.
            Defaults to 3.
        delay (int, optional): Initial delay between retries in seconds.
            Defaults to 10.
        backoff_factor (float, optional): Multiplicative factor applied to
            delay between retries. Defaults to 2.
        logger (logging.Logger, optional): Logger instance for retry attempts
            and failures. If None, logging is disabled. Defaults to None.

    Returns:
        Callable: Decorated function that implements retry logic.

    Raises:
        Exception: The last exception that caused the final retry to fail
            after max_attempts is reached.
            
    Note:
        The delay between retries follows an exponential pattern:
        first retry: delay
        second retry: delay * backoff_factor
        third retry: delay * backoff_factor^2
        and so on.
    """
    def decorator(func: F) -> F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f'Attempt: {attempt+1} of {max_attempts} failed for {func.__name__}: {str(e)}')
                    if attempt == max_attempts - 1:
                        raise
                    logger.warning(f'Retrying {func.__name__} in {current_delay:.2f} seconds')
                    time.sleep(delay)
                    current_delay *= backoff_factor

        return cast(F, wrapper)
    return decorator
