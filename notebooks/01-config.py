# Databricks notebook source
# MAGIC %md
# MAGIC Copyright (c) 2024 Dharmik Naik </br>
# MAGIC This source code is licensed under the MIT license found in the </br>
# MAGIC LICENSE file in the root directory of this source tree.

# COMMAND ----------

class DatabaseError(Exception):
    """Base exception for database operations"""
    pass

class DatabaseNotInitializedError(Exception):
    """Raised when attempting operations on uninitialized database"""
    pass


# COMMAND ----------

import logging 
from logging import Logger
def setup_logging() -> Logger:
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)
logger = setup_logging()


# COMMAND ----------

from dataclasses import dataclass
class DatabaseConfig:
    """Database configuration settings"""
    base_dir_landing_zone: str
    base_dir_checkpoint: str
    db_name: str


# COMMAND ----------

class Config:
    """Configuration management for the application"""

    def __init__(self, env: str):
        self.env: str = env
        self.max_files_per_trigger: int = 1000
        self._database_config: DatabaseConfig = DatabaseConfig(
            base_dir_landing_zone = spark.sql('describe external location `data_zone`').select('url').collect()[0][0],
            base_dir_checkpoint = spark.sql('describe external location `checkpoint`').select('url').collect()[0][0],
            db_name = 'medallion_fitness_db'
        )
    
    @property
    def base_dir_landing_zone(self) -> str:
        return self._database_config.base_dir_landing_zone
    
    @property
    def base_dir_checkpoint(self) -> str:
        return self._database_config.base_dir_checkpoint
    
    @property
    def db_name(self) -> str:
        return self._database_config.db_name
    

