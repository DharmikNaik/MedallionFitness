# Databricks notebook source
# MAGIC %md
# MAGIC Copyright (c) 2024 Dharmik Naik </br>
# MAGIC This source code is licensed under the MIT license found in the </br>
# MAGIC LICENSE file in the root directory of this source tree.

# COMMAND ----------

# MAGIC %run ../utils/exceptions

# COMMAND ----------

from dataclasses import dataclass
from enum import Enum
import logging
from typing import Optional

# COMMAND ----------

class Environment(Enum):
    """
     Support environments for the pipeline
    """
    DEV = "dev"
    PROD = "prod"


# COMMAND ----------

@dataclass(frozen=True)
class StorageConfig:
    """
    Configuration for storage locations
    """
    landing_zone_path: str
    checkpoint_path: str
    lookup_files_path: str

@dataclass(frozen=True)
class DatabaseConfig:
    """
    Configuration for database and table naming
    """
    catalog: str
    schema: str
    bronze_prefix: str = "bronze_"
    silver_prefix: str = "silver_"
    gold_prefix: str = "gold_"

@dataclass(frozen=True)
class SparkConfig:
    """
    Configuration for Spark settings
    """
    app_name: str
    max_files_per_trigger: int = 1000
    checkpoint_cleanup_enabled: bool = True

    

# COMMAND ----------

class Config:
    """
    Configuration management for the Medallion Fitness 
    """

    def __init__(self, env: Optional[str] = None):
        """Initialize configuration for specified environment"""
        self.env = Environment(env.lower() if env else "dev")
        self.logger = self._setup_logging()
        
        # Load configurations
        self._storage = self._init_storage_config()
        self._database = self._init_database_config()
        self._spark = self._init_spark_config()
        
        self.logger.info(f"Configuration initialized for environment: {self.env.value}")

    def _setup_logging(self) -> logging.Logger:
        """
        Configure logging with standardized format
        """
        logging.basicConfig(
            level=logging.INFO if self.env != Environment.PROD else logging.WARNING,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(self.__class__.__name__)
    
    def _init_storage_config(self) -> StorageConfig:
        """
        Initialize storage configuration from external locations
        """
        try:
            landing_zone_path = spark.sql('DESCRIBE EXTERNAL LOCATION `data_zone`').select('url').collect()[0][0]
            checkpoint_path = spark.sql('DESCRIBE EXTERNAL LOCATION `checkpoint`').select('url').collect()[0][0]
            
            return StorageConfig(
                landing_zone_path=landing_zone_path,
                checkpoint_path=checkpoint_path,
                lookup_files_path=f"{landing_zone_path}/raw/lookups"
            )
        except Exception as e:
            raise StorageLocationError(f"Failed to initialize storage configuration: {str(e)}")

    def _init_database_config(self) -> DatabaseConfig:
        """
        Initialize database configuration
        """
        return DatabaseConfig(
            catalog=self.env.value,
            schema="medallion_fitness_db"
        )
    
    def _init_spark_config(self) -> SparkConfig:
        """
        Initialize Spark configuration
        """
        return SparkConfig(
            app_name=f"medallion_fitness_{self.env.value}",
            max_files_per_trigger=1000,
            checkpoint_cleanup_enabled=self.env != Environment.PROD
        )

    @property
    def storage(self) -> StorageConfig:
        """Get storage configuration"""
        return self._storage
    
    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration"""
        return self._database
    
    @property
    def spark(self) -> SparkConfig:
        """Get Spark configuration"""
        return self._spark
    
    def apply_spark_configs(self) -> None:
        """
        Apply Spark configurations to current session
        """
        configs = {
            "spark.app.name": self.spark.app_name,
            "spark.sql.streaming.fileScan.maxFilesPerTrigger": str(self.spark.max_files_per_trigger),
            "spark.databricks.streaming.statefulOperator.checkpointCleanup.enabled": 
                str(self.spark.checkpoint_cleanup_enabled).lower()
        }
        
        for key, value in configs.items():
            spark.conf.set(key, value)
            
        self.logger.info("Applied Spark configurations")

