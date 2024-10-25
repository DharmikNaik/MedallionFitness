# Databricks notebook source
# MAGIC %md
# MAGIC Copyright (c) 2024 Dharmik Naik </br>
# MAGIC This source code is licensed under the MIT license found in the </br>
# MAGIC LICENSE file in the root directory of this source tree.

# COMMAND ----------

# MAGIC %run ../utils/decorators

# COMMAND ----------

from dataclasses import dataclass
from typing import Dict, List, Optional
import time

# COMMAND ----------

@dataclass
class TableSchema:
    """Represents a table schema with its creation SQL"""
    name: str
    sql: str
    is_view: bool = False
    partitioned_by: Optional[List[str]] = None

# COMMAND ----------


class TableDefinitions:
    """
    Central repository for table definitions
    """

    def __init__(self, config: Config):
        self.config = config
        self.catalog = self.config.database.catalog
        self.schema = self.config.database.schema
        
        self._bronze_table_definitions = self._init_bronze_tables()
        self._silver_table_definitions = self._init_silver_tables()
        self._gold_table_definitions = self._init_gold_tables()

    def _get_table_path(self, table_name: str) -> str:
        """Generate fully qualified table path"""
        return f"{self.catalog}.{self.schema}.{table_name}"
    
    def _init_bronze_tables(self) -> Dict[str, TableSchema]:
        """Returns bronze layer table definitions"""
        table_prefix = self.config.database.bronze_prefix
        
        return {
            f"{table_prefix}registered_users": TableSchema(
                name=f"{table_prefix}registered_users",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path(table_prefix + 'registered_users')}(
                        user_id LONG,
                        device_id LONG,
                        mac_address STRING,
                        registration_timestamp DOUBLE,
                        load_time TIMESTAMP,
                        source_file STRING
                    )
                """
            ),
            f"{table_prefix}gym_logins": TableSchema(
                name=f"{table_prefix}gym_logins",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path(table_prefix + 'gym_logins')}(
                        mac_address STRING,
                        gym BIGINT,
                        login DOUBLE,
                        logout DOUBLE,
                        load_time TIMESTAMP,
                        source_file STRING
                    )
                """
            ),
            f"{table_prefix}kafka_multiplex": TableSchema(
                name=f"{table_prefix}kafka_multiplex",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path(table_prefix + 'kafka_multiplex')}(
                        key STRING,
                        value STRING,
                        topic STRING,
                        partition BIGINT,
                        offset BIGINT,
                        timestamp BIGINT,
                        date DATE,
                        week_part STRING,
                        load_time TIMESTAMP,
                        source_file STRING
                    )
                    PARTITIONED BY (topic, week_part)
                """,
                partitioned_by=["topic", "week_part"]
            )
        }

    def _init_silver_tables(self) -> Dict[str, TableSchema]:
        """Returns silver layer table definitions"""
        return {
            "users": TableSchema(
                name="users",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('users')}(
                        user_id BIGINT,
                        device_id BIGINT,
                        mac_address STRING,
                        registration_timestamp TIMESTAMP
                    )
                """
            ),
            "gym_logs": TableSchema(
                name="gym_logs",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('gym_logs')}(
                        mac_address STRING,
                        gym BIGINT,
                        login TIMESTAMP,
                        logout TIMESTAMP
                    )
                """
            ),
            "user_profile": TableSchema(
                name="user_profile",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('user_profile')}(
                        user_id BIGINT,
                        dob DATE,
                        sex STRING,
                        gender STRING,
                        first_name STRING,
                        last_name STRING,
                        street_address STRING,
                        city STRING,
                        state STRING,
                        zip INT,
                        updated TIMESTAMP
                    )
                """
            ),
            "heart_rate": TableSchema(
                name="heart_rate",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('heart_rate')}(
                        device_id LONG,
                        time TIMESTAMP,
                        heartrate DOUBLE,
                        valid BOOLEAN
                    )
                """
            ),
            "workouts": TableSchema(
                name="workouts",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('workouts')}(
                        user_id INT,
                        workout_id INT,
                        time TIMESTAMP,
                        action STRING,
                        session_id INT
                    )
                """
            ),
            "completed_workouts": TableSchema(
                name="completed_workouts",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('completed_workouts')}(
                        user_id INT,
                        workout_id INT,
                        session_id INT,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP
                    )
                """
            ),
            "workout_bpm": TableSchema(
                name="workout_bpm",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('workout_bpm')}(
                        user_id INT,
                        workout_id INT,
                        session_id INT,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        time TIMESTAMP,
                        heartrate DOUBLE
                    )
                """
            ),
            "user_bins": TableSchema(
                name="user_bins",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('user_bins')}(
                        user_id BIGINT,
                        age STRING,
                        gender STRING,
                        city STRING,
                        state STRING
                    )
                """
            ),
            "date_lookup": TableSchema(
                name="date_lookup",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('date_lookup')}(
                        date DATE,
                        week INT,
                        year INT,
                        month INT,
                        dayofweek INT,
                        dayofmonth INT,
                        dayofyear INT,
                        week_part STRING
                    )
                """
            )
        }

    def _init_gold_tables(self) -> Dict[str, TableSchema]:
        """Returns gold layer table definitions"""
        return {
            "workout_bpm_summary": TableSchema(
                name="workout_bpm_summary",
                sql=f"""
                    CREATE TABLE IF NOT EXISTS {self._get_table_path('workout_bpm_summary')}(
                        workout_id INT,
                        session_id INT,
                        user_id BIGINT,
                        age STRING,
                        gender STRING,
                        city STRING,
                        state STRING,
                        min_bpm DOUBLE,
                        avg_bpm DOUBLE,
                        max_bpm DOUBLE,
                        num_recordings BIGINT
                    )
                """
            ),
            "gym_summary": TableSchema(
                name="gym_summary",
                sql=f"""
                    CREATE OR REPLACE VIEW {self._get_table_path('gym_summary')} AS
                    SELECT 
                        to_date(login::timestamp) date,
                        gym,
                        l.mac_address,
                        workout_id,
                        session_id,
                        round((logout::long - login::long)/60,2) minutes_in_gym,
                        round((end_time::long - start_time::long)/60,2) minutes_exercising
                    FROM {self._get_table_path('gym_logs')} l
                    JOIN (
                        SELECT mac_address, workout_id, session_id, start_time, end_time
                        FROM {self._get_table_path('completed_workouts')} w
                        INNER JOIN {self._get_table_path('users')} u ON w.user_id = u.user_id
                    ) w
                    ON l.mac_address = w.mac_address
                    AND w.start_time BETWEEN l.login AND l.logout
                    ORDER BY date, gym, l.mac_address, session_id
                """,
                is_view=True
            )
        }

    @log_execution
    def get_bronze_tables(self) -> Dict[str, TableSchema]:
        return self._bronze_table_definitions

    @log_execution
    def get_silver_tables(self) -> Dict[str, TableSchema]:
        return self._silver_table_definitions
    
    @log_execution
    def get_gold_tables(self) -> Dict[str, TableSchema]:
        return self._gold_table_definitions

# COMMAND ----------

class SetupManager:
    """
    Manages the setup and initialization of the project
    """
    def __init__(self, env: str):
        self.config = Config(env)
        self.initialized = False

        # Set up paths
        self.landing_zone = f"{self.config.storage.landing_zone_path}/raw"
        self.checkpoint_base = f"{self.config.storage.checkpoint_path}/checkpoints"

        # Initialize table definitions
        self.table_definitions = TableDefinitions(self.config)

    @measure_performance
    @log_execution
    def initialize(self) -> None:
        try:
            self._create_database()
            self._create_bronze_tables()
            self._create_silver_tables()
            self._create_gold_tables()
            self.initialized = True
        except InitializationException as e:
            self.config.error(f'Failed to initialize the env: {str(e)}')
            raise
    
    @measure_performance
    @log_execution
    def _create_database(self):
        spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.config.database.catalog}.{self.config.database.schema}')
        spark.sql(f'USE {self.config.database.catalog}.{self.config.database.schema}')

    @log_execution
    def _create_tables(self, tables: Dict[str, TableDefinitions]) -> None:
        """
        Creates table(s)/view(s) from schema definitions

        Args:
            tables: Dict of table/view schemas

        """
        for table in tables.values():
            try:
                self.config.logger.info(f'Creating {"view" if table.is_view else "table"} {table.name}')
                spark.sql(table.sql)
            except DatabaseObjectCreationException as e:
                self.config.error(f'Failed to create {"view" if table.is_view else "table"} {table.name}: {str(e)}')
                raise

    @measure_performance
    @log_execution
    def _create_bronze_tables(self) -> None:
        bronze_layer_definitions = self.table_definitions.get_bronze_tables()
        self._create_tables(bronze_layer_definitions)

    @measure_performance
    @log_execution
    def _create_silver_tables(self) -> None:
        silver_layer_definitions = self.table_definitions.get_silver_tables()
        self._create_tables(silver_layer_definitions)

    @measure_performance
    @log_execution
    def _create_gold_tables(self) -> None:
        gold_layer_definitions = self.table_definitions.get_gold_tables()
        self._create_tables(gold_layer_definitions)

    @measure_performance
    @log_execution
    def validate(self) -> None:
        try:
            db_exists = spark.sql(f'SHOW DATABASES IN {self.config.database.catalog}') \
                            .filter(f'databaseName == "{self.config.database.schema}"') \
                            .count() == 1
            assert db_exists, f'Database {self.config.database.catalog}.{self.config.database.schema} not found'

            all_tables = {
                            **self.table_definitions.get_bronze_tables(),
                            **self.table_definitions.get_silver_tables(),
                            **self.table_definitions.get_gold_tables()    
                        }

            for table in all_tables.values():
                table_exists = spark.sql(f'SHOW TABLES IN {self.config.database.catalog}.{self.config.database.schema}')\
                                .filter(f'isTemporary == false and tableName == "{table.name}"')\
                                .count() == 1
                assert table_exists, f'Table {table.name} not found in {self.config.database.catalog}.{self.config.database.schema}'
        except SetupValidationException as e:
            config.error(f'Setup validation failed: {str(e)}')
            raise
            
    @measure_performance
    @log_execution
    def cleanup(self) -> None:
        """Clean up all resources"""
        try:
            # Drop database if exists
            if spark.sql(f"SHOW DATABASES IN {self.config.database.catalog}") \
                   .filter(f"databaseName == '{self.config.database.schema}'").count() == 1:
                self.config.logger.info(f"Dropping database {self.config.database.catalog}.{self.config.database.schema}")
                spark.sql(f"DROP DATABASE {self.config.database.catalog}.{self.config.database.schema} CASCADE")
            
            # Clean up storage locations
            self.config.logger.info("Cleaning up storage locations")
            dbutils.fs.rm(self.landing_zone, True)
            dbutils.fs.rm(self.checkpoint_base, True)
            
        except EnvCleanupException as e:
            self.config.logger.error(f"Cleanup failed: {str(e)}")
            raise


