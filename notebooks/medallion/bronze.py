# Databricks notebook source
# MAGIC %run ../utils/decorators

# COMMAND ----------

from dataclasses import dataclass
from typing import Optional, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from enum import Enum
from abc import ABC, abstractmethod

# COMMAND ----------

class SourceFormat(Enum):
    """Supported source data formats"""
    CSV = 'csv'
    JSON = 'json'

@dataclass
class SourceConfig:
    """Configuration for a source dataset"""
    name: str
    format: SourceFormat
    schema: str
    path: str
    partition_cols: Optional[List[str]] = None
    options: Optional[dict] = None

# COMMAND ----------

class SourceTransformer(ABC):
    """
    Abstract base class for source-specific transformations
    """
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

class DefaultTransformer(SourceTransformer):
    """
    Default transformer that returns data as-is
    """
    def transform(self, df: DataFrame) -> DataFrame:
        return df
    
class KafkaMultiplexTransformer(SourceTransformer):
    """
    Applies specific transformations for kafka-multiplex data
    """
    def __init__(self, config: Config):
        self.config = config
        self.date_lookup_table = f"{self.config.database.catalog}.{self.config.database.schema}.date_lookup"

    def transform(self, df: DataFrame) -> DataFrame:
        pass
        df_date_lookup = spark.table(self.date_lookup_table).select(F.col("date"), F.col("week_part"))

        df = df.alias("main").join(F.broadcast(df_date_lookup).alias("lookup"),
                [F.to_date((F.col("main.timestamp")/1000).cast("timestamp")) == F.col("lookup.date")],
                "left")
        return df


# COMMAND ----------

@dataclass
class Source:
    config: SourceConfig
    transformer: SourceTransformer = DefaultTransformer()

# COMMAND ----------

class BronzeIngestion:
    """Handles ingestion of data into the bronze layer"""
    
    def __init__(self, env: str):
        """Initialize with environment configuration"""
        self.config = Config(env)
        self.landing_zone_base = f"{self.config.storage.landing_zone_path}/raw"
        self.checkpoint_base = f"{self.config.storage.checkpoint_path}/checkpoints"
        
        # Define source configurations
        self.sources = self._get_sources()

    def _get_sources(self):
        """
            Prepares and returns a dict of sources configs
            Returns:
                Dict[source_dataset_name: str, source]
        """
        return {
            "registered_users": Source(
                SourceConfig(
                    name=f"{config.database.bronze_prefix}registered_users",
                    format=SourceFormat.CSV,
                    schema="user_id long, device_id long, mac_address string, registration_timestamp double",
                    path=f"{self.landing_zone_base}/registered_users",
                    options={"header": "true"}
                )
            ),
            "gym_logins": Source(
                SourceConfig(
                    name=f"{config.database.bronze_prefix}gym_logins", 
                    format=SourceFormat.CSV,
                    schema="mac_address string, gym bigint, login double, logout double",
                    path=f"{self.landing_zone_base}/gym_logins",
                    options={"header": "true"}
                )
            ),
            "kafka_multiplex": Source(
                SourceConfig(
                    name=f"{config.database.bronze_prefix}kafka_multiplex",
                    format=SourceFormat.JSON, 
                    schema="key string, value string, topic string, partition bigint, offset bigint, timestamp bigint",
                    path=f"{self.landing_zone_base}/kafka_multiplex",
                    partition_cols=["topic", "week_part"]
                ),
                KafkaMultiplexTransformer(self.config)
            )
        }

    @log_execution
    def _read_stream(self, source: SourceConfig, transformer: SourceTransformer) -> DataFrame:
        """
        Creates a stream reader based on the source config and returns the streaming dataframe
        Args:
            source (SourceConfig): configuration and metadata about the source dataset
        Returns:
            Streaming DataFrame
        """
        reader = (spark.readStream
                 .format("cloudFiles")
                 .schema(source.schema)
                #  .option("maxFilesPerTrigger", 1000) # default setting in config applied
                 .option("cloudFiles.format", source.format.value))
        
        # Add any source-specific options
        if source.options:
            for key, value in source.options.items():
                reader = reader.option(key, value)
                
        df = reader.load(source.path)
        
        # Add audit columns 
        # moved this upstream before transformation as joining multiple table would cause error for F.input_file_name
        df = (df.withColumn("load_time", F.current_timestamp())
                     .withColumn("source_file", F.input_file_name()))
        
        df = transformer.transform(df)
        return df
    
    @measure_performance
    @log_execution
    def _write_stream(self, df: DataFrame, source: SourceConfig, once: bool = True) -> StreamingQuery:
        """
        Accepts a streaming dataframe and configures the stream writer using the metadata in source config
        Args:
            df: Input DataFrame
            source: configuration and metadata about the source dataset
            once: If True, process available data and stop
        Returns:
            Streaming Query for monitoring and control
        """
        
        # Configure stream writer
        writer = (df.writeStream
                         .format("delta")
                         .option("checkpointLocation", f"{self.checkpoint_base}/{source.name}")
                         .outputMode("append"))
        
        # Add partitioning if specified
        if source.partition_cols:
            writer = writer.partitionBy(*source.partition_cols)
            
        table_name = f"{self.config.database.catalog}.{self.config.database.schema}.{source.name}"
        # Start the stream
        if once:
            return writer.trigger(availableNow=True).toTable(table_name)
        else:
            return writer.trigger(processingTime="5 seconds").toTable(table_name)

    @measure_performance
    @log_execution
    def ingest(self, once: bool = True) -> None:
        """
        Ingest all configured sources into bronze layer
        
        Args:
            once: If True, process available data once and stop. If False, run continuous streaming.
        """
        print(f"\nStarting bronze layer ingestion...")
        active_queries = {} # keep track of all queries
        failed_sources = {} # keep track of failed sources (issue faced while starting the query)

        try:
            for source_name, source in self.sources.items():
               try:
                    df = self._read_stream(source.config, source.transformer)
                    query = self._write_stream(df, source.config, once)
                    active_queries[source_name] = query
                    print(f"Started stream for {source_name}: ID={query.id}")
               except Exception as e:
                   failed_sources[source_name] = str(e)
                   print(f"Failed to start stream for {source_name}: {str(e)}")
            
            if once:
                for source_name, query in active_queries.items():
                    print(f"Waiting for {source_name} query to complete")
                    query.awaitTermination() # Blocks unitl the source is fully processed
                    print(f"Completed processing of {source_name}") 
                print("All once-time processing completed")
        finally:
            # clean up queries
            for query in active_queries.values():
                if query and query.isActive:
                    query.stop()

                
    @measure_performance
    @log_execution
    def validate(self, data_set_count: int) -> None:
        """
        Validate ingested data counts
        
        Args:
            data_set_count: Expected number of data sets processed
        """
        start = int(time.time())
        print(f"\nValidating bronze layer records...")
        
        # Define expected counts based on data set count
        expected_counts = {
            f"{self.config.database.bronze_prefix}registered_users": 5 if data_set_count == 1 else 10,
            f"{self.config.database.bronze_prefix}gym_logins": 8 if data_set_count == 1 else 16,
            f"{self.config.database.bronze_prefix}kafka_multiplex": {
                "user_info": 7 if data_set_count == 1 else 13,
                "workout": 16 if data_set_count == 1 else 32,
                "bpm": data_set_count * 253801
            }
        }
        
        # Validate each table
        for table, expected in expected_counts.items():
            if isinstance(expected, dict):
                # Handle kafka_multiplex with multiple topics
                for topic, count in expected.items():
                    self._validate_count(table, count, f"topic='{topic}'")
            else:
                self._validate_count(table, expected)
                
        print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")
    
    def _validate_count(self, table: str, expected: int, condition: str = "true") -> None:
        """Validate record count for a table"""
        print(f"Validating record counts in {table}...", end='')
        actual = spark.read.table(f"{self.config.database.catalog}.{self.config.database.schema}.{table}").where(condition).count()
        assert actual == expected, f"Expected {expected:,} records, found {actual:,} in {table} where {condition}"
        print(f"Found {actual:,} / Expected {expected:,} records where {condition}: Success")

# COMMAND ----------

bronze = BronzeIngestion("dev")
bronze.ingest(once=True)  # Run once
bronze.validate(data_set_count=1) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.medallion_fitness_db.date_lookup
# MAGIC -- truncate table dev.medallion_fitness_db.bronze_gym_logins
# MAGIC -- truncate table dev.medallion_fitness_db.bronze_registered_users
