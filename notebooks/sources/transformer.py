# Databricks notebook source
# MAGIC %run ../utils/decorators

# COMMAND ----------

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F

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
        self.date_lookup_table_name = f"{self.config.database.catalog}.{self.config.database.schema}.date_lookup"

    @log_execution
    def enrich_w_date_attributes(self, df: DataFrame) -> DataFrame:
        df_date_lookup = spark.table(self.date_lookup_table_name).select(F.col("date"), F.col("week_part"))

        df = df.alias("main"). \
                join(F.broadcast(df_date_lookup).alias("lookup"),
                [F.to_date((F.col("main.timestamp")/1000).cast("timestamp")) == F.col("lookup.date")],
                how="left")

    @log_execution
    @retry()
    def transform(self, df: DataFrame) -> DataFrame:
        df = self.enrich_w_date_attributes(df)
        return df

