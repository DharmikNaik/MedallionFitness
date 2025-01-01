# Databricks notebook source
# MAGIC %run ../utils/decorators

# COMMAND ----------

from typing import Optional, List, Dict
from dataclasses import dataclass
from abc import ABC, abstractmethod

# COMMAND ----------

@dataclass(frozen=True)
class TableConfig:
    source_table: str
    target_table: str
    partition_columns: Optional[List[str]] = None
    grain: Optional[List[str]] = None
    merge_config: MergeConfig

# COMMAND ----------

class StreamConfig:
    table_configs: Dict[str, TableConfig]
    startingVersion: int = 0


# COMMAND ----------

class Upserter(ABC):
    def 

# COMMAND ----------

class Silver:
    def __init__(self, env: Optional[str]):
        config = Config(env)
        self.streaming_config = self._get_streaming_config()
    
    def _get_streaming_config(self):
        """
        Builds a Dict of table name and TableConfig
        """
        pass

    def upsert(self, table_name: str, once: bool = True):
        """
        Upserts data for specific table (table_name)
        Args:
            table_name
            once: if True, process the available data once.
        Returns:
            Streaming query handle for monitoring and control
        """
        try:
            table_config = self.stream_config.table_configs[table_name]
            merge_config = table_config.merge_config
            stream = self.stream_factory.create_stream(
                table_config,
                merge_config
            )

            # create upserter
            upserter = None

            # configure stream writer
            stream_writer = (stream.writeStream
                .foreachBatch(upserter.upsert)
                .outputMode("update")
                .option("checkpointLocation", f"{config.storage.checkpoint_path}/{table_name}")
                .queryName(f"{table_name}_upsert_stream")            
            )

            #trigger
            if once:
                return stream_writer.trigger(availableNow=True).start()
            else:
                return stream_writer.trigger(processingTime=self.streaming_config.processing_time).start()
        except Exception as e:
            print(f"Failed to upsert {table_name}: {str(e)}")

    def upsert_all(self):
        """
        Orchestrator that upserts all silver layer tables
        """
        pass
