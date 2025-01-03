# Databricks notebook source
# MAGIC %run ./transformer

# COMMAND ----------

from enum import Enum
from dataclasses import dataclass
from typing import List, Optional

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
    bronze_schema_name: str
    src_datalake_path: str
    partition_cols: Optional[List[str]] = None
    options: Optional[dict] = None

@dataclass
class Source:
    config: SourceConfig
    transformer: SourceTransformer = DefaultTransformer()
