# Databricks notebook source
# MAGIC %run ./source_config

# COMMAND ----------

from typing import Dict

# COMMAND ----------

class SourceFactory:
    """Factory for creating and managing data sources"""
    
    @staticmethod
    def _get_landing_zone_base(config: Config) -> str:
        """Gets the landing zone base path"""
        return f"{config.storage.landing_zone_path}/raw"
    
    @staticmethod
    def create_sources(config: Config) -> Dict[str, Source]:
        """Creates all configured sources"""
        return {
            "registered_users": SourceFactory._create_registered_users_source(config),
            "gym_logins": SourceFactory._create_gym_logins_source(config),
            "kafka_multiplex": SourceFactory._create_kafka_multiplex_source(config)
        }
    
    @staticmethod
    def _create_registered_users_source(config: Config) -> Source:
        """Creates registered users source configuration"""
        landing_zone_base = SourceFactory._get_landing_zone_base(config)
        return Source(
            SourceConfig(
                name=f"{config.database.bronze_prefix}registered_users",
                format=SourceFormat.CSV,
                schema="user_id long, device_id long, mac_address string, registration_timestamp double",
                path=f"{landing_zone_base}/registered_users",
                options={"header": "true"}
            )
        )
    
    @staticmethod
    def _create_gym_logins_source(config: Config) -> Source:
        """Creates gym logins source configuration"""
        landing_zone_base = SourceFactory._get_landing_zone_base(config)
        return Source(
            SourceConfig(
                name=f"{config.database.bronze_prefix}gym_logins",
                format=SourceFormat.CSV,
                schema="mac_address string, gym bigint, login double, logout double",
                path=f"{landing_zone_base}/gym_logins",
                options={"header": "true"}
            )
        )
    
    @staticmethod
    def _create_kafka_multiplex_source(config: Config) -> Source:
        """Creates kafka multiplex source configuration"""
        landing_zone_base = SourceFactory._get_landing_zone_base(config)
        return Source(
            SourceConfig(
                name=f"{config.database.bronze_prefix}kafka_multiplex",
                format=SourceFormat.JSON,
                schema="key string, value string, topic string, partition bigint, offset bigint, timestamp bigint",
                path=f"{landing_zone_base}/kafka_multiplex",
                partition_cols=["topic", "week_part"]
            ),
            KafkaMultiplexTransformer(config)
        )
