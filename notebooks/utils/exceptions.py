# Databricks notebook source
# MAGIC %md
# MAGIC Copyright (c) 2024 Dharmik Naik </br>
# MAGIC This source code is licensed under the MIT license found in the </br>
# MAGIC LICENSE file in the root directory of this source tree.

# COMMAND ----------

class MedallionBaseException(Exception):
    """Base exception for all custom exceptions in the pipeline"""
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


# COMMAND ----------


class ConfigurationException(MedallionBaseException):
    """Base class for configuration related exceptions"""
    pass

class StorageLocationException(ConfigurationException):
    """Raised when there are issues with storage locations"""
    pass


# COMMAND ----------


class SetupException(MedallionBaseException):
    """Base class for setup related exceptions"""
    pass

class InitializationException(SetupException):
    """Rasied when there are issues with the initialization of the environment"""
    pass

class DatabaseObjectCreationException(InitializationException):
    """Raised when there are errors while creating database objects like tables/views"""
    pass

class SetupValidationException(SetupException):
    """Raised when setup validation fails"""
    pass

class EnvCleanupException(SetupException):
    """Raised issues with cleaning up the env"""
    pass

