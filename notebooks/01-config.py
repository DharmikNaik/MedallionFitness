# Databricks notebook source
# MAGIC %md
# MAGIC Copyright (c) 2024 Dharmik Naik </br>
# MAGIC This source code is licensed under the MIT license found in the </br>
# MAGIC LICENSE file in the root directory of this source tree.

# COMMAND ----------

class Config():
    def __init__(self):
        self.base_dir_landing_zone = spark.sql('describe external location `data_zone`')
        self.base_dir_checkpoint = spark.sql('describe external location `data_zone`').select('url').collect()[0][0]
        self.db_name = spark.sql('describe external location `checkpoint`').select('url').collect()[0][0]
        self.max_files_per_trigger = 1000
        self.max_bytes_per_trigger = None
