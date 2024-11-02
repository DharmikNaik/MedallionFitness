# Databricks notebook source
# MAGIC %run ../../utils/decorators

# COMMAND ----------

class HistoryLoader():
    def __init__(self, env: Optional[str]):
        config = Config(env)
        self.catalog = config.database.catalog
        self.schema = config.database.schema
        self.lookup_tables_path = config.storage.lookup_files_path

    @measure_performance
    @log_execution
    def load_historical_data(self):
        """
        Loads the historical data and lookup tables
        """
        self.load_lookup_tables()

    @measure_performance
    @log_execution
    def load_lookup_tables(self):
        self.load_date_lookup()

    @measure_performance
    @log_execution
    def load_date_lookup(self):
        spark.sql(f"""INSERT OVERWRITE TABLE {self.catalog}.{self.schema}.date_lookup 
                SELECT date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part 
                FROM json.`{self.lookup_tables_path}/6-date-lookup.json/`""")

    
