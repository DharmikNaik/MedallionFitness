# Databricks notebook source
# MAGIC %run ../medallion/bronze/auditor

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------

def get_test_df() -> DataFrame:
    test_data = [("test1", ), ("test2", )]
    return spark.createDataFrame(test_data, ["data"])


def test_bronze_auditor():
    """
    Create a sample dataframe
    add the audit columns

    test:
    check if audit columns were added
    check their data type
    check if original data is preserved
    """
    auditor = BronzeDataAuditor()
    test_df = get_test_df()
    result_df = auditor.add_audit_columns(test_df)

    #test1
    assert "load_time" in result_df.columns, "load_time column not found"
    assert "source_file" in result_df.columns, "source_file column not found"

    #test2
    assert "data" in result_df.columns, "orginal data column not found"
    assert test_df.count() == result_df.count(), "row count mismatch"

    #test3
    schema_dict = {field.name: field.dataType.typeName() for field in result_df.schema.fields}
    assert schema_dict["load_time"] == "timestamp", "load_time should be timestamp"
    assert schema_dict["source_file"] == "string", "source_file should be string"

    result_df.display()



# COMMAND ----------

test_bronze_auditor()
