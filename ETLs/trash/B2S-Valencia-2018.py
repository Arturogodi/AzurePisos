# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

# MAGIC %run ./B2Sfuction

# COMMAND ----------

inputfolder = "Valencia18"
outputfolder = "raw-delta2024"

# Example usage
process_json_files(
    input_path=f"abfss://bronzelayer@adslpisos.dfs.core.windows.net/{inputfolder}/",
    output_path=f"abfss://silverlayer@adslpisos.dfs.core.windows.net/{outputfolder}/",
    schema_location="/tmp/schema_location",
    checkpoint_location="/tmp/checkpoint_location",
    regex_pattern=r''
)
