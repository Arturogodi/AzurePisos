# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import regexp_extract, to_timestamp, input_file_name

def extract_date_from_filename(df, regex_pattern, date_format="yyyyMMddHHmmss"):
    df_with_date = df.withColumn(
        "extracted_date",
        regexp_extract(input_file_name(), regex_pattern, 1)
    )
    df_with_readable_date = df_with_date.withColumn(
        "readable_date",
        to_timestamp("extracted_date", date_format)
    )
    return df_with_readable_date

def read_json_files(input_path, schema_location):
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("multiline", "true")
        .load(input_path)
    )
    return df

def write_to_delta(df, output_path, checkpoint_location):
    query = df.writeStream.trigger(once=True) \
        .format("delta") \
        .option("checkpointLocation", checkpoint_location) \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .start(output_path)
    
    query.awaitTermination()

def process_json_files(input_path, output_path, schema_location, checkpoint_location, regex_pattern):
    df = read_json_files(input_path, schema_location)
    df_transformed = extract_date_from_filename(df, regex_pattern)
    write_to_delta(df_transformed, output_path, checkpoint_location)

def generate_path(folder: str, container: str) -> str:
    """
    Dynamic Path
    
    Args:
        folder (str): La capa a la que te refieres ('bronze', 'silver', 'gold').
        container (str): Nombre del contenedor de Azure Data Lake Gen2 ('bronzelayer', 'silverlayer', 'goldlayer').
        
    Returns:
        str: La ruta generada en formato ABFS (Azure Blob File System).
    """
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{folder}/"

# COMMAND ----------

def read_json_files(input_path, schema_location):
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("multiline", "true")
        .load(input_path)
    )
    return df

def write_to_delta(df, output_path, checkpoint_location):
    query = df.writeStream.trigger(once=True) \
        .format("delta") \
        .option("checkpointLocation", checkpoint_location) \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .start(output_path)
    
    query.awaitTermination()

def process_json_files(input_path, output_path, schema_location, checkpoint_location):
    df = read_json_files(input_path, schema_location)
    write_to_delta(df, output_path, checkpoint_location)

def generate_path(folder: str, container: str) -> str:
    """
    Dynamic Path
    
    Args:
        folder (str): La capa a la que te refieres ('bronze', 'silver', 'gold').
        container (str): Nombre del contenedor de Azure Data Lake Gen2 ('bronzelayer', 'silverlayer', 'goldlayer').
        
    Returns:
        str: La ruta generada en formato ABFS (Azure Blob File System).
    """
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{folder}/"
