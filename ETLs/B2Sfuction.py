# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, to_timestamp, input_file_name

def extract_date_from_filename(df, regex_pattern, date_format="yyyyMMddHHmmss"):
    """
    Extracts a date from the input file name using a regular expression pattern and converts it to a timestamp.

    Args:
        df (DataFrame): Input DataFrame.
        regex_pattern (str): Regular expression pattern to extract the date from the file name.
        date_format (str, optional): Format of the date to be parsed. Defaults to "yyyyMMddHHmmss".

    Returns:
        DataFrame: DataFrame with the extracted date and readable timestamp.
    """
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
    """
    Reads JSON files from the specified input path using Auto Loader.

    Args:
        input_path (str): Path to the input JSON files.
        schema_location (str): Path to store the inferred schema.

    Returns:
        DataFrame: DataFrame containing the loaded JSON data.
    """
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
    """
    Writes the DataFrame to a Delta table.

    Args:
        df (DataFrame): Input DataFrame to be written.
        output_path (str): Path to the output Delta table.
        checkpoint_location (str): Path to store the checkpoint information.

    Returns:
        None
    """
    query = df.writeStream.trigger(once=True) \
        .format("delta") \
        .option("checkpointLocation", checkpoint_location) \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .start(output_path)
    
    query.awaitTermination()

def process_json_files(input_path, output_path, schema_location, checkpoint_location, regex_pattern):
    """
    Processes JSON files by reading, transforming, and writing them to a Delta table.

    Args:
        input_path (str): Path to the input JSON files.
        output_path (str): Path to the output Delta table.
        schema_location (str): Path to store the inferred schema.
        checkpoint_location (str): Path to store the checkpoint information.
        regex_pattern (str): Regular expression pattern to extract the date from the file name.

    Returns:
        None
    """
    df = read_json_files(input_path, schema_location)
    df_transformed = extract_date_from_filename(df, regex_pattern)
    write_to_delta(df_transformed, output_path, checkpoint_location)

def generate_path(folder: str, container: str) -> str:
    """
    Generates a dynamic path in ABFS format for Azure Data Lake Gen2.

    Args:
        folder (str): The layer you are referring to ('bronze', 'silver', 'gold').
        container (str): Name of the Azure Data Lake Gen2 container ('bronzelayer', 'silverlayer', 'goldlayer').

    Returns:
        str: The generated path in ABFS format.
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
