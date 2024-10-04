# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

#   Input path

Bronze_develop_path = generate_path('Developdata','bronzelayer')

#   Output paths

Silver_path = generate_path('raw-develop-delta2024','silverlayer')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader, extract date of filename

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

# Read from Bronze path using Auto Loader with schema evolution enabled
df_bronze = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("multiline", "true")
             .option("cloudFiles.schemaLocation", Bronze_develop_path + "/_schema")
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             .load(Bronze_develop_path))

# Extract date using the provided function
df_with_readable_date = extract_date_from_filename(df_bronze, r'properties_.*_(\d{14})\.json', "yyyyMMddHHmmss")

# Start the streaming query with "once" trigger
query = (df_with_readable_date.writeStream
         .format("delta")
         .option("checkpointLocation", Silver_path + "/_checkpoint")
         .outputMode("append")
         .trigger(once=True)
         .start(Silver_path))

# Wait for the query to finish
query.awaitTermination()

# Read the result from the Silver path
df_result = spark.read.format("delta").load(Silver_path)

# Stop the streaming query
query.stop()

# Display the result DataFrame
display(df_result)
