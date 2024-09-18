# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

#spark.read.json('abfs://bronzelayer@adslpisos.dfs.core.windows.net/Developdata')
df = spark.read.option("multiline", "true").json(
    'abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/properties_40.279824859864206_-3.754122530339181_1000_homes_sale_20240830235003.json'
)
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode

df_expanded = df.withColumn("element", explode("elementList"))
display(df_expanded)

# COMMAND ----------

from pyspark.sql.functions import explode

df_expanded_again = df_expanded.withColumn(
    "property_details", 
    explode("elementList")
)
display(df_expanded_again)

# COMMAND ----------

df_expanded.write.format("delta").mode("overwrite").save("abfss://silverlayer@adslpisos.dfs.core.windows.net/testdf")

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, to_timestamp, input_file_name

# Extract the date part from the filename
df_with_date = df.withColumn(
    "extracted_date",
    regexp_extract(input_file_name(), r'properties_.*_(\d{14})\.json', 1)
)

# Convert the extracted date to a readable timestamp
df_with_readable_date = df_with_date.withColumn(
    "readable_date",
    to_timestamp("extracted_date", "yyyyMMddHHmmss")
)

display(df_with_readable_date)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, to_timestamp, input_file_name

# Configuración del Autoloader para leer todos los JSON en la carpeta
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/tmp/schema_location")
    .option("multiline", "true")
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")
)

# Extract the date part from the filename
df_with_date = df.withColumn(
    "extracted_date",
    regexp_extract(input_file_name(), r'properties_.*_(\d{14})\.json', 1)
)

# Convert the extracted date to a readable timestamp
df_with_readable_date = df_with_date.withColumn(
    "readable_date",
    to_timestamp("extracted_date", "yyyyMMddHHmmss")
)

# Write the transformed data to Delta format and stop the streaming after completion
query = df_with_readable_date.writeStream.trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint_location") \
    .start("abfss://silverlayer@adslpisos.dfs.core.windows.net/testdf")

# Esperar a que termine el proceso
query.awaitTermination()

# COMMAND ----------

display

# COMMAND ----------

# Configuración del Autoloader para leer todos los JSON en la carpeta
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/tmp/schema_location")  # Ubicación para almacenar el esquema detectado
    .option("multiline", "true")  # Por si los JSON están en varias líneas
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")
)

# Muestra el DataFrame en tiempo real
display(df)


# COMMAND ----------

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema_location")
    .option("multiline", "true")
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")
)

# Escribir en modo batch (una sola vez)
query = df.writeStream.trigger(once=True).format("parquet").start("/output/directory")

# Esperar a que termine el proceso
query.awaitTermination()


# COMMAND ----------

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema_location")
    .option("multiline", "true")
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")
)

# Escribir en modo batch (una sola vez)
query = (
    df.writeStream
    .trigger(once=True)
    .format("parquet")
    .option("checkpointLocation", "/tmp/checkpoint_location")
    .start("/output/directory")
)

# Esperar a que termine el proceso
query.awaitTermination()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import explode

df_expanded = df.withColumn("element", explode("elementList")).select(
    "actualPage",
    "alertName",
    "element.*"
)
display(df_expanded)

# COMMAND ----------

# Estadísticas descriptivas básicas del DataFrame
df.describe().show()


# COMMAND ----------

from pyspark.sql.functions import regexp_extract, to_timestamp, input_file_name

# Configuración del Autoloader para leer todos los JSON en la carpeta
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/tmp/schema_location")
    .option("multiline", "true")
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")
)

# Extract the date part from the filename
df_with_date = df.withColumn(
    "extracted_date",
    regexp_extract(input_file_name(), r'properties_.*_(\d{14})\.json', 1)
)

# Convert the extracted date to a readable timestamp
df_with_readable_date = df_with_date.withColumn(
    "readable_date",
    to_timestamp("extracted_date", "yyyyMMddHHmmss")
)

# Write the transformed data to Delta format and stop the streaming after completion
query = df_with_readable_date.writeStream.trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoint_location") \
    .start("abfss://silverlayer@adslpisos.dfs.core.windows.net/testdf")

# Esperar a que termine el proceso
query.awaitTermination()
