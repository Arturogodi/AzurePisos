# Databricks notebook source
# Variables de configuración
catalog = "primercataog"
schema = "babyhealth"
volume = "baby"
download_url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
file_name = "baby_names.csv"
table_name = "baby_names"
silver_table_name = "baby_names_prepared"
gold_table_name = "top_baby_names_2021"
path_volume = f"/Volumes/{catalog}/{schema}/{volume}"
path_table = f"{catalog}.{schema}"

print(path_table)  # Mostrar la ruta completa
print(path_volume)  # Mostrar la ruta completa

# Crear el directorio si no existe
dbutils.fs.mkdirs(path_volume)

# Descargar el archivo CSV al volumen
dbutils.fs.cp(download_url, f"{path_volume}/{file_name}")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Definir el esquema
schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("First_Name", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Count", IntegerType(), True)
])

# Leer los datos desde el CSV
df = (spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(f"{path_volume}/{file_name}"))

# Escribir los datos en la tabla bronze
df.write.format("delta").mode("overwrite").saveAsTable(f"{path_table}.{table_name}")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear la tabla gold
# MAGIC CREATE OR REPLACE TABLE primercataog.babyhealth.top_baby_names_2021 (
# MAGIC     First_Name STRING,
# MAGIC     Count INT,
# MAGIC     Sex STRING,
# MAGIC     County STRING
# MAGIC );
# MAGIC
# MAGIC -- Poblar la tabla gold con los nombres más populares de 2021
# MAGIC INSERT INTO primercataog.babyhealth.top_baby_names_2021
# MAGIC SELECT
# MAGIC     First_Name,
# MAGIC     SUM(Count) AS Count,
# MAGIC     Sex,
# MAGIC     County
# MAGIC FROM
# MAGIC     primercataog.babyhealth.baby_names_prepared
# MAGIC WHERE
# MAGIC     Year_Of_Birth = 2021
# MAGIC GROUP BY
# MAGIC     First_Name,
# MAGIC     Sex,
# MAGIC     County
# MAGIC ORDER BY
# MAGIC     Count DESC;
# MAGIC
# MAGIC
