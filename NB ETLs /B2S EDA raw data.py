# Databricks notebook source
# DBTITLE 1,Setup Conf: Path Storage
# Ejecutar el notebook setup_config para cargar las variables de entorno
%run /PathConfig/Setup_config

# COMMAND ----------

# Importar librerías necesarias
from pyspark.sql import SparkSession

# Configurar el Spark Session si no se hace automáticamente en tu entorno
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# COMMAND ----------

# Obtener las rutas desde las variables de entorno
import os
bronze_path = os.environ["bronze_path"]
silver_path = os.environ["silver_path"]

# Directorio específico dentro de Bronze Layer
bronze_madrid_path = f"{bronze_path}/Madrid18"

# COMMAND ----------

# Uso de Auto Loader para leer los datos desde el directorio Madrid18 (Bronze Layer)
df_bronze = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", f"{bronze_madrid_path}/_schemas") \
    .load(bronze_madrid_path)

# Muestra los datos cargados (si son muchos archivos, limitar el número de filas)
df_bronze.show(10)

# COMMAND ----------

# Aquí podrías realizar una transformación adicional si es necesario
# Por ejemplo, seleccionamos solo las columnas relevantes
df_transformed = df_bronze.select("col1", "col2", "col3")  # Cambiar por las columnas que tengas

# Guardar los datos en formato Delta en la capa Silver
silver_output_path = f"{silver_path}/processed/Madrid18"

df_transformed.write.format("delta").mode("overwrite").save(silver_output_path)

# Crear una tabla Delta a partir de los datos procesados (opcional)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_madrid18
    USING DELTA
    LOCATION '{silver_output_path}'
""")

print(f"Datos de Madrid18 procesados y guardados en Silver Layer en: {silver_output_path}")

