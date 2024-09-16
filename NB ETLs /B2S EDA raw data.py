# Databricks notebook source
ls ./PathConfig/


# COMMAND ----------

# DBTITLE 1,Setup Conf: Path Storage
# Ejecutar el notebook setup_config para cargar las variables de entorno
%run ./PathConfig/setup_config


# COMMAND ----------

import os
# Nombre de la cuenta de almacenamiento
storage_account_name = "adslpisos"

# Rutas de los contenedores (sin subdirectorios adicionales)
bronze_path = f"abfss://bronzelayer@{storage_account_name}.dfs.core.windows.net"
silver_path = f"abfss://silverlayer@{storage_account_name}.dfs.core.windows.net"
gold_path = f"abfss://goldlayer@{storage_account_name}.dfs.core.windows.net"

# Establecer estas rutas como variables de entorno
os.environ["BRONZE_PATH"] = bronze_path
os.environ["SILVER_PATH"] = silver_path
os.environ["GOLD_PATH"] = gold_path

print("Rutas y credenciales configuradas con éxito.")

# COMMAND ----------

# Importar librerías necesarias
from pyspark.sql import SparkSession

# Configurar el Spark Session si no se hace automáticamente en tu entorno
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# COMMAND ----------

# Obtener las rutas desde las variables de entorno
import os
bronze_path = os.environ["BRONZE_PATH"]
silver_path = os.environ["SILVER_PATH"]

# Directorio específico dentro de Bronze Layer
bronze_madrid_path = f"{bronze_path}/Madrid18"

# COMMAND ----------

# Definir la ruta específica al directorio 'Madrid18' dentro de Bronze Layer
bronze_madrid_path = f"{bronze_path}/Madrid18"

# Verificar la estructura de los archivos dentro del directorio
display(dbutils.fs.ls(bronze_madrid_path))


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

