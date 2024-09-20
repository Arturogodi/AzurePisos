# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import functions as F

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("Banco_ETL").getOrCreate()

# Crear el esquema para las tablas
schema_transacciones = StructType([
    StructField("id_cliente", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("cuenta", IntegerType(), True),
    StructField("tipo_transaccion", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("fecha_transaccion", StringType(), True)
])

schema_clientes_info = StructType([
    StructField("id_cliente", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("país", StringType(), True),
    StructField("ciudad", StringType(), True)
])

# Dataset 1: transacciones_clientes_A
data_transacciones_a = [
    (1, "Juan", 1001, "depósito", 2000.0, "2023-07-01"),
    (2, "María", 1002, "retiro", 500.0, "2023-07-02"),
    (3, "Pedro", 1003, "depósito", 1500.0, "2023-07-03"),
    (4, "Ana", 1004, "retiro", 700.0, "2023-07-04"),
    (5, "José", 1005, "depósito", 3000.0, "2023-07-05"),
    (6, "Laura", 1006, "retiro", 800.0, "2023-07-06"),
    (7, "Luis", 1007, "depósito", 1000.0, "2023-07-07"),
    (8, "Marta", 1008, "retiro", 400.0, "2023-07-08"),
    (9, "Diego", 1009, "depósito", 2500.0, "2023-07-09"),
    (10, "Lucía", 1010, "retiro", 600.0, "2023-07-10")
]

# Dataset 2: transacciones_clientes_B (ligeramente modificado)
data_transacciones_b = [
    (1, "Juan", 1001, "depósito", 2100.0, "2023-07-01"),
    (2, "María", 1002, "retiro", 450.0, "2023-07-02"),
    (3, "Pedro", 1003, "depósito", 1700.0, "2023-07-03"),
    (4, "Ana", 1004, "retiro", 600.0, "2023-07-04"),
    (5, "José", 1005, "depósito", 3300.0, "2023-07-05"),
    (6, "Laura", 1006, "retiro", 1000.0, "2023-07-06"),
    (7, "Luis", 1007, "depósito", 1100.0, "2023-07-07"),
    (8, "Marta", 1008, "retiro", 500.0, "2023-07-08"),
    (9, "Diego", 1009, "depósito", 2500.0, "2023-07-09"),
    (10, "Lucía", 1010, "retiro", 600.0, "2023-07-10")
]

# Dataset 3: clientes_informacion
data_clientes_info = [
    (1, "Juan", 30, "España", "Madrid"),
    (2, "María", 25, "España", "Barcelona"),
    (3, "Pedro", 40, "México", "CDMX"),
    (4, "Ana", 35, "Perú", "Lima"),
    (5, "José", 50, "Chile", "Santiago"),
    (6, "Laura", 28, "México", "Guadalajara"),
    (7, "Luis", 38, "Argentina", "Buenos Aires"),
    (8, "Marta", 33, "Colombia", "Bogotá"),
    (9, "Diego", 45, "España", "Valencia"),
    (10, "Lucía", 32, "España", "Sevilla")
]

# Crear los DataFrames
df_transacciones_a = spark.createDataFrame(data_transacciones_a, schema=schema_transacciones)
df_transacciones_b = spark.createDataFrame(data_transacciones_b, schema=schema_transacciones)
df_clientes_info = spark.createDataFrame(data_clientes_info, schema=schema_clientes_info)

# Convertir la columna de fecha a tipo fecha
df_transacciones_a = df_transacciones_a.withColumn("fecha_transaccion", F.to_date("fecha_transaccion", "yyyy-MM-dd"))
df_transacciones_b = df_transacciones_b.withColumn("fecha_transaccion", F.to_date("fecha_transaccion", "yyyy-MM-dd"))


# COMMAND ----------


# Definir la ruta de salida para los archivos Delta (en el entorno de Databricks)
delta_path_a = "/dbfs/FileStore/transacciones_clientes_A_delta"
delta_path_b = "/dbfs/FileStore/transacciones_clientes_B_delta"
delta_path_info = "/dbfs/FileStore/clientes_informacion_delta"

# Guardar los DataFrames como archivos Delta
df_transacciones_a.write.format("delta").mode("overwrite").save(delta_path_a)
df_transacciones_b.write.format("delta").mode("overwrite").save(delta_path_b)
df_clientes_info.write.format("delta").mode("overwrite").save(delta_path_info)

