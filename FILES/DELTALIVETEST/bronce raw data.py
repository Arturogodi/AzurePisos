# Databricks notebook source
import os

# Definir los paths de los ficheros
base_path = "/dbfs/FileStore/shared_uploads/tu_usuario/"  # Reemplaza "tu_usuario" con tu nombre de usuario en Databricks
transacciones_a_path = os.path.join(base_path, "transacciones_clientes_A.csv")
transacciones_b_path = os.path.join(base_path, "transacciones_clientes_B.csv")
clientes_info_path = os.path.join(base_path, "clientes_informacion.csv")


# COMMAND ----------

# Cargar los archivos CSV en DataFrames
df_transacciones_a = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(transacciones_a_path)
df_transacciones_b = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(transacciones_b_path)
df_clientes_info = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(clientes_info_path)

# Mostrar las primeras filas para confirmar que la carga fue exitosa
df_transacciones_a.show(5)
df_transacciones_b.show(5)
df_clientes_info.show(5)


# COMMAND ----------

# Inspeccionar los esquemas de los DataFrames
df_transacciones_a.printSchema()
df_transacciones_b.printSchema()
df_clientes_info.printSchema()

# Revisar estadísticas descriptivas de los datos
df_transacciones_a.describe().show()
df_transacciones_b.describe().show()
df_clientes_info.describe().show()

