# Databricks notebook source
# DBTITLE 1,Import Libraries
import os

# COMMAND ----------

# DBTITLE 1,Environment Variable
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
