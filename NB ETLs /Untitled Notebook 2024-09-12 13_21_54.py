# Databricks notebook source
# Configura las credenciales
storage_account_name = "adslpisos"
container_name = "bronzelayer"
mount_point = "/mnt/bronzelayer"

# URL del contenedor
source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Montar el contenedor
dbutils.fs.mount(source=source, mountPoint=mount_point, extraConfigs={"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})


# COMMAND ----------

import os
# Nombre de la cuenta de almacenamiento
storage_account_name = "adslpisos"

# Rutas de los contenedores (sin subdirectorios adicionales)
bronze_path = f"abfss://bronzelayer@{storage_account_name}.dfs.core.windows.net"
silver_path = f"abfss://silverlayer@{storage_account_name}.dfs.core.windows.net"
gold_path = f"abfss://goldlayer@{storage_account_name}.dfs.core.windows.net"

# COMMAND ----------

spark.read.load(bronze_path)

dbutils.fs.ls(bronze_path)

