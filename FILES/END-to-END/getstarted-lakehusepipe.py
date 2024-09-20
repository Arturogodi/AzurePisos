# Databricks notebook source
external_location = "<your-external-location>"
 catalog = "<your-catalog>"

 dbutils.fs.put(f"{external_location}/filename.txt", "Hello world!", True)
 display(dbutils.fs.head(f"{external_location}/filename.txt"))
 dbutils.fs.rm(f"{external_location}/filename.txt")

 display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storageexternaldatabrick.blob.core.windows.net",
    "DefaultEndpointsProtocol=https;AccountName=storageexternaldatabrick;AccountKey=LA52ywMIv22J/Z9bkXUW/It+R4NaD9jyFuyZsWyW/WsjwASU0d+/TMw5h0q7bJWAqCC0377i/Og0+AStuEf41A==;EndpointSuffix=core.windows.net"
)


# COMMAND ----------

external_location = "contenedorexterno@storageexternaldatabrick.dfs.core.windows.net"
catalog = "primercataog"

# Escribir un archivo en la external location
dbutils.fs.put(f"{external_location}/filename.txt", "Hello world!", True)

# Leer el contenido del archivo
display(dbutils.fs.head(f"{external_location}/filename.txt"))




# COMMAND ----------

# Eliminar el archivo
dbutils.fs.rm(f"{external_location}/filename.txt")

# Mostrar todos los esquemas en el catálogo especificado
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
