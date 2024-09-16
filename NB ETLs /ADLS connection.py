# Databricks notebook source
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D"
storage_account_name = "adslpisos"
container_name = "broncelayer"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adslpisos.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adslpisos.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", "{sas_token}")

# COMMAND ----------

#spark.read.json('abfs://bronzelayer@adslpisos.dfs.core.windows.net/Developdata')
df = spark.read.option("multiline", "true").json(
    'abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/properties_40.279824859864206_-3.754122530339181_1000_homes_sale_20240830235003.json'
)
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

