# Databricks notebook source
# Reemplaza estos valores con los reales de tu configuración
client_id = "<client_id>"
tenant_id = "<tenant_id>"
client_secret = "<client_secret>"
storage_account_name = "<storage_account_name>"
container_name = "<container_name>"

# Configuración de OAuth
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Aplicar configuración de OAuth
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs
)


# COMMAND ----------

sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D"
storage_account_name = "adslpisos"
container_name = "broncelayer"

# Montar el contenedor con SAS
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = {"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)


# COMMAND ----------

sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D"
storage_account_name = "adslpisos"
container_name = "broncelayer"

# Mount the container with SAS
dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{container_name}",
    extra_configs = {
        f"fs.azure.sas.{container_name}.{storage_account_name}.dfs.core.windows.net": sas_token
    }
)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adslpisos.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adslpisos.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D")

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

from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

def flatten_df(nested_df):
    # Inicializamos con las columnas que no están anidadas
    flat_cols = [c[0] for c in nested_df.dtypes if not isinstance(nested_df.schema[c[0]].dataType, (StructType, ArrayType))]
    
    # Expandimos los structs
    struct_cols = [c[0] for c in nested_df.dtypes if isinstance(nested_df.schema[c[0]].dataType, StructType)]
    
    # Expandimos los arrays
    array_cols = [c[0] for c in nested_df.dtypes if isinstance(nested_df.schema[c[0]].dataType, ArrayType)]

    # Seleccionamos las columnas planas
    flat_df = nested_df.select(*flat_cols)

    # Expandimos los struct fields
    for struct_col in struct_cols:
        flat_df = flat_df.select("*", f"{struct_col}.*").drop(struct_col)
    
    # Expandimos los arrays
    for array_col in array_cols:
        flat_df = flat_df.withColumn(array_col, explode(array_col))

    return flat_df

# Aplicamos la función al dataframe
df_expanded = flatten_df(df)
display(df_expanded)


# COMMAND ----------

from pyspark.sql.functions import input_file_name

# Leer todos los archivos JSON de la carpeta
df = spark.read.option("multiline", "true").json('abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/*.json')

# Agregar una columna con el nombre del archivo para referencia
df = df.withColumn("source_file", input_file_name())

# Expandir el DataFrame
df_expanded = flatten_df(df)

# Mostrar el DataFrame expandido
display(df_expanded)

# Guardar en formato Delta (siempre usar append para agregar nuevos datos al Delta Table)
df_expanded.write.format("delta").mode("append").save("abfss://bronzelayer@adslpisos.dfs.core.windows.net/delta/properties")



# COMMAND ----------

# Leer los archivos de forma continua (streaming)
df_stream = spark.readStream.format("json").option("multiline", "true").load('abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/')

# Expandir el DataFrame en streaming
df_expanded_stream = flatten_df(df_stream)

# Escribir en formato Delta de manera continua
df_expanded_stream.writeStream.format("delta").option("checkpointLocation", "abfss://bronzelayer@adslpisos.dfs.core.windows.net/checkpoints/properties") \
  .start("abfss://bronzelayer@adslpisos.dfs.core.windows.net/delta/properties")


# COMMAND ----------

# Usar Auto Loader para leer nuevos archivos JSON de forma continua
df_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaLocation", "abfss://bronzelayer@adslpisos.dfs.core.windows.net/schema/properties") \
    .option("cloudFiles.useNotifications", "true") \
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")

# Expandir el DataFrame
#df_expanded_stream = flatten_df(df_stream)

# Escribir en formato Delta de manera continua
#df_expanded_stream.writeStream \
 #   .format("delta") \
  #  .option("checkpointLocation", "abfss://bronzelayer@adslpisos.dfs.core.windows.net/checkpoints/properties") \
   # .start("abfss://bronzelayer@adslpisos.dfs.core.windows.net/delta/properties")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the JSON files
schema = StructType([
    StructField("field1", StringType(), True),
    StructField("field2", IntegerType(), True),
    # Add other fields as necessary
])

# Use Auto Loader to read new JSON files continuously with the specified schema
df_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "abfss://bronzelayer@adslpisos.dfs.core.windows.net/schema/properties") \
    .option("cloudFiles.useNotifications", "true") \
    .schema(schema) \
    .load("abfss://bronzelayer@adslpisos.dfs.core.windows.net/Developdata/")

display(df_stream)
