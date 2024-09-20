# Databricks notebook source
from pyspark.sql.functions import col
# Variables de configuración
external_location = "<your-external-location>"
catalog = "primercataog"
schema = "babyhealth"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
database = f"{catalog}.e2e_lakehouse_{username}_db"
source = f"{external_location}/e2e-lakehouse-source"
table = f"{database}.target_table"
checkpoint_path = f"{external_location}/_checkpoint/e2e-lakehouse-demo"

# Configurar el entorno
spark.sql(f"SET c.username='{username}'")
spark.sql(f"SET c.database={database}")
spark.sql(f"SET c.source='{source}'")

# Crear la base de datos
spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
spark.sql("CREATE DATABASE ${c.database}")
spark.sql("USE ${c.database}")

# Limpiar datos de ejecuciones previas
dbutils.fs.rm(source, True)
dbutils.fs.rm(checkpoint_path, True)

# Definir una clase para cargar lotes de datos al origen
class LoadData:

    def __init__(self, source):
        self.source = source

    def get_date(self):
        try:
            df = spark.read.format("json").load(source)
        except:
            return "2021-01-01"
        batch_date = df.selectExpr("max(distinct(date(Year))) + 1 day").first()[0]
        if batch_date.month == 3:
            raise Exception("Source data exhausted")
        return batch_date

    def get_batch(self, batch_date):
        return (
            spark.table("samples.nyctaxi.trips")
            .filter(col("Year").cast("date") == batch_date)
        )

    def write_batch(self, batch):
        batch.write.format("json").mode("append").save(self.source)

    def land_batch(self):
        batch_date = self.get_date()
        batch = self.get_batch(batch_date)
        self.write_batch(batch)

RawData = LoadData(source)


# COMMAND ----------

RawData.land_batch().filter("`fare_amount` == batch_date")


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# Configurar Auto Loader para ingerir datos JSON en una tabla Delta
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(source)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .toTable(table))


# COMMAND ----------

# Leer la tabla
df = spark.read.table(table)

# Mostrar los datos
display(df)


# COMMAND ----------


