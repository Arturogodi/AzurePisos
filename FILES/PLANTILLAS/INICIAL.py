# Databricks notebook source
# DBTITLE 1,Connect container
# Montar un contenedor de Azure Blob Storage
storage_account_name = "<STORAGE_ACCOUNT_NAME>"
container_name = "<CONTAINER_NAME>"
storage_account_access_key = dbutils.secrets.get(scope="<SCOPE>", key="<KEY>")

# Conexión al storage
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# Verificación: Listar archivos
display(dbutils.fs.ls(f"/mnt/{container_name}"))

# COMMAND ----------

# DBTITLE 1,Unity Catalog Verification
# Mostrar catálogos
catalogs = spark.sql("SHOW CATALOGS")
display(catalogs)

# COMMAND ----------

# DBTITLE 1,Bronze table
bronze_table = "bronze_table"
raw_data = spark.read.json("/mnt/raw_data/")
raw_data.write.format("delta").mode("append").saveAsTable(bronze_table)

# COMMAND ----------

# DBTITLE 1,Silver table
silver_table = "silver_table"
bronze_df = spark.table("bronze_table")
cleaned_df = bronze_df.filter("column IS NOT NULL")  # Ejemplo de limpieza
cleaned_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

# COMMAND ----------

# DBTITLE 1,Gold Layer
gold_table = "gold_table"
silver_df = spark.table("silver_table")
aggregated_df = silver_df.groupBy("key_column").agg({"value_column": "sum"})
aggregated_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)

# COMMAND ----------

# DBTITLE 1,SET Conf
spark.conf.set("spark.sql.shuffle.partitions", "8")
dbutils.widgets.text("input", "")

# COMMAND ----------

# DBTITLE 1,DLT
@dlt.table
def bronze_table():
    return (
        spark.readStream.format("json").load("/mnt/raw_data/")
    )

@dlt.table
def silver_table():
    return (
        dlt.read("bronze_table").filter("column IS NOT NULL")
    )

