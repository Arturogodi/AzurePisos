# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM samples.nyctaxi.trips

# COMMAND ----------

display(spark.read.table("samples.nyctaxi.trips"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG primercataog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.department
# MAGIC (
# MAGIC    deptcode   INT,
# MAGIC    deptname  STRING,
# MAGIC    location  STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.department VALUES
# MAGIC    (10, 'FINANCE', 'EDINBURGH'),
# MAGIC    (20, 'SOFTWARE', 'PADDINGTON');

# COMMAND ----------

#o uso dentro de catalog - permisions - grant o uso codigo
GRANT SELECT ON default.department TO `data-consumers`;
