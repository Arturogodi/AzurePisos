# Databricks notebook source
df= dbutils.fs.ls("abfss://contenedorexterno@externaldatalake2.dfs.core.windows.net/data.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df= spark.read.csv("abfss://contenedorexterno@externaldatalake2.dfs.core.windows.net/data.csv", header = True)

# COMMAND ----------

display(df)
