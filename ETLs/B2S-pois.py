# Databricks notebook source
# DBTITLE 1,Config
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

# DBTITLE 1,Paths
#Input path

Bronze_madrid_path = generate_path('Madrid18','bronzelayer')
Bronze_barcelona_path = generate_path('Barcelona18','bronzelayer')
Bronze_valencia_path = generate_path('Valencia18','bronzelayer')

#Output paths

Silver_path = generate_path('raw-pois-delta2018','silverlayer')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data from Bronze to Silver

# COMMAND ----------

# DBTITLE 1,Madrid Sales
from pyspark.sql.functions import lit

file_name = "Madrid_Pois.json"
dfm = spark.read.json(f"{Bronze_madrid_path}/{file_name}", multiLine=True)
dfm = dfm.withColumn("Region", lit("Madrid"))
display(dfm)

# COMMAND ----------

# DBTITLE 1,Barcelona Sales
from pyspark.sql.functions import lit

file_name = "Barcelona_POIS.json"
dfb = spark.read.json(f"{Bronze_barcelona_path}/{file_name}", multiLine=True)
dfb = dfb.withColumn("Region", lit("Barcelona"))
display(dfb)

# COMMAND ----------

# DBTITLE 1,Valencia Sales
file_name = "Valencia_POIS.json"
dfv = spark.read.json(f"{Bronze_valencia_path}/{file_name}", multiLine=True)
dfv = dfv.withColumn("Region", lit("Valencia"))
display(dfv)

# COMMAND ----------

file_name = "Madrid_.json"
df_ = spark.read.json(f"{Bronze_madrid_path}/{file_name}", multiLine=True)
df_ = df_.withColumn("Region", lit("Madrid"))
display(df_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data into delta

# COMMAND ----------

# DBTITLE 1,Join into 1 file
# Unify the three DataFrames
df_combined = dfm.union(dfv).union(dfb).union(df_)

# Save the combined DataFrame in Delta format with overwrite mode and schema merge enabled
df_combined.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(Silver_path)

# COMMAND ----------

display(df_combined)
