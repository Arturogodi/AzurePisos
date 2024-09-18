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

Silver_path = generate_path('raw-sales-delta2018','silverlayer')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# DBTITLE 1,Madrid Sales
file_name = "Madrid_Sale.json"
dfm = spark.read.json(f"{Bronze_madrid_path}/{file_name}", multiLine=True)
display(dfm)

# COMMAND ----------

# DBTITLE 1,Barcelona Sales
#file_name = "Barcelona_Sale.json"
#dfb = spark.read.json(f"{Bronze_barcelona_path}/{file_name}", multiLine=True)
#display(df)

# COMMAND ----------

# DBTITLE 1,Valencia Sales
file_name = "Valencia_Sale.json"
dfv = spark.read.json(f"{Bronze_valencia_path}/{file_name}", multiLine=True)
display(dfv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data into delta

# COMMAND ----------

# Unify the three DataFrames
df_combined = dfm.union(dfv)

# Save the combined DataFrame in Delta format with overwrite mode and schema merge enabled
df_combined.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(Silver_path)
