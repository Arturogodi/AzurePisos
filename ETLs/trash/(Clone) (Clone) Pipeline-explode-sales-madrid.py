# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

Develop_path = generate_path('raw-develop-delta2024','silverlayer')

Transform_Data = generate_path('transform-delta2018','silverlayer')

df_develop = spark.read.format("delta").load(Develop_path)


# COMMAND ----------

# Display the expanded DataFrame
display(df_develop)

# COMMAND ----------

from pyspark.sql.functions import explode, split

# Convert 'elementList' from STRING to ARRAY using ',\"' as the delimiter
df_develop = df_develop.withColumn("elementList", split(df_develop["elementList"], ',\"'))

# Expand 'elementList' in 'df_develop' DataFrame
df_develop_expanded = df_develop.withColumn("element", explode("elementList"))

# Display the expanded DataFrame
display(df_develop_expanded)

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

# Count distinct elements in each column
distinct_counts = df_develop_expanded.agg(*(countDistinct(col(c)).alias(c) for c in df_develop_expanded.columns)).collect()[0].asDict()

# Filter columns with more than 1 distinct value
columns_to_keep = [col for col, count in distinct_counts.items() if count > 1]

# Select only the columns to keep
df_filtered = df_develop_expanded.select(columns_to_keep)

# Display the filtered DataFrame
display(df_filtered)

# COMMAND ----------

df_filtered = df_filtered.drop("paginable", "summary", "total", "totalPages", "upperRangePosition")
display(df_filtered)

# COMMAND ----------


