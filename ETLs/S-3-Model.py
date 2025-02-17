# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

Premodel_Data = generate_path('s-clean-properties-model-madrid2024', 'silverlayer')

df_develop = spark.read.format("delta").load(Premodel_Data)

display(df_develop)

# COMMAND ----------

df_develop = df_develop.drop("property_info", "property_info_exploded")
display(df_develop)

# COMMAND ----------

from pyspark.sql.functions import col, first

# Group by property_code, extracted_date, and readable_date, then pivot on Property_key
# Aggregate by taking the first value of Property_value for each group
df_timeseries = df_develop.groupBy("property_code", "extracted_date", "readable_date").pivot("Property_key").agg(first(col("Property_value")))

# Display the resulting DataFrame
display(df_timeseries)

# COMMAND ----------

from pyspark.sql.functions import col, first

df_pivoted = df_develop.groupBy("property_code").pivot("Property_key").agg(first(col("Property_value")))

display(df_pivoted)

# COMMAND ----------

# Generate descriptive statistics of the pivoted DataFrame
summary_stats = df_pivoted.describe()

# Display the descriptive statistics
display(summary_stats)

# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count

# Count null values for each column
null_counts = df_pivoted.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df_pivoted.columns])
display(null_counts)


# COMMAND ----------

# Select columns with null values
columns_with_nulls = [col for col in null_counts.columns if null_counts.select(col).first()[0] > 0]

# Filter the DataFrame to show only columns with null values
null_counts_filtered = null_counts.select(columns_with_nulls)
display(null_counts_filtered)

# Display the content of the columns with null values
df_columns_with_nulls = df_pivoted.select(columns_with_nulls)
display(df_columns_with_nulls)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

distinct_counts = df_pivoted.select([countDistinct(col(c)).alias(c) for c in df_pivoted.columns])
display(distinct_counts)

# COMMAND ----------

# address, property_code, bathrooms, detailedtype, distance, district, exterior, externalreference, floor, isparkingspaceincludesinprice, latitude, longitude, municipality, neighborhood, newdevelopment,newdevelopmentfinished, operation, parkingspace, parkingspaceprice, price, pricebyarea, pricedropinfo, pricedropppercentage, pricedropvalue, propertytype, province, rooms, showaddress, size, status, subtypology, topplus 

# COMMAND ----------

# Select the specified columns from df_pivoted
selected_columns = [
    "address", "property_code", "bathrooms", "detailedtype", "distance", "district", "exterior", 
    "externalreference", "floor", "isparkingspaceincludedinprice", "latitude", "longitude", 
    "municipality", "neighborhood", "newdevelopment", "newdevelopmentfinished", "operation", 
    "parkingspace", "parkingspaceprice", "price", "pricebyarea", "pricedropinfo", "pricedroppercentage", 
    "pricedropvalue", "propertytype", "province", "rooms", "showaddress", "size", "status", 
    "subtypology", "topplus"
]
selected_columns_ts = [
    "extracted_date","readable_date","address", "property_code", "bathrooms", "detailedtype", "distance", "district", "exterior", 
    "externalreference", "floor", "isparkingspaceincludedinprice", "latitude", "longitude", 
    "municipality", "neighborhood", "newdevelopment", "newdevelopmentfinished", "operation", 
    "parkingspace", "parkingspaceprice", "price", "pricebyarea", "pricedropinfo", "pricedroppercentage", 
    "pricedropvalue", "propertytype", "province", "rooms", "showaddress", "size", "status", 
    "subtypology", "topplus"
]

df_selected = df_pivoted.select(*selected_columns)
display(df_selected)

df_timeseries = df_timeseries.select(*selected_columns_ts)
display(df_timeseries)

# COMMAND ----------

# Remove the specified columns from df_selected
columns_to_remove = [
    "externalreference", "parkingspaceprice", "pricedroppercentage", 
    "pricedropvalue", "showaddress", "subtypology"
]

df_final = df_selected.drop(*columns_to_remove)
display(df_final)

df_timeseries = df_timeseries.drop(*columns_to_remove)
display(df_timeseries)

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Count nulls in each column of df_final
null_counts = df_final.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_final.columns])
display(null_counts)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Convert nulls to 'False' for specified columns
df_final = df_final.withColumn("exterior", when(col("exterior").isNull(), 'false').otherwise(col("exterior")))
df_final = df_final.withColumn("isparkingspaceincludedinprice", when(col("isparkingspaceincludedinprice").isNull(), 'false').otherwise(col("isparkingspaceincludedinprice")))

# Remove specified columns
columns_to_remove = ["neighborhood", "newdevelopmentfinished", "parkingspace", "pricedropinfo"]
df_final = df_final.drop(*columns_to_remove)

display(df_final)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Convert nulls to 'False' for specified columns
df_timeseries = df_timeseries.withColumn("exterior", when(col("exterior").isNull(), 'false').otherwise(col("exterior")))
df_timeseries = df_timeseries.withColumn("isparkingspaceincludedinprice", when(col("isparkingspaceincludedinprice").isNull(), 'false').otherwise(col("isparkingspaceincludedinprice")))

# Remove specified columns
columns_to_remove = ["neighborhood", "newdevelopmentfinished", "parkingspace", "pricedropinfo"]
df_timeseries = df_timeseries.drop(*columns_to_remove)

display(df_timeseries)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Convert 'true]' to 'true' and 'false]' to 'false' in the 'topplus' column
df_final = df_final.withColumn("topplus", regexp_replace(col("topplus"), r'true\]', 'true'))
df_final = df_final.withColumn("topplus", regexp_replace(col("topplus"), r'false\]', 'false'))

display(df_final)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Convert 'true]' to 'true' and 'false]' to 'false' in the 'topplus' column for df_timeseries
df_timeseries = df_timeseries.withColumn("topplus", regexp_replace(col("topplus"), r'true\]', 'true'))
df_timeseries = df_timeseries.withColumn("topplus", regexp_replace(col("topplus"), r'false\]', 'false'))

display(df_timeseries)

# COMMAND ----------

from pyspark.sql.functions import when

# Replace nulls in 'status' column with the string 'original'
df_final = df_final.withColumn("status", when(col("status").isNull(), "original").otherwise(col("status")))

display(df_final)

# COMMAND ----------

# Remove rows where 'floor' column contains nulls and then get all unique values
df_check = df_final.filter(col("floor").isNotNull()).select("floor").distinct()

display(df_check)

# COMMAND ----------

# Remove rows where 'floor' column contains nulls
df_final = df_final.filter(col("floor").isNotNull())

# Check if there are any nulls remaining in the DataFrame in all columns
null_counts_remaining = df_final.select([count(when(col(c).isNull(), c)).alias(c) for c in df_final.columns])

display(null_counts_remaining)

# COMMAND ----------

display(df_final)

# COMMAND ----------

# Save model with schema evolution enabled
df_final.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('s-properties-model-madrid2024', 'silverlayer'))
df_timeseries.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('s-properties-timeseries-madrid2024', 'silverlayer'))
