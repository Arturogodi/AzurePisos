# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS 

# COMMAND ----------

Pois_Data = generate_path('raw-pois-delta2018', 'silverlayer')
Poly_Data = generate_path('raw-poly-delta2018', 'silverlayer')
Sales_Data = generate_path('raw-sales-delta2018', 'silverlayer')


# COMMAND ----------

df_pois = spark.read.format("delta").load(Pois_Data).filter("Region = 'Madrid'").dropDuplicates()
df_poly = spark.read.format("delta").load(Poly_Data).filter("city = 'Madrid'")
df_sales = spark.read.format("delta").load(Sales_Data).filter("Region = 'Madrid'")

display(df_pois)
display(df_poly)
display(df_sales)

# COMMAND ----------

from pyspark.sql.functions import col, isnan, when, count

# Function to count nulls in each column except 'geometry'
def count_nulls(df):
    return df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns if c != 'geometry'])

# Count nulls in each DataFrame

nulls_sales = count_nulls(df_sales)

display(nulls_sales)

# COMMAND ----------

# Drop rows with any null values
df_sales_cleaned = df_sales.dropna()

# Drop specified columns
columns_to_drop = ['CONSTRUCTIONYEAR', 'FLATLOCATIONID', 'FLOORCLEAN']
df_sales_cleaned = df_sales_cleaned.drop(*columns_to_drop)

display(df_sales_cleaned)

# COMMAND ----------

# Function to count distinct values in each column except 'geometry'
def count_distinct(df):
    from pyspark.sql.functions import countDistinct
    return df.select([countDistinct(col(c)).alias(c) for c in df.columns if c != 'geometry'])

# Count distinct values in each DataFrame
distinct_sales = count_distinct(df_sales)

display(distinct_sales)

# COMMAND ----------

# Drop specified columns
columns_to_drop = ['amenityid', 'assestid', 'builttypeidid_1', 'builttypeidid_2', 'builttypeidid_3', 'cadastralqualityid', 'adastralqualityyear', 'caddwellingcount', 'cadmaxbuildingfloor']
df_sales_cleaned = df_sales_cleaned.drop(*columns_to_drop)

display(df_sales_cleaned)

# COMMAND ----------

from pyspark.sql.functions import explode, col

# Explode nested columns in df_pois
df_pois_expanded = df_pois \
    .withColumn("Castellana", explode("Castellana")) \
    .withColumn("City_Center", explode("City_Center")) \
    .withColumn("Metro", explode("Metro")) \
    .select(
        col("Castellana.Lat").alias("Castellana_Lat"),
        col("Castellana.Lon").alias("Castellana_Lon"),
        col("City_Center.Lat").alias("City_Center_Lat"),
        col("City_Center.Lon").alias("City_Center_Lon"),
        col("Metro.Lat").alias("Metro_Lat"),
        col("Metro.Lon").alias("Metro_Lon"),
        "Region"
    )

display(df_pois_expanded)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, sqrt, pow, min as spark_min

# Conversion factor from degrees to meters (approximation)
DEGREE_TO_METER = 111320

# Calculate the minimum distance from each row in df_sales_cleaned to the points in df_pois_expanded
df_sales_with_min_distance = df_sales_cleaned.crossJoin(df_pois_expanded) \
    .withColumn("distance_to_Castellana", sqrt(pow((col("LATITUDE") - col("Castellana_Lat")) * DEGREE_TO_METER, 2) + pow((col("LONGITUDE") - col("Castellana_Lon")) * DEGREE_TO_METER, 2))) \
    .withColumn("distance_to_City_Center", sqrt(pow((col("LATITUDE") - col("City_Center_Lat")) * DEGREE_TO_METER, 2) + pow((col("LONGITUDE") - col("City_Center_Lon")) * DEGREE_TO_METER, 2))) \
    .withColumn("distance_to_Metro", sqrt(pow((col("LATITUDE") - col("Metro_Lat")) * DEGREE_TO_METER, 2) + pow((col("LONGITUDE") - col("Metro_Lon")) * DEGREE_TO_METER, 2))) \
    .groupBy("ASSETID") \
    .agg(
        spark_min("distance_to_Castellana").alias("min_distance_to_Castellana"),
        spark_min("distance_to_City_Center").alias("min_distance_to_City_Center"),
        spark_min("distance_to_Metro").alias("min_distance_to_Metro")
    )

display(df_sales_with_min_distance)

# COMMAND ----------

df_sales_joined = df_sales_cleaned.join(df_sales_with_min_distance, on="ASSETID", how="left")
display(df_sales_joined)

# COMMAND ----------


