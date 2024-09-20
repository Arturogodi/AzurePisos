# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS 

# COMMAND ----------

Pois_Data = generate_path('raw-pois-delta2018', 'silverlayer')
Poly_Data = generate_path('raw-poly-delta2018', 'silverlayer')
Sales_Data = generate_path('raw-sales-delta2018', 'silverlayer')


# COMMAND ----------

df_pois = spark.read.format("delta").load(Pois_Data).filter("Region = 'Madrid'")
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

from pyspark.sql.functions import col, sqrt, pow

# Function to calculate distance between two points
def calculate_distance(lat1, lon1, lat2, lon2):
    return sqrt(pow(lat1 - lat2, 2) + pow(lon1 - lon2, 2))

# Explode df_pois to get individual latitude and longitude columns
df_pois_exploded = df_pois.select(
    col("Region"),
    col("City_Center.Lat").alias("City_Center_Lat"),
    col("City_Center.Lon").alias("City_Center_Lon"),
    col("Castellana.Lat").alias("Castellana_Lat"),
    col("Castellana.Lon").alias("Castellana_Lon"),
    col("Metro.Lat").alias("Metro_Lat"),
    col("Metro.Lon").alias("Metro_Lon")
)

# Join df_sales_cleaned with df_pois_exploded on Region
df_sales_with_pois = df_sales_cleaned.join(df_pois_exploded, df_sales_cleaned.Region == df_pois_exploded.Region, "left")

# Calculate distances
df_sales_with_distances = df_sales_with_pois.withColumn(
    "distance_to_castellana",
    calculate_distance(col("Latitude"), col("Longitude"), col("Castellana_Lat"), col("Castellana_Lon"))
).withColumn(
    "distance_to_city_center",
    calculate_distance(col("Latitude"), col("Longitude"), col("City_Center_Lat"), col("City_Center_Lon"))
).withColumn(
    "distance_to_metro",
    calculate_distance(col("Latitude"), col("Longitude"), col("Metro_Lat"), col("Metro_Lon"))
)

# Select relevant columns
df_sales_cleaned = df_sales_with_distances.select(
    df_sales_cleaned.columns + ["distance_to_castellana", "distance_to_city_center", "distance_to_metro"]
)

display(df_sales_cleaned)

# COMMAND ----------

from pyspark.sql.functions import col, sqrt, pow, expr

# Function to calculate distance between two points
def calculate_distance(lat1, lon1, lat2, lon2):
    return sqrt(pow(lat1 - lat2, 2) + pow(lon1 - lon2, 2))

# Explode df_pois to get individual latitude and longitude columns
df_pois_exploded = df_pois.select(
    col("Region"),
    expr("City_Center.Lat[0]").alias("City_Center_Lat"),
    expr("City_Center.Lon[0]").alias("City_Center_Lon"),
    expr("Castellana.Lat[0]").alias("Castellana_Lat"),
    expr("Castellana.Lon[0]").alias("Castellana_Lon"),
    expr("Metro.Lat[0]").alias("Metro_Lat"),
    expr("Metro.Lon[0]").alias("Metro_Lon")
)

# Join df_sales_cleaned with df_pois_exploded on Region
df_sales_with_pois = df_sales_cleaned.join(
    df_pois_exploded,
    df_sales_cleaned["Region"] == df_pois_exploded["Region"],
    "left"
)

# Calculate distances
df_sales_with_distances = df_sales_with_pois.withColumn(
    "distance_to_castellana",
    calculate_distance(
        col("Latitude"),
        col("Longitude"),
        col("Castellana_Lat"),
        col("Castellana_Lon")
    )
).withColumn(
    "distance_to_city_center",
    calculate_distance(
        col("Latitude"),
        col("Longitude"),
        col("City_Center_Lat"),
        col("City_Center_Lon")
    )
).withColumn(
    "distance_to_metro",
    calculate_distance(
        col("Latitude"),
        col("Longitude"),
        col("Metro_Lat"),
        col("Metro_Lon")
    )
)

# Select relevant columns
df_sales_cleaned = df_sales_with_distances.select(
    df_sales_cleaned.columns + [
        "distance_to_castellana",
        "distance_to_city_center",
        "distance_to_metro"
    ]
)

display(df_sales_cleaned)
