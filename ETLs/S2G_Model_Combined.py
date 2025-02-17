# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

RawSalesData_Path = generate_path('raw-sales-delta2018', 'silverlayer')

df_rawsalesdata_2018 = spark.read.format("delta").load(RawSalesData_Path)

display(df_rawsalesdata_2018)

# COMMAND ----------

PropertiesModel_Path = generate_path('s-properties-model-madrid2024', 'silverlayer')

df_propertiesmodel2024 = spark.read.format("delta").load(PropertiesModel_Path)

display(df_propertiesmodel2024)

# COMMAND ----------

from pyspark.sql.functions import col

# Count distinct values for each column in df_rawsalesdata_2018
distinct_values_count = {col_name: df_rawsalesdata_2018.select(col_name).distinct().count() for col_name in df_rawsalesdata_2018.columns}
# Convert the dictionary to a DataFrame
distinct_values_count_df2018 = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

# Display the DataFrame
display(distinct_values_count_df2018)

###

from pyspark.sql.functions import col

# Count distinct values for each column in df_propertiesmodel2024
distinct_values_count = {col_name: df_propertiesmodel2024.select(col_name).distinct().count() for col_name in df_propertiesmodel2024.columns}
# Convert the dictionary to a DataFrame
distinct_values_count_df_2024 = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

# Display the DataFrame
display(distinct_values_count_df_2024)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

# Count distinct values for each column
distinct_values_count = {col_name: df_rawsalesdata_2018.select(col_name).distinct().count() for col_name in df_rawsalesdata_2018.columns}

# Filter columns with less than 1000 distinct values
columns_with_few_distinct_values = [col_name for col_name, count in distinct_values_count.items() if count < 1000]

# Create a list of Rows with distinct values of the filtered columns
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_rawsalesdata_2018.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=distinct_values))

# Convert the list of Rows to a DataFrame
distinct_values_df = spark.createDataFrame(rows)

# Display the DataFrame
display(distinct_values_df)

###

from pyspark.sql import Row
from pyspark.sql.functions import col

# Count distinct values for each column
distinct_values_count = {col_name: df_propertiesmodel2024.select(col_name).distinct().count() for col_name in df_propertiesmodel2024.columns}

# Filter columns with less than 100000 distinct values
columns_with_few_distinct_values = [col_name for col_name, count in distinct_values_count.items() if count < 100000]

# Create a list of Rows with distinct values of the filtered columns
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_propertiesmodel2024.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=distinct_values))

# Convert the list of Rows to a DataFrame
distinct_values_2024_df = spark.createDataFrame(rows)

# Display the DataFrame
display(distinct_values_2024_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Count distinct values for each column in df_rawsalesdata_2018
distinct_values_count = {col_name: df_rawsalesdata_2018.select(col_name).distinct().count() for col_name in df_rawsalesdata_2018.columns}
# Convert the dictionary to a DataFrame
distinct_values_count_df2018 = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

# Display the DataFrame
display(distinct_values_count_df2018)

###

# Count distinct values for each column in df_propertiesmodel2024
distinct_values_count = {col_name: df_propertiesmodel2024.select(col_name).distinct().count() for col_name in df_propertiesmodel2024.columns}
# Convert the dictionary to a DataFrame
distinct_values_count_df_2024 = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

# Display the DataFrame
display(distinct_values_count_df_2024)

# COMMAND ----------

# Features from 2018 and 2024
features_2018 = [
    "AMENITYID", "ASSETID", "BATHNUMBER", "BUILTTYPEID_1", "BUILTTYPEID_2", 
    "BUILTTYPEID_3", "CADASTRALQUALITYID", "CADCONSTRUCTIONYEAR", 
    "CADDWELLINGCOUNT", "CADMAXBUILDINGFLOOR", "CONSTRUCTEDAREA", 
    "CONSTRUCTIONYEAR", "DISTANCE_TO_CASTELLANA", "DISTANCE_TO_CITY_CENTER", 
    "DISTANCE_TO_METRO", "FLATLOCATIONID", "FLOORCLEAN", "HASAIRCONDITIONING", 
    "HASBOXROOM", "HASDOORMAN", "HASEASTORIENTATION", "HASGARDEN", 
    "HASLIFT", "HASNORTHORIENTATION", "HASPARKINGSPACE", 
    "HASSOUTHORIENTATION", "HASSWIMMINGPOOL", "HASTERRACE", "HASWARDROBE", 
    "HASWESTORIENTATION", "ISDUPLEX", "ISINTOPFLOOR", 
    "ISPARKINGSPACEINCLUDEDINPRICE", "ISSTUDIO", "LATITUDE", "LONGITUDE", 
    "PARKINGSPACEPRICE", "PERIOD", "PRICE", "ROOMNUMBER", "UNITPRICE", 
    "geometry", "region"
]

features_2024 = [
    "address", "property_code", "bathrooms", "detailedtype", "distance", 
    "district", "exterior", "floor", "isparkingspaceincludedinprice", 
    "latitude", "longitude", "municipality", "newdevelopment", "operation", 
    "price", "pricebyarea", "propertytype", "province", "rooms", "size", 
    "status", "topplus", "extracted_date", "readable_date", "period"
]

# Convert the names to lowercase to avoid case sensitivity issues
features_2018_lower = [feature.lower() for feature in features_2018]
features_2024_lower = [feature.lower() for feature in features_2024]

# COMMAND ----------

df_rawsalesdata2018 = df_rawsalesdata_2018.filter(df_rawsalesdata_2018.region == 'Madrid')
columns_to_select = ['BATHNUMBER', 'PRICE', 'ROOMNUMBER', 'CONSTRUCTEDAREA', 'PERIOD', 'LATITUDE','LONGITUDE','ISPARKINGSPACEINCLUDEDINPRICE']
df_selected2018 = df_rawsalesdata_2018.select(*columns_to_select)
display(df_selected2018)


# COMMAND ----------

# Filter the 2024 properties data for sales operations in the province of Madrid
df_filtered2024 = df_propertiesmodel2024.filter((df_propertiesmodel2024.operation == 'sale') & (df_propertiesmodel2024.province == 'Madrid'))
display(df_filtered2024)

# COMMAND ----------

# Select specific columns from the filtered 2024 properties data
columns_to_select_v1 = ['bathrooms', 'isparkingspaceincludedinprice', 'latitude', 'longitude', 'price', 'rooms', 'size', 'period']
df_filtered2024 = df_filtered2024.select(*columns_to_select_v1)
display(df_filtered2024)

# COMMAND ----------

# Rename columns in the 2018 properties data to match the 2024 properties data
df_selected_2018_renamed = df_selected2018.withColumnRenamed('BATHNUMBER', 'bathrooms') \
                                 .withColumnRenamed('PRICE', 'price') \
                                 .withColumnRenamed('ROOMNUMBER', 'rooms') \
                                 .withColumnRenamed('CONSTRUCTEDAREA', 'size') \
                                 .withColumnRenamed('PERIOD', 'period') \
                                 .withColumnRenamed('LATITUDE', 'latitude') \
                                 .withColumnRenamed('LONGITUDE', 'longitude') \
                                 .withColumnRenamed('ISPARKINGSPACEINCLUDEDINPRICE', 'isparkingspaceincludedinprice')

# Combine the filtered 2024 properties data with the renamed 2018 properties data
df_combined = df_filtered2024.unionByName(df_selected_2018_renamed)
display(df_combined)

# COMMAND ----------

from pyspark.sql import functions as F

# Number of rows before the union
num_rows_filtered2024 = df_filtered2024.count()
num_rows_selected_2018_renamed = df_selected_2018_renamed.count()

# Number of rows after the union
num_rows_combined = df_combined.count()

# Display the results
print(f"Number of rows in df_filtered2024: {num_rows_filtered2024}")
print(f"Number of rows in df_selected_2018_renamed: {num_rows_selected_2018_renamed}")
print(f"Number of rows in df_combined: {num_rows_combined}")

# Check for null values in the columns
null_counts = df_combined.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_combined.columns])
display(null_counts)

# COMMAND ----------

# Select distinct values from the 'period' column in the combined dataframe
distinct_period_values = df_combined.select("period").distinct()

# Display the distinct period values
display(distinct_period_values)

# COMMAND ----------

# Save the combined dataframe to Delta format, overwriting existing data and merging schemas
df_combined.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('g-model-madrid-min-features-combined', 'goldlayer'))

# Save the filtered 2024 dataframe to Delta format, overwriting existing data and merging schemas
df_filtered2024.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('g-model-madrid-min-features-2024', 'goldlayer'))

# Save the renamed 2018 dataframe to Delta format, overwriting existing data and merging schemas
df_selected_2018_renamed.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('g-model-madrid-min-features-2018', 'goldlayer'))
