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

distinct_values_count = {col_name: df_rawsalesdata_2018.select(col_name).distinct().count() for col_name in df_rawsalesdata_2018.columns}
distinct_values_count_df2018 = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

display(distinct_values_count_df2018)

###

from pyspark.sql.functions import col

distinct_values_count = {col_name: df_propertiesmodel2024.select(col_name).distinct().count() for col_name in df_propertiesmodel2024.columns}
distinct_values_count_df_2024 = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

display(distinct_values_count_df_2024)



# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

# Contar valores distintos por columna
distinct_values_count = {col_name: df_rawsalesdata_2018.select(col_name).distinct().count() for col_name in df_rawsalesdata_2018.columns}

# Filtrar columnas con menos de 1000 valores distintos
columns_with_few_distinct_values = [col_name for col_name, count in distinct_values_count.items() if count < 1000]

# Crear una lista de Rows con los valores distintos de las columnas filtradas
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_rawsalesdata_2018.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=distinct_values))

# Convertir la lista de Rows a un DataFrame
distinct_values_df = spark.createDataFrame(rows)

# Mostrar el DataFrame
display(distinct_values_df)

###

from pyspark.sql import Row
from pyspark.sql.functions import col

# Obtener el conteo de valores distintos por columna
distinct_values_count = {col_name: df_propertiesmodel2024.select(col_name).distinct().count() for col_name in df_propertiesmodel2024.columns}

# Filtrar columnas con menos de 1000 valores distintos
columns_with_few_distinct_values = [col_name for col_name, count in distinct_values_count.items() if count < 100000]

# Crear una lista para las filas del DataFrame
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_propertiesmodel2024.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=distinct_values))

# Convertir la lista a un DataFrame
distinct_values_2024_df = spark.createDataFrame(rows)

# Mostrar el DataFrame
display(distinct_values_2024_df)



# COMMAND ----------

# Features de 2018 y 2024
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

# Convertir los nombres a minúsculas para evitar problemas con mayúsculas/minúsculas
features_2018_lower = [feature.lower() for feature in features_2018]
features_2024_lower = [feature.lower() for feature in features_2024]

# COMMAND ----------

df_rawsalesdata2018 = df_rawsalesdata_2018.filter(df_rawsalesdata_2018.region == 'Madrid')
columns_to_select = ['BATHNUMBER', 'PRICE', 'ROOMNUMBER', 'CONSTRUCTEDAREA', 'PERIOD', 'LATITUDE','LONGITUDE','ISPARKINGSPACEINCLUDEDINPRICE']
df_selected2018 = df_rawsalesdata_2018.select(*columns_to_select)
display(df_selected2018)


# COMMAND ----------

df_filtered2024 = df_propertiesmodel2024.filter((df_propertiesmodel2024.operation == 'sale') & (df_propertiesmodel2024.province == 'Madrid'))
display(df_filtered2024)

# COMMAND ----------

columns_to_select_v1 = ['bathrooms', 'isparkingspaceincludedinprice', 'latitude', 'longitude', 'price',  'rooms', 'size', 'period']
df_filtered2024 = df_filtered2024.select(*columns_to_select_v1)
display(df_filtered2024)

# COMMAND ----------

df_selected_2018_renamed = df_selected2018.withColumnRenamed('BATHNUMBER', 'bathrooms') \
                                 .withColumnRenamed('PRICE', 'price') \
                                 .withColumnRenamed('ROOMNUMBER', 'rooms') \
                                 .withColumnRenamed('CONSTRUCTEDAREA', 'size') \
                                 .withColumnRenamed('PERIOD', 'period') \
                                 .withColumnRenamed('LATITUDE', 'latitude') \
                                 .withColumnRenamed('LONGITUDE', 'longitude') \
                                 .withColumnRenamed('ISPARKINGSPACEINCLUDEDINPRICE', 'isparkingspaceincludedinprice')

df_combined = df_filtered2024.unionByName(df_selected_2018_renamed)
display(df_combined)

# COMMAND ----------

from pyspark.sql import functions as F

# Número de filas antes de la unión
num_rows_filtered2024 = df_filtered2024.count()
num_rows_selected_2018_renamed = df_selected_2018_renamed.count()

# Número de filas después de la unión
num_rows_combined = df_combined.count()

# Mostrar los resultados
print(f"Número de filas en df_filtered2024: {num_rows_filtered2024}")
print(f"Número de filas en df_selected_2018_renamed: {num_rows_selected_2018_renamed}")
print(f"Número de filas en df_combined: {num_rows_combined}")

# Comprobar valores nulos en las columnas
null_counts = df_combined.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_combined.columns])
display(null_counts)

# COMMAND ----------

distinct_period_values = df_combined.select("period").distinct()
display(distinct_period_values)

# COMMAND ----------

df_combined.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('g-model-madrid-min-features-combined', 'goldlayer'))

df_filtered2024.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('g-model-madrid-min-features-2024', 'goldlayer'))

df_selected_2018_renamed.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('g-model-madrid-min-features-2018', 'goldlayer'))

