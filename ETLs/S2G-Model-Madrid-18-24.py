# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

Premodel_Data = generate_path('s-properties-model-madrid2024', 'silverlayer')

df_premodel = spark.read.format("delta").load(Premodel_Data)

display(df_premodel)



# COMMAND ----------

Developdata_Path = generate_path('raw-delta2024', 'silverlayer')

df_developdata = spark.read.format("delta").load(Developdata_Path)

display(df_developdata)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

RawSalesData_Path = generate_path('raw-sales-delta2018', 'silverlayer')

df_rawsalesdata = spark.read.format("delta").load(RawSalesData_Path)

display(df_rawsalesdata)

# COMMAND ----------

from pyspark.sql.functions import col

distinct_values_count = {col_name: df_rawsalesdata.select(col_name).distinct().count() for col_name in df_rawsalesdata.columns}
distinct_values_count_df = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

display(distinct_values_count_df)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

# Contar valores distintos por columna
distinct_values_count = {
    col_name: df_rawsalesdata.select(col_name).distinct().count()
    for col_name in df_rawsalesdata.columns
}

# Filtrar columnas con menos de 1000 valores distintos
columns_with_few_distinct_values = [
    col_name for col_name, count in distinct_values_count.items() if count < 100000
]

# Crear una lista de Rows con los valores distintos de las columnas filtradas
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_rawsalesdata.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=", ".join(map(str, distinct_values))))

# Convertir la lista de Rows a un DataFrame
distinct_values_df = spark.createDataFrame(rows)

# Mostrar el DataFrame
display(distinct_values_df)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import Row

# Contar valores distintos por columna
distinct_values_count = {col_name: df_rawsalesdata.select(col_name).distinct().count() for col_name in df_rawsalesdata.columns}

# Filtrar columnas con menos de 1000 valores distintos
columns_with_few_distinct_values = [col_name for col_name, count in distinct_values_count.items() if count < 1000]

# Crear una lista de Rows con los valores distintos de las columnas filtradas
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_rawsalesdata.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=distinct_values))

# Convertir la lista de Rows a un DataFrame
distinct_values_df = spark.createDataFrame(rows)

# Mostrar el DataFrame
display(distinct_values_df)



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

distinct_period_values = df_rawsalesdata.select("period").distinct()
display(distinct_period_values)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

PropertiesTimeSeriesMadrid2024_Path = generate_path('s-properties-timeseries-madrid2024', 'silverlayer')

df_properties_timeseries_madrid2024 = spark.read.format("delta").load(PropertiesTimeSeriesMadrid2024_Path)

display(df_properties_timeseries_madrid2024)

# COMMAND ----------

from pyspark.sql.functions import date_format

df_properties_timeseries_madrid2024_v1 = df_properties_timeseries_madrid2024.withColumn("period", date_format("readable_date", "yyyyMM"))

display(df_properties_timeseries_madrid2024_v1)

# COMMAND ----------

from pyspark.sql.functions import col

distinct_values_count = {col_name: df_properties_timeseries_madrid2024_v1.select(col_name).distinct().count() for col_name in df_properties_timeseries_madrid2024_v1.columns}
distinct_values_count_df = spark.createDataFrame(distinct_values_count.items(), ["Column", "DistinctCount"])

display(distinct_values_count_df)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import col

# Obtener el conteo de valores distintos por columna
distinct_values_count = {col_name: df_properties_timeseries_madrid2024_v1.select(col_name).distinct().count() for col_name in df_properties_timeseries_madrid2024_v1.columns}

# Filtrar columnas con menos de 1000 valores distintos
columns_with_few_distinct_values = [col_name for col_name, count in distinct_values_count.items() if count < 100000]

# Crear una lista para las filas del DataFrame
rows = []
for col_name in columns_with_few_distinct_values:
    distinct_values = df_properties_timeseries_madrid2024_v1.select(col_name).distinct().rdd.map(lambda row: row[0]).collect()
    rows.append(Row(Column=col_name, DistinctValues=distinct_values))

# Convertir la lista a un DataFrame
distinct_values_2024_df = spark.createDataFrame(rows)

# Mostrar el DataFrame
display(distinct_values_2024_df)

# COMMAND ----------

distinct_period_values = df_properties_timeseries_madrid2024_v1.select("period").distinct()
display(distinct_period_values)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Select relevant columns for the regression model and cast them to the appropriate types
df_model = df_properties_timeseries_madrid2024_v1.select(
    df_properties_timeseries_madrid2024_v1.price.cast('float').alias('price'),
    df_properties_timeseries_madrid2024_v1.size.cast('float').alias('size'),
    df_properties_timeseries_madrid2024_v1.rooms.cast('float').alias('rooms'),
    df_properties_timeseries_madrid2024_v1.bathrooms.cast('float').alias('bathrooms')
)

# Assemble features into a single vector column
assembler = VectorAssembler(
    inputCols=["size", "rooms", "bathrooms"],
    outputCol="features"
)
df_model = assembler.transform(df_model).select("features", "price")

# Split the data into training and test sets
train_data, test_data = df_model.randomSplit([0.8, 0.2], seed=1234)

# Initialize and train the linear regression model
lr = LinearRegression(labelCol="price", featuresCol="features")
lr_model = lr.fit(train_data)

# Display the model summary metrics
training_summary = lr_model.summary
print("RMSE: %f" % training_summary.rootMeanSquaredError)
print("r2: %f" % training_summary.r2)
print("Coefficients: %s" % str(lr_model.coefficients))
print("Intercept: %s" % str(lr_model.intercept))

# COMMAND ----------

# Crear una nueva propiedad con características específicas
nueva_propiedad = spark.createDataFrame([
    (85.0, 3.0, 2.0)  # tamaño, habitaciones, baños
], ["size", "rooms", "bathrooms"])

# Ensamblar las características en una columna de vector
nueva_propiedad = assembler.transform(nueva_propiedad).select("features")

# Predecir el precio usando el modelo entrenado
prediccion = lr_model.transform(nueva_propiedad)

# Mostrar el precio estimado
display(prediccion.select("prediction"))

# COMMAND ----------



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

display(distinct_values_df)
display(distinct_values_2024_df)

# COMMAND ----------

display(df_rawsalesdata)
display(df_properties_timeseries_madrid2024_v1)

# COMMAND ----------

df_rawsalesdata2018 = df_rawsalesdata.filter(df_rawsalesdata.region == 'Madrid')
display(df_rawsalesdata2018)

# COMMAND ----------

columns_to_select = ['BATHNUMBER', 'PRICE', 'ROOMNUMBER', 'CONSTRUCTEDAREA', 'PERIOD', 'LATITUDE','LONGITUDE','ISPARKINGSPACEINCLUDEDINPRICE']
df_selected2018 = df_rawsalesdata.select(*columns_to_select)
display(df_selected2018)

# COMMAND ----------

df_filtered2024 = df_properties_timeseries_madrid2024_v1.filter((df_properties_timeseries_madrid2024_v1.operation == 'sale') & (df_properties_timeseries_madrid2024_v1.province == 'Madrid'))
display(df_filtered2024)

# COMMAND ----------

columns_to_select_v1 = ['bathrooms', 'isparkingspaceincludedinprice', 'latitude', 'longitude', 'price',  'rooms', 'size', 'period']
df_filtered2024 = df_properties_timeseries_madrid2024_v1.select(*columns_to_select_v1)
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
