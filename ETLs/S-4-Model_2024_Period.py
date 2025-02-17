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

RawSalesData_Path = generate_path('raw-sales-delta2018', 'silverlayer')

df_rawsalesdata = spark.read.format("delta").load(RawSalesData_Path)

display(df_rawsalesdata)

# COMMAND ----------

# Selecciona valores distintos de la columna 'period' del DataFrame 'df_rawsalesdata'
distinct_period_values = df_rawsalesdata.select("period").distinct()

# Muestra los valores distintos de 'period'
display(distinct_period_values)

# COMMAND ----------

PropertiesTimeSeriesMadrid2024_Path = generate_path('s-properties-timeseries-madrid2024', 'silverlayer')

df_properties_timeseries_madrid2024 = spark.read.format("delta").load(PropertiesTimeSeriesMadrid2024_Path)

display(df_properties_timeseries_madrid2024)

# COMMAND ----------

from pyspark.sql.functions import date_format

# Convert the 'readable_date' column to 'period' in 'yyyyMM' format
df_properties_timeseries_madrid2024_v1 = df_properties_timeseries_madrid2024.withColumn("period", date_format("readable_date", "yyyyMM"))

display(df_properties_timeseries_madrid2024_v1)

# COMMAND ----------

distinct_period_values = df_properties_timeseries_madrid2024_v1.select("period").distinct()
display(distinct_period_values)

# COMMAND ----------

from pyspark.sql.functions import when

# Update the 'period' column based on specific conditions
df_properties_timeseries_madrid2024_v1 = df_properties_timeseries_madrid2024_v1.withColumn(
    "period",
    when(df_properties_timeseries_madrid2024_v1.period.isin("202408", "202407", "202409"), "202409")  # If period is in Aug, Jul, or Sep 2024, set to Sep 2024
    .when(df_properties_timeseries_madrid2024_v1.period.isin("202410", "202411", "202412"), "202412")  # If period is in Oct, Nov, or Dec 2024, set to Dec 2024
    .when(df_properties_timeseries_madrid2024_v1.period.isin("202501", "202502", "202503"), "202503")  # If period is in Jan, Feb, or Mar 2025, set to Mar 2025
    .otherwise(df_properties_timeseries_madrid2024_v1.period)  # Otherwise, keep the original period
)

display(df_properties_timeseries_madrid2024_v1)

# COMMAND ----------

# Save the DataFrame to a Delta table in overwrite mode with schema merging enabled
df_properties_timeseries_madrid2024_v1.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('s-properties-model-madrid2024', 'silverlayer'))
