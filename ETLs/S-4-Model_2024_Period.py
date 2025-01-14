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

distinct_period_values = df_rawsalesdata.select("period").distinct()
display(distinct_period_values)

# COMMAND ----------

PropertiesTimeSeriesMadrid2024_Path = generate_path('s-properties-timeseries-madrid2024', 'silverlayer')

df_properties_timeseries_madrid2024 = spark.read.format("delta").load(PropertiesTimeSeriesMadrid2024_Path)

display(df_properties_timeseries_madrid2024)

# COMMAND ----------

from pyspark.sql.functions import date_format

df_properties_timeseries_madrid2024_v1 = df_properties_timeseries_madrid2024.withColumn("period", date_format("readable_date", "yyyyMM"))

display(df_properties_timeseries_madrid2024_v1)

# COMMAND ----------

distinct_period_values = df_properties_timeseries_madrid2024_v1.select("period").distinct()
display(distinct_period_values)

# COMMAND ----------

from pyspark.sql.functions import when

df_properties_timeseries_madrid2024_v1 = df_properties_timeseries_madrid2024_v1.withColumn(
    "period",
    when(df_properties_timeseries_madrid2024_v1.period.isin("202408", "202407", "202409"), "202409")
    .when(df_properties_timeseries_madrid2024_v1.period.isin("202410", "202411", "202412"), "202412")
    .when(df_properties_timeseries_madrid2024_v1.period.isin("202501", "202502", "202503"), "202503")
    .otherwise(df_properties_timeseries_madrid2024_v1.period)
)

display(df_properties_timeseries_madrid2024_v1)

# COMMAND ----------

df_properties_timeseries_madrid2024_v1.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('s-properties-model-madrid2024', 'silverlayer'))

# COMMAND ----------



# COMMAND ----------


