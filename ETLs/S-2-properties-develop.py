# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

Transform_Data = generate_path('s-properties-develop-madrid2024', 'silverlayer')

df_develop = spark.read.format("delta").load(Transform_Data)

display(df_develop)

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, count, when, isnan

# Count distinct elements, nulls, and empty strings for each column
distinct_counts = df_develop.agg(*(countDistinct(col(c)).alias(c) for c in df_develop.columns))
null_counts = df_develop.select([count(when(col(c).isNull(), c)).alias(c) for c in df_develop.columns])

# Display the results
display(distinct_counts)
display(null_counts)


# COMMAND ----------

# Identify columns to drop
columns_to_drop = ['_rescued_data'] + [col for col in distinct_counts.columns if distinct_counts.select(col).collect()[0][0] <= 1]

# Drop the identified columns
df_develop_cleaned = df_develop.drop(*columns_to_drop)

# Display the cleaned DataFrame
display(df_develop_cleaned)

# COMMAND ----------

# Drop the specified columns
columns_to_drop = ['paginable', 'summary', 'total', 'totalPages', 'upperRangePosition', 'elementListarray', 'elementList_element']
df_develop_cleaned = df_develop_cleaned.drop(*columns_to_drop)

# Display the cleaned DataFrame
display(df_develop_cleaned)

# COMMAND ----------

# Split the DataFrame into two parts
df_modelo_propiedad = df_develop_cleaned.select('extracted_element', 'split_elements', 'extracted_date', 'readable_date').withColumnRenamed('extracted_element', 'property_code').withColumnRenamed('split_elements', 'property_info')
df_resto = df_develop_cleaned.drop('split_elements').withColumnRenamed('extracted_element', 'property_code')

# Display the DataFrames
display(df_modelo_propiedad)
display(df_resto)

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode the property_info column
df_modelo_propiedad_exploded = df_modelo_propiedad.withColumn("property_info_exploded", explode("property_info"))

# Display the exploded DataFrame
display(df_modelo_propiedad_exploded)

# COMMAND ----------

from pyspark.sql.functions import split, explode, col, lower, regexp_extract
from pyspark.sql import functions as F

# Extract words from property_info_exploded until the first double quote
df_words = df_modelo_propiedad_exploded.withColumn(
    "words",
    split(regexp_extract(col("property_info_exploded"), r'^[^"]*', 0), ' ')
)

# Explode the words into individual rows
df_exploded_words = df_words.withColumn("word", explode(col("words")))

# Convert words to lowercase and filter out empty strings
df_filtered_words = df_exploded_words.withColumn("word", lower(col("word"))).filter(col("word") != "")

# Group by word and count occurrences
df_word_counts = df_filtered_words.groupBy("word").count().orderBy(F.desc("count"))

# Display the word counts
display(df_word_counts)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Create a new column 'Property_key' which is the first word of 'property_info_exploded'
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.withColumn(
    "Property_key",
    lower(split(regexp_extract(col("property_info_exploded"), r'^[^"]*', 0), ' ')[0])
)

# Create a new column 'Property_value' which is the part of the string after ':'
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.withColumn(
    "Property_value",
    regexp_replace(col("property_info_exploded"), r'^[^:]*:\s*', '')
)

# Remove all double quotes from 'Property_value'
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.withColumn(
    "Property_value",
    regexp_replace(col("Property_value"), r'"', '')
)

# Display the updated DataFrame
display(df_modelo_propiedad_exploded)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Remove curly braces from 'Property_value'
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.withColumn(
    "Property_value",
    regexp_replace(col("Property_value"), r'[{}]', '')
)

# Display the updated DataFrame
display(df_modelo_propiedad_exploded)

# COMMAND ----------

from pyspark.sql.functions import when

# Update 'Property_value' to keep only the part after the colon if it contains a colon
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.withColumn(
    "Property_value",
    when(col("Property_value").contains(":"), regexp_replace(col("Property_value"), r'^[^:]*:\s*', '')).otherwise(col("Property_value"))
)

# Display the updated DataFrame
display(df_modelo_propiedad_exploded)

# COMMAND ----------

# Filter out rows where 'Property_key' contains the string 'priceinfo'
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.filter(~col("Property_key").contains("priceinfo"))

# Display the updated DataFrame
display(df_modelo_propiedad_exploded)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Update 'Property_value' to remove commas if 'Property_key' contains 'topplus'
df_modelo_propiedad_exploded = df_modelo_propiedad_exploded.withColumn(
    "Property_value",
    when(col("Property_key").contains("topplus"), regexp_replace(col("Property_value"), ",", "")).otherwise(col("Property_value"))
)

# Display the updated DataFrame
display(df_modelo_propiedad_exploded)

# COMMAND ----------

# Save df_modelo_propiedad_exploded to the specified path with schema evolution enabled
df_modelo_propiedad_exploded.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('s-clean-properties-model-madrid2024', 'silverlayer'))

# Save df_resto to the specified path with schema evolution enabled
df_resto.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(generate_path('s-properties-raw-madrid2024', 'silverlayer'))
