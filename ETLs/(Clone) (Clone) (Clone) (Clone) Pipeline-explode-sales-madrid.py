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
df_intermediate = df_develop.withColumn("elementListarray", split(df_develop["elementList"], 'propertyCode'))

# Display the expanded DataFrame
display(df_intermediate)

# COMMAND ----------

from pyspark.sql.functions import expr

# Extract the substring up to the first occurrence of ',\"' for each element in elementListarray
df_with_extracted = df_intermediate.withColumn("extracted_elements", expr("transform(elementListarray, x -> split(x, ',\"')[0])"))

# Display the DataFrame with the extracted elements
display(df_with_extracted)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Remove all non-numeric characters from the 'extracted_elements' column
df_cleaned = df_with_extracted.withColumn("extracted_elements", expr("transform(extracted_elements, x -> regexp_replace(x, '[^0-9]', ''))"))

# Display the cleaned DataFrame
display(df_cleaned)

# COMMAND ----------

from pyspark.sql.functions import explode

# Explode 'extracted_elements' so each row contains a single element from the array
df_exploded_extracted = df_cleaned.withColumn("extracted_element", explode("extracted_elements"))

# Explode 'elementListarray' so each row contains a single element from the array
df_exploded_elementList = df_cleaned.withColumn("elementList_element", explode("elementListarray"))

# Display the DataFrames after processing
display(df_exploded_extracted)
display(df_exploded_elementList)

# COMMAND ----------

from pyspark.sql.functions import expr, slice

# Start iterating the array from position 1
df_cleaned = df_cleaned.withColumn("extracted_elements", expr("slice(extracted_elements, 2, size(extracted_elements) - 1)"))

# Expand 'extracted_elements' with ',\"' as the delimiter
df_cleaned = df_cleaned.withColumn("element", explode(expr("transform(extracted_elements, x -> split(x, ',\"'))")))

# Display the DataFrame after processing
display(df_cleaned)
