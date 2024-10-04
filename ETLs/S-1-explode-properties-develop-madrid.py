# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paths

# COMMAND ----------

Develop_path = generate_path('raw-develop-delta2024','silverlayer')

Transform_Data = generate_path('s-properties-develop-madrid2024', 'silverlayer')

df_develop = spark.read.format("delta").load(Develop_path)


# COMMAND ----------

# Display the expanded DataFrame
display(df_develop)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ETLs

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

from pyspark.sql.functions import expr

# Remove the first element from the array 'elementListarray'
df_intermediate_modified = df_intermediate.withColumn("elementListarray", expr("slice(elementListarray, 2, size(elementListarray)-1)"))

# Explode the modified 'elementListarray'
df_exploded_modified = df_intermediate_modified.withColumn("elementList_element", explode("elementListarray"))

# Display the resulting DataFrame
display(df_exploded_modified)

# COMMAND ----------

from pyspark.sql.functions import expr

# Extract the substring up to the first occurrence of '","' for the column 'elementList_element'
df_extracted = df_exploded_modified.withColumn("extracted_element", expr("split(elementList_element, '\",\"')[0]"))

# Remove the extracted part from 'elementList_element'
df_extracted = df_extracted.withColumn("elementList_element", expr("regexp_replace(elementList_element, split(elementList_element, '\",\"')[0] || '\",\"', '')"))

# Remove the first '":" from the new column 'extracted_element'
df_extracted = df_extracted.withColumn("extracted_element", expr("regexp_replace(extracted_element, '^\":\"', '')"))

# Display the resulting DataFrame
display(df_extracted)

# COMMAND ----------

from pyspark.sql.functions import split

# Split 'elementList_element' by ',\"' into a new column 'split_elements'
df_final = df_extracted.withColumn("split_elements", split("elementList_element", ',\"'))

# Display the resulting DataFrame
display(df_final)

# COMMAND ----------

# Escribir el resultado df_final en Transform_Data = generate_path('s-develop-madrid2024','silverlayer') modo overwrite en delta
Transform_Data = generate_path('s-properties-develop-madrid2024', 'silverlayer')
df_final.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(Transform_Data)
