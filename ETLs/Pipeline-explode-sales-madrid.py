# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

Develop_path = generate_path('raw-develop-delta2024','silverlayer')

Transform_Data = generate_path('transform-delta2018','silverlayer')

df_develop = spark.read.format("delta").load(Develop_path)


# COMMAND ----------

from pyspark.sql.functions import explode, split

# Convert 'elementList' from STRING to ARRAY
df_develop = df_develop.withColumn("elementList", split(df_develop["elementList"], ","))

# Expand 'elementList' in 'df_develop' DataFrame
df_develop_expanded = df_develop.withColumn("element", explode("elementList"))

# Display the expanded DataFrame
display(df_develop_expanded)

# COMMAND ----------

from pyspark.sql.functions import col

# Get distinct count for each column in df_develop_expanded
distinct_counts = {col_name: df_develop_expanded.select(col_name).distinct().count() for col_name in df_develop_expanded.columns}

# Convert the dictionary to a DataFrame for better visualization
distinct_counts_df = spark.createDataFrame(distinct_counts.items(), ["Column", "Distinct_Count"])

# Display the distinct counts DataFrame
display(distinct_counts_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Filter columns where distinct count is greater than 1
columns_to_keep = [row['Column'] for row in distinct_counts_df.collect() if row['Distinct_Count'] > 1]

# Select only the columns to keep from df_develop_expanded
df_develop_expanded_filtered = df_develop_expanded.select(*columns_to_keep)

# Display the filtered DataFrame
display(df_develop_expanded_filtered)

# COMMAND ----------

# Drop the 'elementList' column from df_develop_expanded_filtered
df_develop_expanded_filtered_no_elementlist = df_develop_expanded_filtered.drop('elementList').drop('summary')

# Describe statistics for numerical columns
statistics_df = df_develop_expanded_filtered_no_elementlist.describe()

# Display the statistics DataFrame
display(statistics_df)

# COMMAND ----------

# Drop the 'elementList' column from df_develop_expanded_filtered
df_drop = df_develop.drop('elementList').drop('summary')

# Describe statistics for numerical columns
statistics_df = df_drop.describe()

# Display the statistics DataFrame
display(statistics_df)
