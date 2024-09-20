# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

# MAGIC %run ./EDAMadrid

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
