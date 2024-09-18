# Databricks notebook source
# MAGIC %run ../ConfigFolder/ConfigSAS

# COMMAND ----------

Poly_path = generate_path('raw-poly-delta2018','silverlayer')
Delta_path = generate_path('raw-pois-delta2018','silverlayer')
Sales_path = generate_path('raw-sales-delta2018','silverlayer')
Develop_path = generate_path('raw-develop-delta2024','silverlayer')

Transform_Data = generate_path('transform-delta2018','silverlayer')


# COMMAND ----------

# Load the data from the specified paths
df_poly = spark.read.format("delta").load(Poly_path)
df_delta = spark.read.format("delta").load(Delta_path)
df_sales = spark.read.format("delta").load(Sales_path)
df_develop = spark.read.format("delta").load(Develop_path)

# Display basic statistics for each dataframe
display(df_poly.describe())
display(df_delta.describe())
display(df_sales.describe())
display(df_develop.describe())

# COMMAND ----------

# Get the distinct schemas in df_develop
distinct_schemas = df_develop.select("alertName").distinct().collect()

# Create a dictionary to hold dataframes for each schema
schema_dfs = {}

# Loop through each distinct schema and filter the dataframe
for schema in distinct_schemas:
    schema_value = schema["alertName"]
    schema_dfs[schema_value] = df_develop.filter(df_develop["alertName"] == schema_value)

# Display the dataframes for each schema
for schema_value, schema_df in schema_dfs.items():
    print(f"Schema: {schema_value}")
    display(schema_df)

# COMMAND ----------

# Get the distinct schemas in df_develop
distinct_schemas = df_develop.select("alertName").distinct().collect()

# Create a dictionary to hold the number of columns for each schema
schema_column_counts = {}

# Loop through each distinct schema and count the number of columns
for schema in distinct_schemas:
    schema_value = schema["alertName"]
    schema_df = df_develop.filter(df_develop["alertName"] == schema_value)
    schema_column_counts[schema_value] = len(schema_df.columns)

# Display the number of columns for each schema
for schema_value, column_count in schema_column_counts.items():
    print(f"Schema: {schema_value}, Column Count: {column_count}")

# COMMAND ----------

# Get the schemas of each dataframe
schema_poly = set(df_poly.schema.fieldNames())
schema_delta = set(df_delta.schema.fieldNames())
schema_sales = set(df_sales.schema.fieldNames())
schema_develop = set(df_develop.schema.fieldNames())

# Find the differences between the schemas
diff_poly_delta = schema_poly.symmetric_difference(schema_delta)
diff_poly_sales = schema_poly.symmetric_difference(schema_sales)
diff_poly_develop = schema_poly.symmetric_difference(schema_develop)
diff_delta_sales = schema_delta.symmetric_difference(schema_sales)
diff_delta_develop = schema_delta.symmetric_difference(schema_develop)
diff_sales_develop = schema_sales.symmetric_difference(schema_develop)

# Create a dictionary to hold the differences
schema_differences = {
    "poly_delta": diff_poly_delta,
    "poly_sales": diff_poly_sales,
    "poly_develop": diff_poly_develop,
    "delta_sales": diff_delta_sales,
    "delta_develop": diff_delta_develop,
    "sales_develop": diff_sales_develop
}

# Display the schema differences
for key, value in schema_differences.items():
    print(f"Differences between {key}:")
    display(value)

# COMMAND ----------

from pyspark.sql.functions import explode

# Expand 'elementList' in 'df_develop' DataFrame
df_develop_expanded = df_develop.withColumn("element", explode("elementList"))

# Display the expanded DataFrame
display(df_develop_expanded)
