# Databricks notebook source
# MAGIC %run ./B2Sfuction

# COMMAND ----------

# MAGIC %run ../ConfigFolder/Venv_nb

# COMMAND ----------

# Definimos las variables de entrada
container_bronze = "bronzelayer"
folder_barcelona = "Barcelona18"
container_silver = "silverlayer"
folder_2018 = "raw-delta2018"
regex_pattern = r"(\d{14})"  # Ejemplo de patrón regex para extraer la fecha del nombre del archivo



# COMMAND ----------

# Generamos las rutas de entrada y salida
input_path = generate_path(folder_barcelona, container_bronze)
output_path = generate_path('prueba22', container_silver)
schema_location = generate_path("schema", container_bronze)
checkpoint_location = generate_path("checkpoint", container_silver)

# Leemos los archivos JSON
df = read_json_files(input_path, schema_location)

# Escribimos los datos en formato Delta
write_to_delta(df, output_path, checkpoint_location)

# COMMAND ----------

def list_files_in_directory(input_path):
    files_info = dbutils.fs.ls(input_path)  # Si usas Databricks
    files = [file_info.path for file_info in files_info if file_info.path.endswith(".json")]
    return files

def read_individual_json_files(file_path):
    df = (
        spark.readStream
        .format("json")
        .option("multiline", "true")
        .load(file_path)
    )
    return df

def process_individual_json(file):
    df = read_individual_json_files(file)
    return df

def write_individual_df_to_delta(df, output_path, checkpoint_location):
    df.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start(output_path)

def process_all_files(input_path, output_path, schema_location, checkpoint_location):
    # Listar todos los archivos JSON en el directorio
    files = list_files_in_directory(input_path)
    
    # Procesar cada archivo individualmente
    for file in files:
        df = process_individual_json(file)
        write_individual_df_to_delta(df, output_path, checkpoint_location)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_individual_json_files(file_path):
    schema = StructType([
        StructField("field1", StringType(), True),
        StructField("field2", IntegerType(), True),
        # Add other fields as per your JSON structure
    ])
    
    df = (
        spark.readStream
        .schema(schema)
        .format("json")
        .load(file_path)
    )
    return df

def process_individual_json(file):
    df = read_individual_json_files(file)
    return df

def process_all_files(input_path, output_path, schema_location, checkpoint_location):
    files = dbutils.fs.ls(input_path)
    for file in files:
        df = process_individual_json(file.path)
        write_individual_df_to_delta(df, output_path, checkpoint_location)

def write_individual_df_to_delta(df, output_path, checkpoint_location):
    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .start(output_path)
    )


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def process_all_files(input_path, output_path, schema_location, checkpoint_location):
    try:
        # Read the schema
        schema = spark.read.json(schema_location).schema
        
        # Read the input data
        df = spark.read.schema(schema).json(input_path)
        
        # Process the data (example transformation)
        processed_df = df.filter(df['logOffset'].isNotNull())
        
        # Write the output data
        processed_df.write.mode('overwrite').json(output_path)
        
    except AnalysisException as e:
        print(f"AnalysisException: {e}")
    except Exception as e:
        print(f"Exception: {e}")

# Call the function with appropriate paths
process_all_files(input_path, output_path, schema_location, checkpoint_location)
