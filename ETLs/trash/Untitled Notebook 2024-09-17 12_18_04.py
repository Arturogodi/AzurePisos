# Databricks notebook source
# Definimos las variables de entrada
container_bronze = "bronzelayer"
folder_developdata = "Developdata"
container_silver = "silverlayer"
folder_2024 = "raw-delta2024"
regex_pattern = r"(\d{14})"  # Ejemplo de patrón regex para extraer la fecha del nombre del archivo

# Generamos las rutas de entrada y salida
input_path = generate_path(folder_developdata, container_bronze)
output_path = generate_path(folder_2024, container_silver)
schema_location = generate_path("schema", container_bronze)
checkpoint_location = generate_path("checkpoint", container_silver)

# Procesamos los archivos JSON
process_json_files(input_path, output_path, schema_location, checkpoint_location, regex_pattern)

# COMMAND ----------

def list_files_in_directory(input_path):
    files_info = dbutils.fs.ls(input_path)  # Si usas Databricks
    files = [file_info.path for file_info in files_info if file_info.path.endswith(".json")]
    return files

def read_individual_json_files(file_path):
    df = (
        spark.read
        .format("json")
        .option("multiline", "true")
        .load(file_path)
    )
    return df

def process_individual_json(file, regex_pattern):
    df = read_individual_json_files(file)
    df_transformed = extract_date_from_filename(df, regex_pattern)
    return df_transformed

def write_individual_df_to_delta(df, output_path, checkpoint_location):
    df.write \
        .format("delta") \
        .option("checkpointLocation", checkpoint_location) \
        .mode("append") \
        .save(output_path)

def process_all_files(input_path, output_path, schema_location, checkpoint_location, regex_pattern):
    # Listar todos los archivos JSON en el directorio
    files = list_files_in_directory(input_path)
    
    # Procesar cada archivo individualmente
    for file in files:
        df_transformed = process_individual_json(file, regex_pattern)
