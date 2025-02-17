# Databricks notebook source
# MAGIC %run ./Venv_keys

# COMMAND ----------

#sas_token = "copy from Azure Storage into Venv_keys notebook to access the value"

# COMMAND ----------

# Define the variables
storage_account_name = "adslpisos"


# Set Spark configurations for Azure Data Lake Storage
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token)

# COMMAND ----------

def generate_path(folder: str, container: str) -> str:
    """
    Dynamic Path
    
    Args:
        folder (str): La capa a la que te refieres ('bronze', 'silver', 'gold').
        container (str): Nombre del contenedor de Azure Data Lake Gen2 ('bronzelayer', 'silverlayer', 'goldlayer').
        
    Returns:
        str: La ruta generada en formato ABFS (Azure Blob File System).
    """
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{folder}/"

