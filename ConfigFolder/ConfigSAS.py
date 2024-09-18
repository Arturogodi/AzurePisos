# Databricks notebook source
# Define the variables
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D"
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

