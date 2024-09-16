# Databricks notebook source
# Save the configuration settings to a Python script
config_script = """
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D"
storage_account_name = "adslpisos"
container_name = "broncelayer"
silver_layer_path = "silverlayer"
gold_layer_path = "goldlayer"

spark.conf.set("fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", "{sas_token}")
"""

# Write the script to a file using dbutils.fs.put
dbutils.fs.put("/FileStore/configure_storage.py", config_script, overwrite=True)

# To run this script from other notebooks, use the following command in those notebooks:
# %run /FileStore/configure_storage.py

# COMMAND ----------

# Correct the path to the configuration script
config_script = """
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-09-13T17:26:20Z&st=2024-09-13T09:26:20Z&spr=https&sig=A%2BvylbgjVYdXrOSGENqY0I%2BzjsCdK4RYUWRV9sNYSV4%3D"
storage_account_name = "adslpisos"
container_name = "broncelayer"
silver_layer_path = "silverlayer"
gold_layer_path = "goldlayer"

spark.conf.set("fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", "{sas_token}")
"""

# Write the script to a file using dbutils.fs.put
dbutils.fs.put("/Users/jose.a.gomez.diaz@avanade.com/AzurePisos/configure_storage.py", config_script, overwrite=True)

# To run this script from other notebooks, use the following command in those notebooks:
# %run /Users/jose.a.gomez.diaz@avanade.com/AzurePisos/configure_storage.py
