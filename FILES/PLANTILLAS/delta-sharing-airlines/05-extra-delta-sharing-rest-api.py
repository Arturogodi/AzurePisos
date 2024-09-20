# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delta Sharing - consuming data using REST API
# MAGIC
# MAGIC Let's deep dive on how Delta Sharing can be used to consume data using the native REST api.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/delta-sharing/resources/images/delta-sharing-flow.png" width="900px"/>
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&org_id=3987092476296021&notebook=%2F05-extra-delta-sharing-rest-api&demo_name=delta-sharing-airlines&event=VIEW&path=%2F_dbdemos%2Fgovernance%2Fdelta-sharing-airlines%2F05-extra-delta-sharing-rest-api&version=1">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A cluster has been created for this demo
# MAGIC To run this demo, just select the cluster `dbdemos-delta-sharing-airlines-jose_a_gomez_diaz` from the dropdown menu ([open cluster configuration](https://adb-3987092476296021.1.azuredatabricks.net/#setting/clusters/0902-110221-1xh2jc8h/configuration)). <br />
# MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('delta-sharing-airlines')` or re-install the demo: `dbdemos.install('delta-sharing-airlines')`*

# COMMAND ----------

# MAGIC %md ## Exporing REST API Using Databricks OSS Delta Sharing Server
# MAGIC
# MAGIC Databricks hosts a sharing server for test: https://sharing.delta.io/ 
# MAGIC
# MAGIC *Note: it doesn't require authentification, real-world scenario require a Bearer token in your calls*

# COMMAND ----------

# DBTITLE 1,Installing jq to have nice json display as cells output
# MAGIC %sh sudo apt-get install jq

# COMMAND ----------

# DBTITLE 1,List Shares, a share is a top level container
# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares -s | jq '.'

# COMMAND ----------

# DBTITLE 1,List Schema within the delta_sharing share
# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas -s | jq '.'

# COMMAND ----------

# DBTITLE 1,List the tables within our share
# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables -s | jq '.'

# COMMAND ----------

# MAGIC %md ### Get metadata from our "boston-housing" table

# COMMAND ----------

# MAGIC %sh curl https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables/boston-housing/metadata -s | jq '.'

# COMMAND ----------

# MAGIC %md ### Getting the data
# MAGIC Delta Share works by creating temporary self-signed links to download the underlying files. It leverages Delta Lake statistics to pushdown the query and only retrive a subset of file. 
# MAGIC
# MAGIC The REST API allow you to get those links and download the data:

# COMMAND ----------

# DBTITLE 1,Getting access to boston-housing data
# MAGIC %sh curl -X POST https://sharing.delta.io/delta-sharing/shares/delta_sharing/schemas/default/tables/boston-housing/query -s -H 'Content-Type: application/json' -d @- << EOF
# MAGIC {
# MAGIC    "predicateHints" : [
# MAGIC       "date >= '2021-01-01'",
# MAGIC       "date <= '2021-01-31'"
# MAGIC    ],
# MAGIC    "limitHint": 1000
# MAGIC }
# MAGIC EOF
