-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Delta Lake internals
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="width:200px; float: right"/>
-- MAGIC
-- MAGIC Let's deep dive into Delta Lake internals.
-- MAGIC
-- MAGIC ## Exploring delta structure
-- MAGIC
-- MAGIC Under the hood, Delta is composed of parquet files and a transactional log. Transactional log contains all the metadata operation. Databricks leverage this information to perform efficient data skipping at scale among other things.
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&org_id=3987092476296021&notebook=%2F05-Advanced-Delta-Lake-Internal&demo_name=delta-lake&event=VIEW&path=%2F_dbdemos%2Fdata-engineering%2Fdelta-lake%2F05-Advanced-Delta-Lake-Internal&version=1">
-- MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### A cluster has been created for this demo
-- MAGIC To run this demo, just select the cluster `dbdemos-delta-lake-jose_a_gomez_diaz` from the dropdown menu ([open cluster configuration](https://adb-3987092476296021.1.azuredatabricks.net/#setting/clusters/0902-110200-3bb4q7yj/configuration)). <br />
-- MAGIC *Note: If the cluster was deleted after 30 days, you can re-create it with `dbdemos.create_cluster('delta-lake')` or re-install the demo: `dbdemos.install('delta-lake')`*

-- COMMAND ----------

-- DBTITLE 1,Init the demo data
-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md ### Exploring delta structure
-- MAGIC
-- MAGIC Delta is composed of parquet files and a transactional log

-- COMMAND ----------

DESCRIBE DETAIL user_delta

-- COMMAND ----------

-- DBTITLE 1,Delta is composed of parquet files
-- MAGIC %python
-- MAGIC folder = spark.sql("DESCRIBE DETAIL user_delta").collect()[0]['location']
-- MAGIC print(folder)
-- MAGIC display(dbutils.fs.ls(folder))

-- COMMAND ----------

-- DBTITLE 1,And a transactional log
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(folder+"/_delta_log"))

-- COMMAND ----------

-- DBTITLE 1,Each log contains parquet files stats for efficient data skipping
-- MAGIC %python
-- MAGIC with open("/dbfs/"+raw_data_location+"/user_delta/_delta_log/00000000000000000000.json") as f:
-- MAGIC   for l in f.readlines():
-- MAGIC     print(json.dumps(json.loads(l), indent = 2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OPTIMIZE in action
-- MAGIC Running an `OPTIMIZE` + `VACUUM` will re-order all our files.
-- MAGIC
-- MAGIC As you can see, we have multiple small parquet files in our folder:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(folder))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's OPTIMIZE our table to see how the engine will compact the table:

-- COMMAND ----------

OPTIMIZE user_delta;
-- as we vacuum with 0 hours, we need to remove the safety check:
set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM user_delta retain 0 hours;

-- COMMAND ----------

-- DBTITLE 1,Only one parquet file remains after the OPTIMIZE+VACUUM operation
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(folder))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it! You know everything about Delta Lake!
-- MAGIC
-- MAGIC As next step, you learn more about Delta Live Table to simplify your ingestion pipeline: `dbdemos.install('delta-live-table')`
-- MAGIC
-- MAGIC Go back to [00-Delta-Lake-Introduction]($./00-Delta-Lake-Introduction).
