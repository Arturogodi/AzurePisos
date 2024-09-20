# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install('lakehouse-retail-c360', catalog='main', schema='dbdemos_retail_c360')

# COMMAND ----------

import dbdemos
dbdemos.install('uc-04-audit-log')

# COMMAND ----------

import dbdemos
dbdemos.install('uc-02-external-location')

# COMMAND ----------

import dbdemos
dbdemos.install('cdc-pipeline')

# COMMAND ----------

import dbdemos
dbdemos.install('dlt-cdc')

# COMMAND ----------

import dbdemos
dbdemos.install('uc-03-data-lineage')

# COMMAND ----------

dbdemos.install('identity-pk-fk')

# COMMAND ----------

dbdemos.install('auto-loader')

# COMMAND ----------

dbdemos.install('delta-lake')

# COMMAND ----------

dbdemos.install('delta-sharing-airlines')

# COMMAND ----------

dbdemos.install('lakehouse-fsi-credit')

# COMMAND ----------

dbdemos.install('lakehouse-fsi-fraud')

# COMMAND ----------

dbdemos.install('uc-04-system-tables', catalog='main', schema='billing_forecast')

# COMMAND ----------

dbdemos.install('dlt-unit-test')

# COMMAND ----------

dbdemos.install('uc-05-upgrade')
