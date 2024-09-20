# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-a18d57e2-019d-45b4-9ba4-b704a190ff0a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Cloud Computing 101
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC - Contrast local vs on-prem vs cloud computing
# MAGIC - Introduce the basics of cloud computing
# MAGIC - Explore how Databricks works in a cloud based setting with Spark

# COMMAND ----------

# DBTITLE 0,--i18n-4e55ca41-dc1d-4f3f-9162-8b418a85c60e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Local Execution
# MAGIC
# MAGIC Local execution refers to when you're leveraging only the compute of your local machine to execute code. For example, you're a data scientist running Jupyter notebooks locally on your laptop. 
# MAGIC
# MAGIC <img src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/courses/Python/LocalPicture.png" >

# COMMAND ----------

# DBTITLE 0,--i18n-7d43d290-d840-449b-875e-063186261763
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### On-Prem
# MAGIC
# MAGIC On-prem is short for on-premise. This refers to the situation where someone manages multiple computers that communicate with each other to store data and run code. This offers significantly more compute power and storage than a single machine. 
# MAGIC
# MAGIC
# MAGIC Here is an illustration showing an on-prem setting:
# MAGIC
# MAGIC <img src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/courses/Python/OnPremPicture.png">

# COMMAND ----------

# DBTITLE 0,--i18n-232b8ed7-36c8-4e48-88d3-5c17143e3d76
# MAGIC %md-sandbox
# MAGIC
# MAGIC
# MAGIC #### Cloud
# MAGIC
# MAGIC Managing an on-prem system is difficult, expensive, and scales poorly. An popular alternative is to rent storage and computer power from cloud providers. 
# MAGIC These providers are typically large technology companies such as Amazon, Microsoft, and Google. 
# MAGIC
# MAGIC In this situation, a user simply accesses data and compute via a web browser or other application, while the actual data and computation are being stored and ran in large warehouses of machines called a data center managed by these companies. This is referred to as a cloud-based setting. 
# MAGIC
# MAGIC It is much less expensive and easier to use cloud storage because you don't have to create or manage your own data center. It also allows for easy scaling: just buy as much storage and compute power as you need at the moment and turn it off when you are finished. 
# MAGIC
# MAGIC <img src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/courses/Python/CloudPicture.png" style="width:800px;height:500px;">

# COMMAND ----------

# DBTITLE 0,--i18n-a5697312-b91f-4f54-bb69-2b835a9bd278
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Virtual Machines
# MAGIC
# MAGIC In a cloud based setting we use computers managed by cloud providers to run code and store data. 
# MAGIC
# MAGIC We are able to run code this way by using **virtual machines** on those computers. 
# MAGIC
# MAGIC A virtual machine separates the CPU, memory, networking, and disk storage from other virtual machines on the same computer. 
# MAGIC
# MAGIC By renting virtual machines on cloud computers, we can use the resources those computers provide without worrying about sharing information with other users also renting virtual machines.

# COMMAND ----------

# DBTITLE 0,--i18n-a74616ce-3011-4c79-b0f9-f72fbfbb032e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Cloud Storage
# MAGIC
# MAGIC Cloud providers offer ways to store data on the cloud easily. These services use computers and software that are specialized for storing data in a reliable way that can scale well.
# MAGIC
# MAGIC One type of storage offered by cloud providers is **object storage**, which can store any type of data including text, images, videos, and other binary data. Some examples of cloud object storage are:
# MAGIC
# MAGIC * [Amazon Simple Storage Service (Amazon S3)](https://aws.amazon.com/s3/)
# MAGIC * Microsoft's [Azure Data Lake Storage Gen2 (ADLS Gen 2)](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
# MAGIC * [Google Cloud Storage](https://cloud.google.com/storage)
# MAGIC
# MAGIC Cloud providers also offer services to store and manage relational databases &mdash; such as MySQL, PostgreSQL, and Microsoft SQL Server &mdash; and key-value stores or other "NoSQL" databases &mdash; such as Amazon DynamoDB, Azure Cosmos DB, and Google Cloud Bigtable.

# COMMAND ----------

# DBTITLE 0,--i18n-cc376440-5b2e-4d04-ae07-77c5a08b66d9
# MAGIC %md-sandbox
# MAGIC
# MAGIC
# MAGIC #### Databricks
# MAGIC
# MAGIC <img src="https://s3.us-west-2.amazonaws.com/files.training.databricks.com/images/databricks_cloud_overview.png" style="width:800px;height:500px;">
# MAGIC
# MAGIC Databricks provides a unified, cloud-based platform for running and managing a wide variety of data analytics, business intelligence, data science, and machine learning tasks. Databricks runs on multiple cloud providers and can process the data you store in cloud object storage using the virtual machines of that cloud provider.

# COMMAND ----------

# DBTITLE 0,--i18n-e631525b-075f-485e-979a-dfc8ab90973e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Apache Spark
# MAGIC
# MAGIC A single computer usually has the memory and computational power to perform calculations on data sets up to the size of a few gigabytes or less. Data sets larger than that either can't fit into the memory of a single computer or take an unacceptably long time for a single computer to process. For these types of "big data" use cases, we need a system that can split a large data set into smaller subsets &mdash; often referred to as **partitions** &mdash; and then distribute the processing of these data partitions across a number of computers.
# MAGIC
# MAGIC [Apache Spark](https://spark.apache.org/) is an open-source data processing engine that manages distributed processing of large data sets.
# MAGIC
# MAGIC For example, let's say that we have a large data set and we want to calculate various statistics for some of its numeric columns. With Apache Spark, our program only needs to specify the data set to read and the statistics that we want calculated. We can then run the program on a set of computers that have been configured to serve as an Apache Spark **cluster**. When we run it, Spark automatically:
# MAGIC
# MAGIC * determines how to divide the data set into partitions,
# MAGIC * assigns those partitions to the various computers of the cluster with instructions for calculating per-partition statistics, and
# MAGIC * finally collects those per-partitions statistics and calculates the final results we requested.
# MAGIC
# MAGIC Spark was created originally as a research project at the University of California Berkeley. In 2013, the project was donated to the Apache Software Foundation. That same year the creators of Spark founded Databricks.
# MAGIC
# MAGIC Databricks, in general, uses Apache Spark as the computation engine for the platform. Databricks provides simple management tools for running Spark clusters composed of cloud-provided virtual machines to process the data you have in cloud object storage and other systems.

# COMMAND ----------

# DBTITLE 0,--i18n-5b907bd3-bc39-41d1-8294-1be99f57ca26
# MAGIC %md-sandbox
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/sparkcluster.png" style="width:600px;height:250px;">

# COMMAND ----------

# DBTITLE 0,--i18n-4e483909-d12e-4a76-94e4-b5e04e726ce2
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Unity Catalog
# MAGIC
# MAGIC Unity Catalog is a unified governance solution for data and AI assets on the Databricks Data Intelligence Platform. It is designed to standardize a security model that is consistent and transparent across all clouds. Unity Catalog supports structured data (tables and views), unstructured data (files and folders), and AI assets. It can be integrated with your own object storage so you can manage access to those objects as if they were directly within your metastore. Unity Catalog also supports external catalogs like Alation, Collibra, Informatica EDC, and others.
# MAGIC
# MAGIC Unity Catalog goes one step beyond the traditional two-level namespace and provides an additional level for organizing your securable objects: **catalogs** and **schemas (databases)**.

# COMMAND ----------

# MAGIC %sql SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql SHOW SCHEMAS IN samples

# COMMAND ----------

# MAGIC %sql SHOW TABLES IN samples.tpch

# COMMAND ----------

# DBTITLE 0,--i18n-a499fb6b-eb36-4732-9af9-c84329cdce8a
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Code Versioning and Collaboration with Git
# MAGIC
# MAGIC [Git](https://git-scm.com/) is a free and open source version control system. This means that it tracks the changes to code and allows you to store different versions of a project. You can restore previous versions if needed, and it also allows for branching and merging of a project where you can create different versions of a project focused on developing different features and then combine them back together. 
# MAGIC
# MAGIC Git is a tool that can be run on your local machine or on Databricks to help with version control, but it shines as a collaboration tool when combined with [GitHub](https://github.com/). GitHub is a cloud-based hosting service that lets you manage Git code repositories, and it allows multiple users to download versions of a project, develop for the project, and then push back their changes. These changes can then be merged, so this creates an easy system for collaboration that forms the backbone of code projects. 
# MAGIC
# MAGIC Open Source technology is usually available as a public Github Repository where anyone can download the code and help develop it. For instance, Apache Spark is open source and you can view all its code, download it, and even help create new features all from its GitHub page [here](https://github.com/apache/spark).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
