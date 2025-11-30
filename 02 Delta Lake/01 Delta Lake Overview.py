# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Overview
# MAGIC
# MAGIC Delta Lake is an open-source storage layer that brings reliability, performance, and advanced features to data lakes. Built on top of Apache Spark, it enables ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake solves common data lake challenges such as data corruption, inconsistent schemas, and lack of versioning.
# MAGIC
# MAGIC **Key Features:**
# MAGIC * ACID transactions for data reliability
# MAGIC * Schema enforcement and evolution
# MAGIC * Time travel (data versioning)
# MAGIC * Upserts and deletes (MERGE INTO)
# MAGIC * Data compaction and optimization
# MAGIC * Native support for streaming and batch workloads
# MAGIC
# MAGIC In the following steps, we'll demonstrate each of these features with code and explanations.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists training.delta_demo.people_delta_demo

# COMMAND ----------

# DBTITLE 1,Create Delta Table
from pyspark.sql import Row

# Create initial DataFrame with sample data
data = [Row(id=1, name='Alice', age=30), Row(id=2, name='Bob', age=25), Row(id=3, name='Charlie', age=35)]
df = spark.createDataFrame(data)

# Overwrite and create a managed Delta table with the DataFrame in the training.delta_demo schema
df.write.format('delta').mode('overwrite').saveAsTable('training.delta_demo.people_delta_demo')

# Read the Delta table to verify creation and display its contents
delta_df = spark.read.table('training.delta_demo.people_delta_demo')
display(delta_df)

# COMMAND ----------

df.write.format('delta').mode('overwrite').save("/Volumes/training/delta_demo/my_volume/mytable")

# COMMAND ----------

df2=spark.read.load("/Volumes/training/delta_demo/my_volume/mytable")
display(df2)

# COMMAND ----------

# DBTITLE 1,Describe History
# MAGIC %sql
# MAGIC describe history training.delta_demo.people_delta_demo

# COMMAND ----------

# DBTITLE 1,Schema Evolution
from pyspark.sql.functions import lit

# Create DataFrame with new rows and add 'country' column
data = [Row(id=7, name='Tom'), Row(id=8, name='Herry'), Row(id=9, name='Jerry')]
df = spark.createDataFrame(data).withColumn("country", lit("USA"))
display(df)


# COMMAND ----------

# Append new rows to Delta table, merging schema to add 'country' column if not present
df.write.format('delta').mode('append')\
     .option('mergeSchema', 'true')\
    .saveAsTable('training.delta_demo.people_delta_demo')

# Read and display updated Delta table to verify schema evolution and new data
result_df = spark.read.table('training.delta_demo.people_delta_demo')
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history training.delta_demo.people_delta_demo

# COMMAND ----------

# DBTITLE 1,Time Travel
# MAGIC %sql
# MAGIC select * from  training.delta_demo.people_delta_demo version as of 2

# COMMAND ----------

# DBTITLE 1,Time Travel
# MAGIC %sql
# MAGIC select * from  training.delta_demo.people_delta_demo 

# COMMAND ----------

# DBTITLE 1,Restore Table
# MAGIC %sql
# MAGIC restore training.delta_demo.people_delta_demo version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.delta_demo.people_delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history training.delta_demo.people_delta_demo

# COMMAND ----------

# DBTITLE 1,Merge Into

from pyspark.sql.functions import lit

# Prepare new data for merge
data = [Row(id=1, name='Tom', age=28), Row(id=7, name='Herry', age=27), Row(id=8, name='Jerry', age=37)]
df = spark.createDataFrame(data)
display(df)


# COMMAND ----------

# Create a temporary view for the new data
df.createOrReplaceTempView("updates")

# Perform MERGE INTO for insert and update based on id column
merge_sql = """
MERGE INTO training.delta_demo.people_delta_demo AS target
USING updates AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET target.name = source.name, target.age = source.age
WHEN NOT MATCHED THEN
  INSERT (id, name, age) VALUES (source.id, source.name, source.age)
"""

spark.sql(merge_sql)

# Remove country column and keep only id, name, and age
result_df = spark.read.table('training.delta_demo.people_delta_demo').select("id", "name", "age")
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history training.delta_demo.people_delta_demo