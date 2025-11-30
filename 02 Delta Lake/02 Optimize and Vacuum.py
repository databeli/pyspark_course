# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table Optimization Notebook
# MAGIC
# MAGIC This notebook focuses on key Delta Lake maintenance operations:
# MAGIC
# MAGIC - **Optimizing Tables**: Use the OPTIMIZE command to compact small files and improve query performance.
# MAGIC - **ZORDER Optimization**: Apply ZORDER BY to colocate related data, enabling efficient data skipping and faster queries.
# MAGIC - **Running VACUUM**: Remove obsolete files and reclaim storage space by executing the VACUUM command.
# MAGIC
# MAGIC These features help maintain efficient, performant Delta tables in your data lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists training.delta_demo.people_delta_demo

# COMMAND ----------

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

df.write.format('delta').mode('append').save("/Volumes/training/delta_demo/my_volume/mytable")

# COMMAND ----------

df2=spark.read.format('delta').load("/Volumes/training/delta_demo/my_volume/mytable")
display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize "/Volumes/training/delta_demo/my_volume/mytable"

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize training.delta_demo.people_delta_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize "/Volumes/training/delta_demo/my_volume/mytable" zorder by id

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize training.delta_demo.people_delta_demo zorder by id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL training.delta_demo.people_delta_demo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum training.delta_demo.people_delta_demo dry run 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history test.test_schema.employee_target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum test.test_schema.employee_target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum test.test_schema.employee_target_table retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET 'delta.logRetentionDuration' = 'interval 30 days',
# MAGIC SET 'delta.deletedFileRetentionDuration' = 'interval 7 days'