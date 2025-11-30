# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists training.delta_demo.people_delta_demo

# COMMAND ----------

from pyspark.sql import Row

# Create initial DataFrame with sample data
data = [Row(id=1, name='Alice', age=30), Row(id=2, name='Bob', age=25), Row(id=3, name='Charlie', age=35)]
df = spark.createDataFrame(data)

# Overwrite and create a managed Delta table with the DataFrame in the training.delta_demo schema
df.write.format('delta').mode('overwrite').saveAsTable('training.delta_demo.source_table')

# Read the Delta table to verify creation and display its contents
delta_df = spark.read.table('training.delta_demo.source_table')
display(delta_df)

# COMMAND ----------

df.write.format('delta').mode('append').save("/Volumes/training/delta_demo/my_volume/source_table")

# COMMAND ----------

df2=spark.read.format('delta').load("/Volumes/training/delta_demo/my_volume/source_table")
display(df2)

# COMMAND ----------

# DBTITLE 1,Shallow Clone
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE training.delta_demo.target_shallow SHALLOW CLONE training.delta_demo.source_table ;

# COMMAND ----------

# DBTITLE 1,Deep Clone
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE training.delta_demo.target_deep DEEP CLONE training.delta_demo.source_table ;