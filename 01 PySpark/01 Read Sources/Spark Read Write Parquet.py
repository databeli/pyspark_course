# Databricks notebook source
df=spark.read.json("/Volumes/training/my_volume/my_volume/json_files/sales_single_line.json")
display(df)

# COMMAND ----------

df.write.format("parquet").save("/Volumes/training/my_volume/my_volume/parquet_files_2")

# COMMAND ----------

df2=spark.read.format("parquet").load("/Volumes/training/my_volume/my_volume/parquet_files_2")

# COMMAND ----------

display(df2)