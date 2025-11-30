# Databricks notebook source
df=spark.read.json("/Volumes/training/my_volume/my_volume/json_files/sales_single_line.json")
display(df)

# COMMAND ----------

df=spark.read.option("multiline","true").json("/Volumes/training/my_volume/my_volume/json_files/sales_multi_line.json")
display(df)

# COMMAND ----------

df.write.format("csv").save("/Volumes/training/my_volume/my_volume/csv_out_2")