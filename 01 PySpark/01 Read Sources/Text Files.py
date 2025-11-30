# Databricks notebook source
df=spark.read.text("/Volumes/training/my_volume/my_volume/log_files/")
display(df)

# COMMAND ----------

df=spark.read.text("/Volumes/training/my_volume/my_volume/log_files/",wholetext=True)
display(df)

# COMMAND ----------

df = spark.read.format("binaryFile").load("/Volumes/training/my_volume/my_volume/log_files/")
display(df)

# COMMAND ----------

df=spark.read.text("/Volumes/training/my_volume/my_volume/log_files/",wholetext=True)
df2=df.selectExpr("value", "_metadata.file_path", "_metadata.file_name", "_metadata.file_size", "_metadata.file_modification_time")
display(df2)


# COMMAND ----------

df=spark.read.parquet("/Volumes/training/my_volume/my_volume/parquet_files_2/")
display(df)

# COMMAND ----------

df2=df.selectExpr("customer_name",'id','price','product','quantity','remarks', "_metadata.file_path", "_metadata.file_name", "_metadata.file_size", "_metadata.file_modification_time")
display(df2)