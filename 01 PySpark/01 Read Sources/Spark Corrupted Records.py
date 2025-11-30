# Databricks notebook source
df=spark.read.format("json").\
    option("mode","PERMISSIVE").\
    load("/Volumes/training/my_volume/my_volume/corrupted_files/sales_corrupted.json")
display(df)

# COMMAND ----------

df=spark.read.format("csv").load("/Volumes/training/my_volume/my_volume/corrupted_files/sales_corrupted.csv")
display(df)

# COMMAND ----------

schema_str = "id INT, customer_name STRING, product STRING, quantity INT, price DOUBLE, remarks STRING, _corrupt_record STRING"

df=spark.read.\
schema(schema_str)\
.format("csv")\
.option("mode","DROPMALFORMED")\
.load("/Volumes/training/my_volume/my_volume/corrupted_files/sales_corrupted.csv")

display(df)

# COMMAND ----------

schema_str = "id INT, customer_name STRING, product STRING, quantity INT, price DOUBLE, remarks STRING"

df=spark.read.\
schema(schema_str)\
.format("csv")\
.option("badRecordsPath","/Volumes/training/my_volume/my_volume/badRecordsPath")\
.load("/Volumes/training/my_volume/my_volume/corrupted_files/sales_corrupted.csv")

display(df)

# COMMAND ----------

df2=spark.read.format("json").option("recursiveFileLookup", "true").load("/Volumes/training/my_volume/my_volume/badRecordsPath")
display(df2)