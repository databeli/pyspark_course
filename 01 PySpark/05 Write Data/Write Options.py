# Databricks notebook source
df=spark.read.table("training.silver.customers")
display(df)

# COMMAND ----------

# DBTITLE 1,Write File Formats
df.write.mode("overwrite").format("csv").save("/Volumes/train_write/my_schema/my_volume/write_test4")

# COMMAND ----------

# DBTITLE 1,Write to a table
df.write.saveAsTable("train_write.my_schema.customers_2")

# COMMAND ----------

# DBTITLE 1,Write Modes
df.write.mode("overwrite").save("/Volumes/train_write/my_schema/my_volume/write_test1")

# COMMAND ----------

# DBTITLE 1,Partition
from pyspark.sql.functions import year

df_with_year = df.withColumn("year", year("registration_date"))
df_with_year.write.mode("overwrite").partitionBy("year").save("/Volumes/train_write/my_schema/my_volume/write_test5")

# COMMAND ----------

df2=spark.read.load("/Volumes/train_write/my_schema/my_volume/write_test5")
display(df2)

# COMMAND ----------

from pyspark.sql.functions import year

df_with_year = df.withColumn("year", year("registration_date"))
df_with_year.write.mode("overwrite").partitionBy("year").saveAsTable("train_write.my_schema.customers5")