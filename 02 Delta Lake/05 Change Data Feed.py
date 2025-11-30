# Databricks notebook source
# MAGIC %sql
# MAGIC drop table training.delta_demo.people

# COMMAND ----------

# DBTITLE 1,Create Table
# MAGIC %sql
# MAGIC CREATE TABLE training.delta_demo.people (first_name STRING, age LONG) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Insert Data
df = spark.createDataFrame([("Bob", 23), ("Sue", 25), ("Jim", 27)]).toDF(
    "first_name", "age"
)
df.write.mode("append").format("delta").saveAsTable("training.delta_demo.people")

# COMMAND ----------

# DBTITLE 1,Show Data
# MAGIC %sql
# MAGIC select * from training.delta_demo.people

# COMMAND ----------

# DBTITLE 1,Delete Data
# MAGIC %sql
# MAGIC delete from training.delta_demo.people where first_name = 'Bob'

# COMMAND ----------

# DBTITLE 1,Update Data
# MAGIC %sql
# MAGIC update training.delta_demo.people set age=30 where first_name = 'Jim'

# COMMAND ----------

# DBTITLE 1,Read CDF
df=spark.read.format("delta")\
    .option("readChangeFeed", "true")\
    .option("startingVersion", 0)\
    .table("training.delta_demo.people")
display(df)

# COMMAND ----------

# DBTITLE 1,Alter Table Properties
# MAGIC %sql
# MAGIC ALTER TABLE training.delta_demo.people SET TBLPROPERTIES (delta.enableChangeDataFeed = true)