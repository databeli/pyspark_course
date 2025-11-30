# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE training.delta_demo.sales_partitioned (
# MAGIC     order_id        STRING,
# MAGIC     order_date      DATE,
# MAGIC     country         STRING,
# MAGIC     category        STRING,
# MAGIC     amount          DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (country, category);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO training.delta_demo.sales_partitioned VALUES
# MAGIC ("1", "2024-01-01", "USA", "Electronics", 1200.50),
# MAGIC ("2", "2024-01-02", "INDIA", "Clothing", 450.00),
# MAGIC ("3", "2024-01-03", "USA", "Clothing", 180.75),
# MAGIC ("4", "2024-01-01", "UK", "Furniture", 899.00);
# MAGIC

# COMMAND ----------

df=spark.read.table("training.delta_demo.sales_partitioned")
df.write.partitionBy("country", "category").save("/Volumes/training/delta_demo/my_volume/sales_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace TABLE training.delta_demo.sales_clustered(
# MAGIC     order_id        STRING,
# MAGIC     order_date      DATE,
# MAGIC     country         STRING,
# MAGIC     category        STRING,
# MAGIC     amount          DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (country, category, order_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO training.delta_demo.sales_clustered VALUES
# MAGIC ("1", "2024-01-01", "USA", "Electronics", 1200.50),
# MAGIC ("2", "2024-01-02", "INDIA", "Clothing", 450.00),
# MAGIC ("3", "2024-01-03", "USA", "Clothing", 180.75),
# MAGIC ("4", "2024-01-01", "UK", "Furniture", 899.00);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize training.delta_demo.sales_clustered