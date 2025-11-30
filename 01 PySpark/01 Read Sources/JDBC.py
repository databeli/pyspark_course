# Databricks notebook source
from pyspark.sql import SparkSession

# JDBC connection properties
jdbc_url = "jdbc:databricks://<your url>:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ac27879dd8a7975e;"

access_token = "Your Token"

# SQL query or table
query = "(SELECT * FROM samples.bakehouse.sales_franchises LIMIT 100) AS t"

# Read data
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", query) \
    .option("user", "token") \
    .option("password", access_token) \
    .load()

display(df)


# COMMAND ----------

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "samples.bakehouse.sales_franchises") \
    .option("user", "token") \
    .option("password", access_token) \
    .load()
display(df)