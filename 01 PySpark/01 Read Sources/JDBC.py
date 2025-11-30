# Databricks notebook source
from pyspark.sql import SparkSession

# JDBC connection properties
jdbc_url = "jdbc:databricks://dbc-3f1c04d0-60d3.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ac27879dd8a7975e;"

access_token = "dapi5a37ec288ed16b28e613e5bd00a24869"

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