# Databricks notebook source
data = [
    ("Order ID: 56789",),
    ("Invoice# 98321 processed",),
    ("No numbers here",),
    ("Ref-45A12B",)
]

df = spark.createDataFrame(data, ["text_col"])
df.createOrReplaceTempView("orders")
display(df)

# COMMAND ----------

import re

# Define Python function for extraction
def extract_number(text):
    if text is None:
        return None
    match = re.search(r"\d+", text)   # regex to find first sequence of digits
    return match.group(0) if match else None

# COMMAND ----------

spark.udf.register("extractNumberUDF", extract_number)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.SparkSession
# MAGIC import scala.util.matching.Regex
# MAGIC
# MAGIC // Scala function to extract first numeric sequence from text
# MAGIC def extractNumber(text: String): String = {
# MAGIC   if (text == null) return null
# MAGIC
# MAGIC   val pattern: Regex = "\\d+".r   // regex to find continuous digits
# MAGIC   pattern.findFirstIn(text) match {
# MAGIC     case Some(value) => value
# MAGIC     case None => null
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC // Register as UDF
# MAGIC spark.udf.register("extractNumberUDF", extractNumber _)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import expr

df2=df.withColumn("extracted_number", expr("extractNumberUDF(text_col)"))

display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT text_col,
# MAGIC          extractNumberUDF(text_col) AS extracted_number
# MAGIC   FROM orders