# Databricks notebook source
# MAGIC %md
# MAGIC # Complex Types in PySpark
# MAGIC
# MAGIC This notebook demonstrates how to work with complex data types in PySpark, including arrays, structs, and JSON. Each section provides code examples and explanations to help you understand and use these types effectively in your data processing workflows.
# MAGIC
# MAGIC **Notebook structure:**
# MAGIC * Introduction
# MAGIC * Creating a DataFrame with complex types
# MAGIC * ArrayType operations (split, length, contains, explode)
# MAGIC * StructType with arrays
# MAGIC * JSON operations (get_json_object, from_json)
# MAGIC * Summary and key takeaways

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a DataFrame with Complex Types
# MAGIC
# MAGIC Let's start by creating a sample DataFrame that includes various complex types:
# MAGIC * `array` (list of values)
# MAGIC * `struct` (nested fields)
# MAGIC * `json` (stringified JSON)
# MAGIC
# MAGIC This DataFrame will be used in subsequent examples to demonstrate operations on these types.

# COMMAND ----------

# DBTITLE 1,Create and Display DataFrame with Complex Types
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, IntegerType
from pyspark.sql import functions as F

# Sample data
rows = [
    Row(
        id=1,
        names=["Alice", "Bob"],
        scores=[85, 90],
        info=Row(age=25, city="New York"),
        json_str='{"dept": "Engineering", "level": 2}'
    ),
    Row(
        id=2,
        names=["Carol"],
        scores=[78, 88, 92],
        info=Row(age=30, city="San Francisco"),
        json_str='{"dept": "HR", "level": 1}'
    )
]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("names", ArrayType(StringType()), True),
    StructField("scores", ArrayType(IntegerType()), True),
    StructField("info", StructType([
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("json_str", StringType(), True)
])

df = spark.createDataFrame(rows, schema)
df.createOrReplaceTempView("df")
display(df)

# COMMAND ----------

# DBTITLE 1,PySpark - Array Operations
from pyspark.sql import functions as F

# Get the length of the 'names' array
array_length_df = df.withColumn("names_length", F.size(F.col("names")))

# Check if 'names' array contains 'Alice'
array_contains_df = array_length_df.withColumn("has_Alice", F.array_contains(F.col("names"), "Alice"))

# Split a string column into an array (for demonstration, create a new column)
df_array = array_contains_df.withColumn("city_words", F.split(F.col("info.city"), " "))

display(df_array.select("id", "names", "names_length", "has_Alice", "city_words"))

# COMMAND ----------

# DBTITLE 1,SQL - Array Operations
# MAGIC %sql
# MAGIC SELECT
# MAGIC   id,
# MAGIC   names,
# MAGIC   size(names) AS names_length,
# MAGIC   array_contains(names, 'Alice') AS has_Alice,
# MAGIC   split(info.city, ' ') AS city_words
# MAGIC FROM df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploding Arrays
# MAGIC
# MAGIC The `explode` function creates a new row for each element in an array column. This is useful for flattening nested data.

# COMMAND ----------

# DBTITLE 1,PySpark - Explode Array
exploded_df = df.select("id", F.explode(F.col("names")).alias("name"))
display(exploded_df)

# COMMAND ----------

# DBTITLE 1,SQL - Explode Array
# MAGIC %sql
# MAGIC SELECT id, explode(names) AS name FROM df

# COMMAND ----------

# DBTITLE 1,PySpark - StructType
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Accessing fields in the 'info' struct
struct_df = df.withColumn("age", F.col("info.age")).withColumn("city", F.col("info.city"))

display(struct_df)

# COMMAND ----------

# DBTITLE 1,SQL - Struct Type
# MAGIC %sql
# MAGIC SELECT *, info.age AS age, info.city AS city
# MAGIC     FROM df

# COMMAND ----------

# DBTITLE 1,PySpark - Json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Extract a value using get_json_object
json_df = df.withColumn("dept", F.get_json_object(F.col("json_str"), "$.dept")) \
    .withColumn("level", F.get_json_object(F.col("json_str"), "$.level"))

display(json_df)

# COMMAND ----------

# DBTITLE 1,SQL - JSON
# MAGIC %sql
# MAGIC SELECT *,
# MAGIC        get_json_object(json_str, '$.dept') AS dept,
# MAGIC        get_json_object(json_str, '$.level') AS level
# MAGIC FROM df

# COMMAND ----------

# DBTITLE 1,PySpark - Json to Struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Parse JSON string into struct
json_schema = StructType([
    StructField("dept", StringType(), True),
    StructField("level", IntegerType(), True)
])
from_json_df = df.withColumn("json_struct", F.from_json(F.col("json_str"), json_schema))
# Accessing fields in the 'info' struct
struct_df = from_json_df.withColumn("dept", F.col("json_struct.dept"))

display(struct_df)

# COMMAND ----------

# DBTITLE 1,SQL - JSON to Struct
# MAGIC %sql
# MAGIC SELECT *,
# MAGIC            from_json(json_str, 'STRUCT<dept:STRING,level:INT>') AS json_struct,
# MAGIC            from_json(json_str, 'STRUCT<dept:STRING,level:INT>').dept AS dept
# MAGIC     FROM df