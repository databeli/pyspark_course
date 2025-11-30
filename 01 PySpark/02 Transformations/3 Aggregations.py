# Databricks notebook source
# DBTITLE 1,Create Dataframe
df=spark.read.table("samples.bakehouse.sales_transactions")
display(df)

# COMMAND ----------

# DBTITLE 1,PySpark -Count
df.count()

# COMMAND ----------

# DBTITLE 1,PySpark - First and Last
df.first()
# df.last()

# COMMAND ----------

# DBTITLE 1,PySpark - Min - Max
from  pyspark.sql.functions import min, max
df2=df.select(min("quantity"), max("quantity"))
display(df2)

# COMMAND ----------

# DBTITLE 1,PySpark - Sum and Average
from  pyspark.sql.functions import sum, avg
df2=df.select(sum("quantity"), avg("quantity"))
display(df2)

# COMMAND ----------

# DBTITLE 1,SQL - Count, Min, Max, Average, Sum
# MAGIC %sql
# MAGIC select 
# MAGIC   count(*),
# MAGIC   min(quantity),
# MAGIC   max(quantity),
# MAGIC   avg(quantity),
# MAGIC   sum(quantity)
# MAGIC from samples.bakehouse.sales_transactions

# COMMAND ----------

# DBTITLE 1,PySpark - Group By
df2=df.groupBy("paymentMethod").count()
display(df2)

# COMMAND ----------

# DBTITLE 1,SQL Group by
# MAGIC %sql
# MAGIC select
# MAGIC   paymentMethod,
# MAGIC   count(*),
# MAGIC   avg(quantity)
# MAGIC from samples.bakehouse.sales_transactions
# MAGIC   group by paymentMethod
# MAGIC
# MAGIC  having count(*) > 1100

# COMMAND ----------

# DBTITLE 1,PySpark - Group by - Having
from pyspark.sql.functions import col
df2 = df.groupBy("paymentMethod").count().filter(col("count") > 1100)
display(df2)

# COMMAND ----------

# DBTITLE 1,PySpark - Collect Set and List
from pyspark.sql.functions import collect_set, collect_list
df2=df.groupBy("paymentMethod").agg(collect_set("product"), collect_list("product"))
display(df2)

# COMMAND ----------

# DBTITLE 1,SQL - Collect Set and List
# MAGIC %sql
# MAGIC select 
# MAGIC   paymentMethod,
# MAGIC   collect_set(product),
# MAGIC   collect_list(product)
# MAGIC from samples.bakehouse.sales_transactions
# MAGIC group by paymentMethod

# COMMAND ----------

# DBTITLE 1,PySpark - Window Function - Row Number
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("product").orderBy(df["quantity"].desc())
df2 = df.withColumn("row_number_over_product", row_number().over(window_spec))
df3=df2.select("product","customerID","quantity","row_number_over_product")
display(df3)

# COMMAND ----------

df4=df3.filter(df3["row_number_over_product"]==1)
display(df4)

# COMMAND ----------

# DBTITLE 1,PySpark - Window Function - Rank, Dense Rank
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

window_spec = Window.partitionBy("product").orderBy(df["quantity"].desc())
df2 = df.withColumn("row_number_over_product", row_number().over(window_spec)) \
    .withColumn("rank_over_product", rank().over(window_spec)) \
    .withColumn("dense_rank_over_product", dense_rank().over(window_spec)) 
df3 = df2.select(
    "product",
    "customerID",
    "quantity",
    "row_number_over_product",
    "rank_over_product",
    "dense_rank_over_product"
)
display(df3)

# COMMAND ----------

# DBTITLE 1,SQL - Window Function - Row Number, Rank, Dense Rank
# MAGIC %sql
# MAGIC SELECT
# MAGIC   product,
# MAGIC   customerID,
# MAGIC   quantity,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY product ORDER BY quantity DESC) AS row_number_over_product,
# MAGIC   RANK() OVER (PARTITION BY product ORDER BY quantity DESC) AS rank_over_product,
# MAGIC   DENSE_RANK() OVER (PARTITION BY product ORDER BY quantity DESC) AS dense_rank_over_product
# MAGIC FROM samples.bakehouse.sales_transactions