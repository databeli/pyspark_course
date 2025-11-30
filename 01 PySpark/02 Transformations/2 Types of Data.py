# Databricks notebook source
# DBTITLE 1,Create Dataframe
df=spark.read.table("samples.bakehouse.sales_transactions")
display(df)

# COMMAND ----------

# DBTITLE 1,Boolean - PySpark
# df2=df.where("paymentMethod =='mastercard'")
df2=df.where("paymentMethod !='mastercard'")
display(df2)

# COMMAND ----------

# DBTITLE 1,Boolean - SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_transactions where paymentMethod ='mastercard'
# MAGIC -- select * from samples.bakehouse.sales_transactions where paymentMethod !='mastercard'

# COMMAND ----------

# DBTITLE 1,Numbers - PySpark
from pyspark.sql.functions import expr, pow, round
# df2=df.withColumn("myCol",pow("quantity",2)/6)
df2=df.withColumn("myCol",round(pow("quantity",2)/6,2))
display(df2)

# COMMAND ----------

# DBTITLE 1,Numbers - SQL
# MAGIC %sql
# MAGIC select *,round(pow(quantity,2)/6,2) from samples.bakehouse.sales_transactions 

# COMMAND ----------

# DBTITLE 1,String - PySpark
df2=df.select("product").\
withColumn("lower_product",expr("lower(product)")).\
withColumn("upper_product",expr("upper(product)")).\
withColumn("trim_product",expr("trim(product)")).\
withColumn("substr_product",expr("substr(product,0,4)")).\
withColumn("split_product",expr("split(product,' ')"))
display(df2)

# COMMAND ----------

# DBTITLE 1,String - SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC         product,
# MAGIC         lower(product) AS lower_product,
# MAGIC         upper(product) AS upper_product,
# MAGIC         trim(product) AS trim_product,
# MAGIC         substr(product, 0, 4) AS substr_product,
# MAGIC         split(product, ' ') AS split_product
# MAGIC     FROM samples.bakehouse.sales_transactions

# COMMAND ----------

# DBTITLE 1,RegEx - PySpark
from pyspark.sql.functions import regexp_extract, regexp_replace
df2=df.select("product").\
withColumn("regexp_extract_product", expr("regexp_extract(product, r'^\S+\s+(\S+)\s+\S+$', 1)")).\
withColumn("regexp_replace_product", expr("regexp_replace(product, r'^\S+\s+(\S+)\s+\S+$', 'AAAA')"))
display(df2)

# COMMAND ----------

# DBTITLE 1,RegEx - SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     product,
# MAGIC     regexp_extract(product, r'^\S+\s+(\S+)\s+\S+$', 1) AS regexp_extract_product,
# MAGIC     regexp_replace(product, r'^\S+\s+(\S+)\s+\S+$', 'AAAA') AS regexp_replace_product
# MAGIC   FROM samples.bakehouse.sales_transactions

# COMMAND ----------

# DBTITLE 1,Date and Timestamp - PySpark
from pyspark.sql.functions import current_date, current_timestamp, to_date,date_add,date_sub,lit,to_timestamp
df2 = df.select("dateTime")\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())\
.withColumn("dateTime_date", to_date("dateTime"))\
.withColumn("dateTime_date_add", date_add(to_date("dateTime"),5))\
.withColumn("dateTime_date_sub", date_sub(to_date("dateTime"),5))\
.withColumn("custom_date", to_date(lit("20251014"),'yyyyMMdd'))\
.withColumn("custom_timestamp", to_timestamp(lit("20251014112233"),'yyyyMMddHHmmss'))
display(df2)

# COMMAND ----------

# DBTITLE 1,Date and Timestamp - SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   dateTime,
# MAGIC   current_date() AS today,
# MAGIC   current_timestamp() AS now,
# MAGIC   to_date(dateTime) AS dateTime_date,
# MAGIC   date_add(to_date(dateTime), 5) AS dateTime_date_add,
# MAGIC   date_sub(to_date(dateTime), 5) AS dateTime_date_sub,
# MAGIC   to_date('20251014', 'yyyyMMdd') AS custom_date,
# MAGIC   to_timestamp('20251014112233', 'yyyyMMddHHmmss') AS custom_timestamp
# MAGIC FROM samples.bakehouse.sales_transactions

# COMMAND ----------

# DBTITLE 1,Nulls - PySpark
from pyspark.sql.functions import coalesce, expr
df2 = df.select("transactionID","customerID")\
.withColumn("coalesce_id", coalesce("transactionID", "customerID"))\
.withColumn("if_else", expr("CASE WHEN transactionID IS NOT NULL THEN transactionID ELSE customerID END"))\
.na.drop(subset=["transactionID"])
# .na.drop('all')
# .na.fill('myvalue')
display(df2)

# COMMAND ----------

# DBTITLE 1,Nulls - SQL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC         transactionID,
# MAGIC         customerID,
# MAGIC         coalesce(transactionID, customerID) AS coalesce_id,
# MAGIC         CASE WHEN transactionID IS NOT NULL THEN transactionID ELSE customerID END AS if_else
# MAGIC     FROM samples.bakehouse.sales_transactions