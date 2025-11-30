# Databricks notebook source
# DBTITLE 1,Create DataFrame
df=spark.read.table("samples.bakehouse.sales_franchises")
display(df)

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Get Columns - PySpark
df.columns

# COMMAND ----------

# DBTITLE 1,Get Column - SQL
# MAGIC %sql
# MAGIC SHOW COLUMNS IN samples.bakehouse.sales_franchises;

# COMMAND ----------

# DBTITLE 1,Select - PySpark
df2=df.select("name","country")
display(df2)

# COMMAND ----------

# DBTITLE 1,Select - SQL
# MAGIC %sql
# MAGIC select name,country from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Expr - PySpark
from pyspark.sql.functions import expr
# df2=df.select("name",expr("country as my_country"))
df2=df.select("name",expr("concat(city,'_',country) as location"))
display(df2)

# COMMAND ----------

# DBTITLE 1,SelectExpr - PySprak
from pyspark.sql.functions import expr
df2=df.selectExpr("name","country as my_country")
display(df2)

# COMMAND ----------

# DBTITLE 1,Alias - PySpark
from pyspark.sql.functions import expr
df2=df.select("name",expr("concat(city,'_',country)").alias("location"))
display(df2)

# COMMAND ----------

# DBTITLE 1,Expr,Alias - SQL
# MAGIC %sql
# MAGIC select name,concat(city,'_',country) as location from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Add New Column - PySpark
df2=df.withColumn("location",expr("concat(city,'_',country)"))
display(df2)

# COMMAND ----------

# DBTITLE 1,Add new Column - SQL
# MAGIC %sql
# MAGIC select *,concat(city,'_',country) as location from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Constant Value - PySaprk
from pyspark.sql.functions import lit
df2=df.withColumn("one",lit(1))
display(df2)

# COMMAND ----------

# DBTITLE 1,Constant - SQL
# MAGIC %sql
# MAGIC select *,1 as one from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Drop Column - PySpark
df2=df.drop("country")
display(df2)

# COMMAND ----------

# DBTITLE 1,Drop Column - SQL
# MAGIC %sql
# MAGIC select * except(country)  from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Cast - PySpark
from pyspark.sql.functions import col
df2=df.withColumn("supplier_id_double", col("supplierID").cast("double"))
display(df2)

# COMMAND ----------

# DBTITLE 1,Cast - SQL
# MAGIC %sql
# MAGIC select *,cast(supplierID as double) as supplier_id_double  from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Filter - PySpark
df2=df.filter(col("country")=='US')
# df2=df.where("country =='US'")
display(df2)

# COMMAND ----------

# DBTITLE 1,Filter - SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises where country='US'

# COMMAND ----------

# DBTITLE 1,Distinct - PySpark
df2=df.distinct()
# df2=df.select("country").distinct()
# df2=df.select("country","city").distinct()
display(df2)

# COMMAND ----------

# DBTITLE 1,Distinct - SQL
# MAGIC %sql
# MAGIC -- select distinct * from samples.bakehouse.sales_franchises;
# MAGIC select distinct country from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Union - PySpark
df2=df
display(df)
display(df2)

# COMMAND ----------

df3=df.union(df2)
display(df3)

# COMMAND ----------

df3=df.unionAll(df2)
display(df3)

# COMMAND ----------

# DBTITLE 1,Union - SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises
# MAGIC -- union
# MAGIC union all
# MAGIC select * from samples.bakehouse.sales_franchises;

# COMMAND ----------

# DBTITLE 1,OrderBy - PySpark
from pyspark.sql.functions import desc
# df2=df.orderBy("country")
df2=df.orderBy(col("country").desc())
display(df2)

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises order by country
# MAGIC desc

# COMMAND ----------

# DBTITLE 1,Limit - PySpark
df2=df.limit(10)
display(df2)

# COMMAND ----------

# DBTITLE 1,Limit - SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises limit 10

# COMMAND ----------

# DBTITLE 1,Repartition and Coalesce
df2=df.repartition(2)
# df2=df.coalesce(2)
display(df2)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Collect
df.collect()

# COMMAND ----------

# DBTITLE 1,TempView - PySpark
df.limit(5).createOrReplaceTempView("myview")

# COMMAND ----------

# DBTITLE 1,TempView - SQL
# MAGIC %sql
# MAGIC select * from myview

# COMMAND ----------

# DBTITLE 1,SQL to Dataframe
df2=spark.sql("select * from myview limit 2")
display(df2)