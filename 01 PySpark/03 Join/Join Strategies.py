# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Sample Employee info
data_employee = [
    Row(employee_id=1, name='Alice', city='New York'),
    Row(employee_id=2, name='Bob', city='Los Angeles'),
    Row(employee_id=3, name='Charlie', city='Chicago'),
    Row(employee_id=4, name='David', city='Houston'),
    Row(employee_id=5, name='Eva', city='Phoenix')
]

# Sample Salary info
data_salary = [
    Row(employee_id=1, salary=70000, deprt='HR'),
    Row(employee_id=2, salary=80000, deprt='Engineering'),
    Row(employee_id=3, salary=65000, deprt='Finance'),
    Row(employee_id=6, salary=90000, deprt='Marketing'),
    Row(employee_id=7, salary=75000, deprt='Sales')
]

spark = SparkSession.getActiveSession()
df_employee = spark.createDataFrame(data_employee)
df_salary = spark.createDataFrame(data_salary)

# Register temp views for SQL

# Employee info
df_employee.createOrReplaceTempView('employee')
# Salary info
df_salary.createOrReplaceTempView('salary')

display(df_employee)
display(df_salary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast Join Threshold

# COMMAND ----------

threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold") #default = 10MB
print(threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting Broadcast join threshold

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1gb")
threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Physical Plan

# COMMAND ----------

df_employee.join(df_salary, on='employee_id', how='inner').explain()

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN
# MAGIC SELECT *
# MAGIC FROM employee
# MAGIC INNER JOIN salary
# MAGIC ON employee.employee_id = salary.employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hint for broadcast join

# COMMAND ----------

df=df_employee.join(df_salary.hint("broadcast"), on='employee_id', how='inner')
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT /*+ BROADCAST(salary) */
# MAGIC   employee.*
# MAGIC , salary.salary
# MAGIC , salary.deprt
# MAGIC FROM employee
# MAGIC INNER JOIN salary
# MAGIC ON employee.employee_id = salary.employee_id