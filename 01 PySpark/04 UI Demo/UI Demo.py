# Databricks notebook source

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
    Row(employee_id=2, salary=80000, deprt='HR'),
    Row(employee_id=3, salary=65000, deprt='Marketing'),
    Row(employee_id=6, salary=90000, deprt='Marketing'),
    Row(employee_id=7, salary=75000, deprt='Marketing')
]

df_employee = spark.createDataFrame(data_employee)
df_salary = spark.createDataFrame(data_salary)

# Register temp views for SQL

# Employee info
df_employee.createOrReplaceTempView('employee')
# Salary info
df_salary.createOrReplaceTempView('salary')

# COMMAND ----------

df_inner = df_employee.join(df_salary, on='employee_id', how='inner')

# COMMAND ----------

df_inner.show()