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
# MAGIC **Inner Join**
# MAGIC
# MAGIC An inner join returns only the rows where the join key exists in both dataframes. In our example, it will return employees who have salary information.

# COMMAND ----------

# DBTITLE 1,Inner Join - PySpark
df_inner = df_employee.join(df_salary, on='employee_id', how='inner')
display(df_inner)

# COMMAND ----------

# DBTITLE 1,Inner Join - SQL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee e
# MAGIC INNER JOIN salary s
# MAGIC ON e.employee_id = s.employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **Outer Join**
# MAGIC
# MAGIC An outer join returns all rows from both dataframes, matching rows where possible and filling with nulls where there is no match. This shows all employees and all salary records, even if there is no match.

# COMMAND ----------

# DBTITLE 1,Outer Join - PySpark
df_outer = df_employee.join(df_salary, on='employee_id', how='outer')
display(df_outer)

# COMMAND ----------

# DBTITLE 1,Outer Join - SQL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee e
# MAGIC FULL OUTER JOIN salary s
# MAGIC ON e.employee_id = s.employee_id;

# COMMAND ----------

# DBTITLE 1,SQL Left Outer Join Example
# MAGIC %md
# MAGIC **Left Outer Join**
# MAGIC
# MAGIC A left outer join returns all rows from the left dataframe (Employee info), and the matched rows from the right dataframe (Salary info). Unmatched rows from the left will have nulls for columns from the right.

# COMMAND ----------

# DBTITLE 1,Left Outer Join - PySpark
df_left_outer = df_employee.join(df_salary, on='employee_id', how='left_outer')
display(df_left_outer)

# COMMAND ----------

# DBTITLE 1,Left Outer Join - SQL
# MAGIC %sql
# MAGIC SELECT e.employee_id, e.name, e.city, s.salary, s.deprt
# MAGIC FROM employee e
# MAGIC LEFT OUTER JOIN salary s
# MAGIC ON e.employee_id = s.employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **Right Outer Join**
# MAGIC
# MAGIC A right outer join returns all rows from the right dataframe (Salary info), and the matched rows from the left dataframe (Employee info). Unmatched rows from the right will have nulls for columns from the left.

# COMMAND ----------

# DBTITLE 1,Right Outer Join - PySpark
df_right_outer = df_employee.join(df_salary, on='employee_id', how='right_outer')
display(df_right_outer)

# COMMAND ----------

# DBTITLE 1,Right Outer Join - SQL
# MAGIC %sql
# MAGIC SELECT s.employee_id, e.name, e.city, s.salary, s.deprt
# MAGIC FROM employee e
# MAGIC RIGHT OUTER JOIN salary s
# MAGIC ON e.employee_id = s.employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **Left Semi Join**
# MAGIC
# MAGIC A left semi join returns only the rows from the left dataframe (Employee info) where the join key exists in the right dataframe (Salary info). It does not include columns from the right dataframe.

# COMMAND ----------

# DBTITLE 1,Left Semi Join - PySpark
df_left_semi = df_employee.join(df_salary, on='employee_id', how='left_semi')
display(df_left_semi)

# COMMAND ----------

# DBTITLE 1,Left Semi Join - SQL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee e
# MAGIC LEFT SEMI JOIN salary s
# MAGIC ON e.employee_id = s.employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC **Left Anti Join**
# MAGIC
# MAGIC A left anti join returns only the rows from the left dataframe (Employee info) where the join key does NOT exist in the right dataframe (Salary info). It does not include columns from the right dataframe.

# COMMAND ----------

# DBTITLE 1,Left Anti Join  - PySpark
df_left_anti = df_employee.join(df_salary, on='employee_id', how='left_anti')
display(df_left_anti)

# COMMAND ----------

# DBTITLE 1,Left Anti Join - SQL
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employee e
# MAGIC LEFT ANTI JOIN salary s
# MAGIC ON e.employee_id = s.employee_id

# COMMAND ----------

# MAGIC %md
# MAGIC **Cross Join**
# MAGIC
# MAGIC A cross join returns the Cartesian product of the two dataframes, matching every row in the left dataframe with every row in the right dataframe. Use with caution as it can produce a large number of rows.

# COMMAND ----------

# DBTITLE 1,Cross Join - PySpark
df_cross = df_employee.crossJoin(df_salary)
display(df_cross)

# COMMAND ----------

# DBTITLE 1,Cross Join - SQL
# MAGIC %sql
# MAGIC SELECT * FROM employee CROSS JOIN salary

# COMMAND ----------

# MAGIC %md
# MAGIC **Handling Duplicate Rows and Same Column Names in Joins**
# MAGIC
# MAGIC When joining dataframes, duplicate rows can be removed using `dropDuplicates()`. If both dataframes have columns with the same name (other than join keys), PySpark automatically adds suffixes to distinguish them. You can also use `select` or `withColumnRenamed` to resolve column name conflicts.

# COMMAND ----------

# DBTITLE 1,Example: Remove Duplicates and Resolve Column Name Conflicts
# Add a duplicate row to df_employee for demonstration
df_employee_dup = df_employee.union(df_employee.filter(df_employee.employee_id == 1))
# Remove duplicates
df_employee_nodup = df_employee_dup.dropDuplicates()
# Join and resolve column name conflicts
joined = df_employee_nodup.join(df_salary, on='employee_id', how='inner')
# Rename columns if needed
joined = joined.withColumnRenamed('name', 'employee_name')
display(joined)