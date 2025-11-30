# Databricks notebook source
# MAGIC %md
# MAGIC This cell creates a sample employee DataFrame using PySpark, registers it as a temporary view named `employee`, and displays the employee data. This setup is used for further operations in the notebook.

# COMMAND ----------

# DBTITLE 1,Create Sample Dataframe
from pyspark.sql import Row

# Sample Employee info
data_employee = [
    Row(employee_id=1, name='Alice', city='New York'),
    Row(employee_id=2, name='Bob', city='Los Angeles'),
    Row(employee_id=3, name='Charlie', city='Chicago'),
    Row(employee_id=4, name='David', city='Houston'),
    Row(employee_id=5, name='Eva', city='Phoenix')
]

df_employee = spark.createDataFrame(data_employee)

# Employee info
df_employee.createOrReplaceTempView('employee')

display(df_employee)

# COMMAND ----------

# MAGIC %md
# MAGIC This cell explains that the following SQL code creates a new table 'employee_target_table' in the specified schema, populating it with employee data and adding insert and update timestamps.

# COMMAND ----------

# DBTITLE 1,Create Target Table
# MAGIC %sql
# MAGIC create or replace table test.test_schema.employee_target_table
# MAGIC as
# MAGIC select
# MAGIC   *,
# MAGIC   current_timestamp() as insert_timestamp,
# MAGIC   current_timestamp() as update_timestamp
# MAGIC from employee

# COMMAND ----------

# MAGIC %md
# MAGIC This cell explains that the following SQL code queries all records from the 'employee_target_table' to show its current contents.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.test_schema.employee_target_table

# COMMAND ----------

# DBTITLE 1,Create New Dataframe with source data
from pyspark.sql import Row

# Sample Employee info
data_employee = [
    Row(employee_id=1, name='Alice', city='Phoenix'),
    Row(employee_id=2, name='Bob', city='Houston'),
    Row(employee_id=6, name='Alfred', city='Phoenix'),
    Row(employee_id=7, name='Kate', city='Los Angeles')
]

df_employee = spark.createDataFrame(data_employee)

# Employee info
df_employee.createOrReplaceTempView('employee')

display(df_employee)

# COMMAND ----------

# MAGIC %md
# MAGIC This cell explains that the following SQL code performs a MERGE operation to update existing records and insert new ones into 'employee_target_table' based on the latest employee data.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Merge with custom columns
# MAGIC %sql
# MAGIC MERGE INTO test.test_schema.employee_target_table target
# MAGIC USING employee source
# MAGIC ON target.employee_id=source.employee_id 
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.city = source.city,
# MAGIC     target.update_timestamp = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (employee_id,name,city,insert_timestamp,update_timestamp)
# MAGIC   VALUES (
# MAGIC     source.employee_id,
# MAGIC     source.name,
# MAGIC     source.city,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC This cell explains that the following SQL code queries the 'employee_target_table' to show the results after the merge operation.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.test_schema.employee_target_table

# COMMAND ----------

# DBTITLE 1,Merge and Delete with Custom Columns
# MAGIC %sql
# MAGIC MERGE INTO test.test_schema.employee_target_table target
# MAGIC USING employee source
# MAGIC ON target.employee_id=source.employee_id 
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.city = source.city,
# MAGIC     target.update_timestamp = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (employee_id,name,city,insert_timestamp,update_timestamp)
# MAGIC   VALUES (
# MAGIC     source.employee_id,
# MAGIC     source.name,
# MAGIC     source.city,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC   )
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC This cell explains that the following SQL code queries the 'employee_target_table' to show the results after the merge and delete operation.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.test_schema.employee_target_table

# COMMAND ----------

from pyspark.sql import functions as F
df_employee.withColumn('insert_timestamp', F.current_timestamp()).withColumn('update_timestamp', F.current_timestamp()).createOrReplaceTempView('employee')

# COMMAND ----------

# DBTITLE 1,Merge wt
# MAGIC %sql
# MAGIC MERGE INTO test.test_schema.employee_target_table target
# MAGIC USING employee source
# MAGIC ON target.employee_id=source.employee_id 
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC   
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC This cell explains that the following SQL code queries the 'employee_target_table' to show its final state after all merge operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.test_schema.employee_target_table

# COMMAND ----------

target_table_name="test.test_schema.employee_target_table target"
merge_query=f"""
MERGE INTO {target_table_name}
USING employee source
ON target.employee_id=source.employee_id 

WHEN MATCHED THEN
  UPDATE SET *

WHEN NOT MATCHED THEN
  INSERT *
  
WHEN NOT MATCHED BY SOURCE THEN
  DELETE
"""
spark.sql(merge_query)