# Databricks notebook source
# MAGIC %md
# MAGIC # DQX - The Data Quality Framework

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQX Deployment and Usage Options

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are different deployment and usage options for DQX:
# MAGIC
# MAGIC | Item | Option 1 | Option 2|
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | Installation| Deploy as a Library | Deploy as a workspace tool |
# MAGIC | Usage | Use with Spark Core or Spark Structure Streaming| Use with Lakeflow Pipelines (formerly DLT) |
# MAGIC | Quality Rules| Define as YAML/JSON | Define as Code |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX as Library <br>
# MAGIC For this demo, we will install DQX as library and define quality rules as YAML.

# COMMAND ----------

# DBTITLE 1,Install DQX Library
# MAGIC
# MAGIC %pip install databricks-labs-dqx
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Set Catalog and Schema for Demo Dataset
default_database_name = "main"
default_schema_name = "default"

dbutils.widgets.text("demo_database_name", default_database_name, "Catalog Name")
dbutils.widgets.text("demo_schema_name", default_schema_name, "Schema Name")

database = dbutils.widgets.get("demo_database_name")
schema = dbutils.widgets.get("demo_schema_name")

print(f"Selected Catalog for Demo Dataset: {database}")
print(f"Selected Schema for Demo Dataset: {schema}")

sensor_table = f"{database}.{schema}.sensor_data"
maintenance_table = f"{database}.{schema}.maintenance_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the datasets
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Sensor Bronze Tables

sensor_bronze_df = spark.read.table(sensor_table)
print("=== Sensor Data Sample ===")
display(sensor_bronze_df.limit(10))

# COMMAND ----------

mntnc_bronze_df = spark.read.table(maintenance_table)
print("=== Maintenance Data Sample ===")
display(mntnc_bronze_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Common Imports
import yaml
from pprint import pprint

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig, TableChecksStorageConfig

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Problem - Team doesn't know the Quality Rules for Maintenance Dataset
# MAGIC ### Feature - Infer the Data Quality Rules using DQX

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-1**. Read Raw Data and Instantiate DQX 

# COMMAND ----------

# Read Input Data
mntnc_bronze_df = spark.read.table(maintenance_table)

# Instantiate DQX engine
ws = WorkspaceClient()
dq_engine = DQEngine(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-2**. Run DQX Profiler

# COMMAND ----------

# Profile Inpute Data
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(mntnc_bronze_df)
display(summary_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-3** **Infer** Quality Rules

# COMMAND ----------

# Profile Inpute Data
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(mntnc_bronze_df)

# Generate DQX quality rules/checks
generator = DQGenerator(ws)
maintenance_checks = generator.generate_dq_rules(profiles)  

# COMMAND ----------

# Generate DQX quality rules/checks
generator = DQGenerator(ws)
maintenance_checks = generator.generate_dq_rules(profiles)  # with default level "error"

# COMMAND ----------

# DBTITLE 1,Review the Inferred checks
print("=== Inferred DQ Checks ===\n")

for idx, check in enumerate(maintenance_checks):
   print(f"========Check {idx} ==========\n")
   pprint(check)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Step-3**. Apply Inferred Quality Rules to Input Data

# COMMAND ----------

# Apply checks on input data
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(mntnc_bronze_df, maintenance_checks)

print("=== Maintenance Bad Data Sample ===")
display(quarantined_df)

# COMMAND ----------

display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Quality Rules for Machine Sensor Data
# MAGIC
# MAGIC | Rule Type             | Example Rule                                                                                           | Purpose / Impact                                                        |DQ Rule|Quality Error Level|
# MAGIC |-----------------------|-------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|-|--|
# MAGIC | **Completeness**      | Required fields (`sensor_id`, `machine_id`) must not be null    | Ensures all critical data is present and usable     |`is_not_null_and_not_empty`                    |ERROR|
# MAGIC | **Range / Domain**    | `reading_value` (temperature): 0–100 | Detects outliers and sensor faults; ensures physical plausibility       |**FILTER quality Check + `is_in_range`**| WARN|
# MAGIC | **Format Standardization**  |  `machine_id` follows standard format                             | Standardizes data for integration and analysis                          |`regex_match` |WARN|
# MAGIC | **Timeliness**        | `reading_timestamp` is not in the future; beyond 3 days                                     | Prevents erroneous time-series data                            |`is_not_in_future` |ERROR|
# MAGIC | **Correctness**        | `calibration_date` is eariler than `reading_timestamp`| Prevents erroneous sesnor readings data                            |`SQL Expression` |ERROR|
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sensor Dataset Quality Rules YAML

# COMMAND ----------

# DBTITLE 1,Sensor Dataset Quality Rules YAML
sensor_dq_checks = yaml.safe_load("""
# Completeness Check on 2 columns
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
    - sensor_id
    - machine_id
        
# Filter + Range Based Check
- criticality: warn
  filter: sensor_type = 'temperature'
  check:
    function: is_in_range
    arguments:
      column: reading_value
      min_limit: 0
      max_limit: 100

# Regex Based Check
- criticality: warn
  check:
    function: regex_match
    arguments:
      column: machine_id
      regex: '^MCH-\\d{3}$'

# timeliness check
- criticality: error
  check:
    function: is_not_in_future
    arguments:
      column: reading_timestamp
      offset: 259200

# sql_expression check
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: (calibration_date > date(reading_timestamp))
      msg: Sensor calibration_date is later than sensor reading_timestamp
      name: calib_dt_gt_reading_ts
      negate: true
""")

# validate the checks
status = DQEngine.validate_checks(sensor_dq_checks)
print(status)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine Bad Data & Perform Granular Issue Detection

# COMMAND ----------

# DBTITLE 1,Apply quality checks defined in YAML
# read sensor data
sensor_bronze_df = spark.read.table(sensor_table)

# Load quality rules from YAML file
# sensor_dq_checks = dq_engine.load_checks(config=WorkspaceFileChecksStorageConfig(location=sensor_dq_rules_yaml))

# Apply checks on input data
valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(sensor_bronze_df, sensor_dq_checks)

print("=== Bad Data DF ===")
display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do not Quarantine Bad Data & Add extra columns

# COMMAND ----------

df = dq_engine.apply_checks_by_metadata(sensor_bronze_df, sensor_dq_checks)
display(df)