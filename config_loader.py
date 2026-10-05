# Databricks notebook source
# MAGIC %md
# MAGIC ## Layer 1 config loader — task 1
# MAGIC
# MAGIC Reads the approved mapping for one Keka or NetSuite source table from Lakebase
# MAGIC and hands it to `layer1_etl_job` as the `etl_config` task value.
# MAGIC
# MAGIC Run on **serverless**. Task key must be `load_config`.

# COMMAND ----------

# MAGIC %pip install "pg8000>=1.30" "sqlalchemy>=2.0"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run "./utils/lakebase_config"

# COMMAND ----------

# MAGIC %run "./utils/transformation_functions"

# COMMAND ----------

import json

dbutils.widgets.text("sourceTableName", "")
dbutils.widgets.text("sourceCatalogName", "thoughtfocus_nsk")
dbutils.widgets.text("sourceSchemaName", "keka_ns_csv")
dbutils.widgets.text("config_schema", "app")

sourceTableName = dbutils.widgets.get("sourceTableName").strip()
sourceCatalogName = dbutils.widgets.get("sourceCatalogName").strip()
sourceSchemaName = dbutils.widgets.get("sourceSchemaName").strip()
configSchema = dbutils.widgets.get("config_schema").strip()

if not sourceTableName:
    raise ValueError("sourceTableName is required")
if not sourceCatalogName:
    raise ValueError("sourceCatalogName is required")
if not sourceSchemaName:
    raise ValueError("sourceSchemaName is required")
if not configSchema:
    raise ValueError("config_schema is required")

print(f"sourceTableName   : {sourceTableName}")
print(f"sourceCatalogName : {sourceCatalogName}")
print(f"sourceSchemaName  : {sourceSchemaName}")
print(f"config_schema     : {configSchema}")

# COMMAND ----------

etl_config = load_etl_config(
    sourceTableName,
    source_catalog=sourceCatalogName,
    source_schema=sourceSchemaName,
    schema=configSchema,
)

# COMMAND ----------

payload = json.dumps(etl_config, ensure_ascii=False)
taskValue = encode_task_value(payload, label=sourceTableName)
dbutils.jobs.taskValues.set(key="etl_config", value=taskValue)
print(
    f"etl_config published ({len(payload)} chars, "
    f"{len(etl_config['columnMapping'])} columns, "
    f"{task_value_stored_size(taskValue)} of {48 * 1024} bytes stored"
    f"{', gzip+base64' if task_value_is_compressed(taskValue) else ''})"
)
print(payload)
