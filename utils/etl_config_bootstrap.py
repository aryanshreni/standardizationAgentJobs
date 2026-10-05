# Databricks notebook source
# MAGIC %md
# MAGIC ## ETL config bootstrap
# MAGIC
# MAGIC `%run` this after `./utils/transformation_functions`. It reads the `etl_config`
# MAGIC task value from `load_config` and sets the names the silver notebook uses.

# COMMAND ----------

import json

_missing = [n for n in ("wrap_string", "build_etl_metadata_columns", "decode_task_value")
            if n not in dir()]
if _missing:
    raise NameError(
        f"{', '.join(_missing)} not defined — %run \"./utils/transformation_functions\" first"
    )

# COMMAND ----------

dbutils.widgets.text("sourceTableName", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("debug_etl_config", "")

requestedSourceTable = dbutils.widgets.get("sourceTableName").strip()
JobRun_ID = dbutils.widgets.get("job_run_id")

_rawEtlConfig = dbutils.jobs.taskValues.get(
    taskKey="load_config",
    key="etl_config",
    debugValue=dbutils.widgets.get("debug_etl_config").strip(),
)
if not (_rawEtlConfig or "").strip():
    raise ValueError(
        "no 'etl_config' task value from task 'load_config' — the Job must have a task "
        "named exactly load_config running config_loader on serverless"
    )

etlConfig = json.loads(decode_task_value(_rawEtlConfig))
if requestedSourceTable and requestedSourceTable.lower() != etlConfig["sourceTableName"].lower():
    raise ValueError(
        f"sourceTableName widget is '{requestedSourceTable}' but load_config "
        f"resolved '{etlConfig['sourceTableName']}'"
    )

sourceCatalogName = wrap_string(etlConfig["sourceCatalogName"], "`")
sourceSchemaName = wrap_string(etlConfig["sourceSchemaName"], "`")
sourceTableName = wrap_string(etlConfig["sourceTableName"], "`")
targetCatalogName = wrap_string(etlConfig["targetCatalogName"], "`")
targetSchemaName = wrap_string(etlConfig["targetSchemaName"], "`")
targetTableName = wrap_string(etlConfig["targetTableName"], "`")
sourceSystem = etlConfig["sourceSystem"]
linkKeyName = etlConfig["linkKeyName"]
linkKeyColumns = etlConfig["linkKeyColumns"]
columnMapping = etlConfig["columnMapping"]
auditTableName = etlConfig["auditTableName"].strip()
cycleIdentifierColumn = etlConfig.get("cycleIdentifierColumn") or ""
resolvedTargetTable = etlConfig["targetTableName"].strip()
surrogateKeyColumnName = etlConfig.get("surrogateKeyColumnName") or "silver_key"
stagingTableName = "staging_" + resolvedTargetTable
tableDescription = (etlConfig.get("tableDescription") or "").strip()
tableTags = [t.strip() for t in (etlConfig.get("tableTags") or "").split(",") if t.strip()]

for _m in columnMapping:
    if not (_m.get("description") or "").strip():
        _m["description"] = (_m.get("column_desc") or "").strip()
    raw_tags = _m.get("tags") or ""
    if isinstance(raw_tags, list):
        _m["tags"] = [str(t).strip() for t in raw_tags if str(t).strip()]
    else:
        raw_tags = str(raw_tags).strip()
        if raw_tags and raw_tags[0] == "[":
            try:
                _m["tags"] = [str(t).strip() for t in json.loads(raw_tags) if str(t).strip()]
            except ValueError:
                _m["tags"] = []
        elif raw_tags:
            _m["tags"] = [t.strip() for t in raw_tags.split(",") if t.strip()]
        else:
            _m["tags"] = []

etlMetadataColumns = build_etl_metadata_columns(
    linkKeyName, linkKeyColumns, resolvedTargetTable, cycleIdentifierColumn, columnMapping)

pii_errors = []
for m in columnMapping:
    tgt = m["target_col"]
    if "pii" not in m:
        pii_errors.append(f"  '{tgt}': missing 'pii' flag")
    elif m["pii"] is True and "SHA2_MASK" not in [t.upper() for t in m["transformations"]]:
        pii_errors.append(f"  '{tgt}': pii=true but SHA2_MASK not in transformations")
if pii_errors:
    raise ValueError("columnMapping PII validation failed:\n" + "\n".join(pii_errors))

print(f"source : {sourceCatalogName}.{sourceSchemaName}.{sourceTableName}")
print(f"target : {targetCatalogName}.{targetSchemaName}.{targetTableName}")
print(f"system : {sourceSystem}")
print(f"silver_key : {surrogateKeyColumnName}")
print(f"columns : {len(columnMapping)}")
print(f"JobRun_ID : {JobRun_ID}")
