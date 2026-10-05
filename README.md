# Standardization Agent: Layer 1 (silver) jobs

The agent names and you approve. These notebooks write the silver table, the approved columns, and the source rows into `thoughtfocus_nsk.silver_standardized`.

## Files

This repo root is the Databricks Git folder. Both notebooks `%run "./utils/..."`.

```text
config_loader.py          task 1, serverless
layer1_etl_job.py         task 2, classic cluster
utils/
  lakebase_config.py
  etl_config_bootstrap.py
  transformation_functions.py
04_sections/              setup notes
```

## How a load works

Each source table in `thoughtfocus_nsk.keka_ns_csv` has its own Databricks job. The notebooks are the same. Only the job's default `sourceTableName` changes.

```text
Task 1: load_config   (serverless)   reads the approved mapping from Lakebase
         │
         ▼
Task 2: layer1_etl    (classic)      reads the source table and writes silver
```

Two tasks are required: serverless can reach Lakebase; a classic cluster can read the source tables.

The agent starts the job with:

```text
sourceTableName   <that table>
job_run_id        <agent run id>
```

Job defaults:

```text
sourceCatalogName   thoughtfocus_nsk
sourceSchemaName    keka_ns_csv
config_schema       public
```

## Silver table

- Catalog / schema: `thoughtfocus_nsk.silver_standardized`
- Extra identity column: `silver_key` (not a source column)
- `source_system`: `keka` or `netsuite`, from the source table name
- Load: the current source table. `FileDate` / `ETLDate` are not required
- A re-run replaces the silver rows for that table
- Comments and tags come from the approved Lakebase rows

## The 19 jobs

| Source table | Job name |
|---|---|
| `keka_currencies_inc_std` | `keka_currencies_inc_std_load` |
| `keka_departments_inc_std` | `keka_departments_inc_std_load` |
| `keka_employees_inc_skill_std` | `keka_employees_inc_skill_std_load` |
| `keka_employees_inc_std` | `keka_employees_inc_std_load` |
| `keka_exitreasons_inc_std` | `keka_exitreasons_inc_std_load` |
| `keka_group_types_inc_std` | `keka_group_types_inc_std_load` |
| `keka_groups_inc_std` | `keka_groups_inc_std_load` |
| `keka_holiday_calendar_dates_inc_std` | `keka_holiday_calendar_dates_inc_std_load` |
| `keka_holiday_calendar_inc_std` | `keka_holiday_calendar_inc_std_load` |
| `keka_leave_request_inc_std` | `keka_leave_request_inc_std_load` |
| `keka_locations_inc_std` | `keka_locations_inc_std_load` |
| `keka_noticeperiods_inc_std` | `keka_noticeperiods_inc_std_load` |
| `keka_titles_inc_std` | `keka_titles_inc_std_load` |
| `get_ns_customers_std` | `get_ns_customers_std_load` |
| `get_ns_employees_std` | `get_ns_employees_std_load` |
| `get_ns_projects_std` | `get_ns_projects_std_load` |
| `ns_project_tasks_std` | `ns_project_tasks_std_load` |
| `ns_resource_allocation_bulk_std` | `ns_resource_allocation_bulk_std_load` |
| `ns_timesheet_std` | `ns_timesheet_std_load` |

Create the first job, prove a load, then clone the rest. Setup steps are in `04_sections/02_job_setup_and_permissions.md`.

## What the agent stores

After you approve, Lakebase already has the active `table_mappings` and `column_mappings` rows. Put each job id in Setup as `{SOURCE_TABLE_UPPER}_LOAD_JOB_ID`, for example `KEKA_GROUP_TYPES_INC_STD_LOAD_JOB_ID`. `DATABRICKS_LOAD_JOB_ID` is only a fallback for one-table tests.

If that id is empty, the agent does not start a job and the silver table is not created.
