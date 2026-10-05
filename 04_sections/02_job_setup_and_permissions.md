# Job setup and permissions

Create these jobs only after `config_loader.py` and `layer1_etl_job.py` are in the Databricks Git folder with `utils/` next to them.

## 1. Job service principal

Jobs run as this SP, not as a person.

1. **Settings → Identity and access → Service principals → Add new**.
2. Copy the Application ID.
3. On the SP **Permissions** tab, give your dev group **Service Principal: User**.

## 2. One job template

Each load job has two tasks:

```text
Task 1: load_config   (Serverless)        reads the approved mapping from Lakebase
          │
          ▼
Task 2: layer1_etl    (Classic cluster)   writes thoughtfocus_nsk.standardized
```

Task 1 must be named exactly `load_config`. Task 2 must depend on it.

1. **Jobs & Pipelines → Create → Job**.
2. Name it `keka_group_types_inc_std_load` for the first one.
3. Task 1: Notebook `config_loader`, compute Serverless.
4. Task 2: Notebook `layer1_etl_job`, classic cluster, **Depends on** `load_config`.
5. Job parameters (on the job, not the tasks):

```text
job_run_id          {{job.run_id}}
sourceTableName     keka_group_types_inc_std
sourceCatalogName   thoughtfocus_nsk
sourceSchemaName    keka_ns_csv
config_schema       <Lakebase schema with table_mappings and column_mappings>
```

6. **Run as** the Job SP.
7. Permissions: app identity **Can Manage Run**; dev group **Can Manage**.

## 3. Data grants for the Job SP

| Object | Rights |
|---|---|
| Catalog `thoughtfocus_nsk` | USE CATALOG |
| Schema `thoughtfocus_nsk.keka_ns_csv` | USE SCHEMA, SELECT |
| Schema `thoughtfocus_nsk.standardized` | USE SCHEMA, SELECT, MODIFY, CREATE TABLE, APPLY TAG (create the schema first if needed) |
| Table `thoughtfocus_nsk.standardized.dbx_notebook_audit` | SELECT, MODIFY (the first run can create it) |

Lakebase, in the Lakebase SQL editor:

```sql
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT databricks_create_role('<job-sp-application-id>', 'SERVICE_PRINCIPAL');
GRANT USAGE ON SCHEMA <lakebase-schema> TO "<job-sp-application-id>";
GRANT SELECT ON ALL TABLES IN SCHEMA <lakebase-schema> TO "<job-sp-application-id>";
ALTER DEFAULT PRIVILEGES IN SCHEMA <lakebase-schema> GRANT SELECT ON TABLES TO "<job-sp-application-id>";
```

## 4. Prove the first job

1. Confirm Lakebase has an active mapping for `keka_group_types_inc_std`.
2. Run the job once in Databricks.
3. Confirm `thoughtfocus_nsk.standardized.hr_group_types` exists and has rows.
4. Copy the Job ID into the agent as `KEKA_GROUP_TYPES_INC_STD_LOAD_JOB_ID`.

## 5. Clone the other 18 jobs

For each remaining source table: clone the job, rename it `<source_table>_load`, change only `sourceTableName`, copy the new Job ID into `{SOURCE_TABLE_UPPER}_LOAD_JOB_ID`.
