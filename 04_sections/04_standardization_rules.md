# Silver table rules (Keka / NetSuite)

These are the rules the load job follows. Naming still comes from the agent's dictionary.

| Rule | Value |
|---|---|
| Source catalog.schema | `thoughtfocus_nsk.keka_ns_csv` |
| Silver catalog.schema | `thoughtfocus_nsk.silver_standardized` |
| Table name | The approved `table_mappings.target_table_name` (`hr_*` or `ns_*` / `allocation_*`) |
| Description | The Unity Catalog source comment, saved on approval |
| Column names | Approved `column_mappings` rows only |
| Column case | `lower_snake_case` |
| Abbreviations | Written out in full (dictionary) |
| Surrogate key | `silver_key`, identity, added by the job |
| `source_system` | `keka` or `netsuite`, from the source table name |
| Load | Current source table; no `FileDate` / `ETLDate` required |
| Audit table | `thoughtfocus_nsk.silver_standardized.dbx_notebook_audit` |
