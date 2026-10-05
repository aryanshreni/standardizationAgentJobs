# Snapshot load (Keka / NetSuite)

The silver job writes the **current** source table. It does not look for `FileDate` or `ETLDate`, and it does not carry rows forward from an earlier month.

```python
createStagingTableQuery = f"""
SELECT
    '{sourceSystem}'  AS `source_system`,
    {link_key_expr}   AS `silver_link_key`,
    {col_expressions_str}
FROM {sourceCatalogName}.{sourceSchemaName}.{sourceTableName} src
"""
```

A re-run deletes the silver rows and inserts again, so silver matches the source table as it is now.

`silver_key` is an identity column on the target. It is not selected from the source.
