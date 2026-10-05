# Databricks notebook source
# MAGIC %md
# MAGIC ## Lakebase ETL config — Keka / NetSuite
# MAGIC
# MAGIC `%run` this notebook to make `load_etl_config` available. Do not call it directly.
# MAGIC
# MAGIC It reads the approved mapping for one source table. Run it on **serverless**.
# MAGIC A classic cluster cannot resolve the Lakebase hostname.

# COMMAND ----------

import json
import os
import ssl
import subprocess
import sys
from urllib.parse import quote_plus

try:
    from sqlalchemy import create_engine, text
except ImportError:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "sqlalchemy", "--quiet"],
        check=True,
    )
    from sqlalchemy import create_engine, text

try:
    import pg8000  # noqa: F401
except ImportError:
    pg8000 = None

# COMMAND ----------

NOTEBOOK_TRANSFORMATIONS = frozenset({
    "TRIM", "CAST", "DATE_CAST", "DATE_JULIAN", "DATE_MMDDYY", "DATE_MMDDYYYY",
    "AMOUNT_UNPACK", "CAST_DIVIDE", "LPAD_ZIP5", "LPAD_ZIP4", "SHA2_MASK", "TRIM_CAST",
})
_TERMINAL_TRANSFORMATIONS = frozenset({
    "CAST", "DATE_CAST", "DATE_JULIAN", "DATE_MMDDYY", "DATE_MMDDYYYY",
    "AMOUNT_UNPACK", "CAST_DIVIDE", "TRIM_CAST",
})
_TX_TRANSLATION = {
    "TRIM": ["TRIM"],
    "TRIM_CAST": ["TRIM"],
    "CAST": ["CAST"],
    "CAST_DECIMAL": [],
    "CAST_TO_STRING": [],
    "CAST_STRING": [],
    "DATE_PARSE": [],
    "DATE_CAST": ["DATE_CAST"],
    "DATE_JULIAN": ["DATE_JULIAN"],
    "DATE_MMDDYY": ["DATE_MMDDYY"],
    "DATE_MMDDYYYY": ["DATE_MMDDYYYY"],
    "PRESERVE_LEADING_ZEROS": [],
    "LPAD_ZIP5": ["LPAD_ZIP5"],
    "LPAD_ZIP4": ["LPAD_ZIP4"],
    "SHA2_MASK": [],
}
_TX_UNSUPPORTED = {
    "UPPERCASE": "no notebook equivalent in apply_single_tx",
    "LOWERCASE": "no notebook equivalent in apply_single_tx",
}
_NB_OWNED_SOURCE = frozenset({"filedate", "etldate"})
_NB_OWNED_TARGET = frozenset({
    "source_system", "silver_key", "silver_link_key", "jobrun_id", "targetetldate",
})
_SQL_WORDS = (
    "cast", "concat", "coalesce", "case", "when", "substring", "substr", "trim",
    "nullif", "select", "date_format", "to_date", "lpad", "rpad", "sha2", "ifnull",
)
_KEY_TOKENS = ("nbr", "id", "key", "num", "no")
_DEFAULT_CATALOG = "thoughtfocus_nsk"
_DEFAULT_SCHEMA = "silver_standardized"
_DEFAULT_AUDIT = "thoughtfocus_nsk.silver_standardized.dbx_notebook_audit"


class LakebaseConfigError(RuntimeError):
    pass


def _s(value):
    return "" if value is None else str(value).strip()


def source_system_for(table_name):
    name = _s(table_name).lower()
    if name.startswith("keka_") or name.startswith("get_keka_"):
        return "keka"
    if name.startswith("get_ns_") or name.startswith("ns_"):
        return "netsuite"
    return ""


def _has_amount_unpack(transformations_value):
    if transformations_value is None:
        return False
    return "AMOUNT_UNPACK" in _s(transformations_value).upper()


def _split_source_cols(source_col):
    raw = _s(source_col)
    if not raw:
        return None
    sep = "|" if "|" in raw else ("," if "," in raw else None)
    if sep is None:
        return None
    parts = [p.strip() for p in raw.split(sep) if p.strip()]
    return parts if len(parts) > 1 else None


def _divisor_from_row(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return int(number) if number.is_integer() else number


def _divisor_from_decimal(data_type):
    upper = _s(data_type).upper()
    if not (upper.startswith("DECIMAL") and "(" in upper and "," in upper):
        return None
    try:
        inner = upper[upper.index("(") + 1: upper.index(")")]
        scale = int(inner.split(",")[1].strip())
        return 10 ** scale
    except (ValueError, IndexError):
        return None


_PII_TAG = "pii"
_MAX_TAGS = 2


def _parse_tag_list(raw):
    if isinstance(raw, list):
        return [_s(t) for t in raw if _s(t)]
    text = _s(raw)
    if not text:
        return []
    if text[0] == "[":
        try:
            parsed = json.loads(text)
        except ValueError:
            return []
        return [_s(t) for t in parsed if _s(t)] if isinstance(parsed, list) else []
    return [t.strip() for t in text.split(",") if t.strip()]


def _tags_with_pii(raw_tags, pii):
    tags, seen = [], set()
    for tag in _parse_tag_list(raw_tags):
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        tags.append(_PII_TAG if key == _PII_TAG else tag)
    if pii is True and _PII_TAG not in seen:
        tags.append(_PII_TAG)
    if _PII_TAG in tags:
        tags = [_PII_TAG] + [t for t in tags if t != _PII_TAG]
    return ", ".join(tags[:_MAX_TAGS])


def _tri_bool(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    token = str(value).strip().lower()
    if token in ("true", "t", "yes", "y", "1"):
        return True
    if token in ("false", "f", "no", "n", "0"):
        return False
    return None


def is_expression(system_column_name):
    raw = _s(system_column_name)
    if not raw:
        return False
    if "(" in raw or "," in raw or "*" in raw or "||" in raw:
        return True
    lowered = " " + raw.lower() + " "
    return any(f" {word} " in lowered for word in _SQL_WORDS) or " as " in lowered


def render_data_type(row, errors, label):
    raw = _s(row.get("target_data_type"))
    if not raw:
        errors.append(f"{label}: no target_data_type")
        return ""
    upper = raw.upper()
    if "(" in upper:
        return upper
    if upper in ("DECIMAL", "NUMERIC", "DEC"):
        errors.append(f"{label}: target_data_type '{raw}' must be DECIMAL(p,s)")
        return ""
    if upper in ("VARCHAR", "CHAR", "TEXT", "NVARCHAR", "STRING"):
        return "STRING"
    return upper


def _parse_transformations(value, label, warnings):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw_tokens = [_s(v) for v in value if _s(v)]
    else:
        raw = _s(value)
        if not raw:
            return []
        if raw[0] in "[{":
            try:
                parsed = json.loads(raw)
            except ValueError as exc:
                warnings.append(f"{label}: transformations is not valid JSON ({exc}); ignored")
                return []
            if not isinstance(parsed, list):
                warnings.append(f"{label}: transformations JSON is not an array; ignored")
                return []
            raw_tokens = [_s(v) for v in parsed if _s(v)]
        else:
            raw_tokens = [_s(p) for p in raw.split(",") if _s(p)]
    result = []
    for token in raw_tokens:
        result.extend(p for p in (s.strip() for s in token.split("|")) if p)
    return result


def translate_transformations(tokens, label, dropped, warnings):
    steps = []
    for token in tokens:
        upper = _s(token).upper()
        if not upper:
            continue
        if upper in _TX_UNSUPPORTED:
            dropped.setdefault(label, []).append(f"{upper} ({_TX_UNSUPPORTED[upper]})")
            continue
        if upper in _TX_TRANSLATION:
            mapped = _TX_TRANSLATION[upper]
        elif upper in NOTEBOOK_TRANSFORMATIONS:
            mapped = [upper]
        else:
            dropped.setdefault(label, []).append(f"{upper} (unknown token)")
            continue
        for step in mapped:
            if step not in steps:
                steps.append(step)
    return steps


def _table(schema, name):
    return f'"{schema}".{name}' if schema else name


def fetch_configs(conn, schema="app"):
    try:
        rows = conn.execute(text(f"SELECT name, value FROM {_table(schema, 'configs')}"))
    except Exception:
        return {}
    return {_s(r[0]): "" if r[1] is None else str(r[1]) for r in rows}


def fetch_active_table_mapping(conn, source_table, schema="app"):
    row = conn.execute(
        text(
            f"SELECT id, source_table_name, target_table_name, "
            f"       table_description, tags, approved_at, version "
            f"FROM {_table(schema, 'table_mappings')} "
            f"WHERE lower(source_table_name) = lower(:t) AND is_active"
        ),
        {"t": _s(source_table)},
    ).mappings().first()
    return dict(row) if row else None


def fetch_active_column_mappings(conn, source_table, schema="app"):
    rows = conn.execute(
        text(
            f"SELECT id, source_column_name, target_column_name, "
            f"       source_data_type, target_data_type, transformations, pii, "
            f"       column_desc, tags, divisor, version "
            f"FROM {_table(schema, 'column_mappings')} "
            f"WHERE lower(source_table_name) = lower(:t) AND is_active "
            f"ORDER BY target_column_name, source_column_name"
        ),
        {"t": _s(source_table)},
    ).mappings().all()
    return [dict(r) for r in rows]


def link_key_columns_from_mapping(column_mapping):
    chosen = []
    for entry in column_mapping:
        source = entry.get("source_col")
        if isinstance(source, list):
            continue
        name = _s(source)
        if any(token in name.lower() for token in _KEY_TOKENS):
            chosen.append(name)
        if len(chosen) == 2:
            break
    if not chosen:
        for entry in column_mapping:
            source = entry.get("source_col")
            if isinstance(source, str) and source:
                chosen.append(source)
                break
    return chosen


def resolve_column_mapping(column_rows, *, link_key_name, surrogate_key_column, reports, errors):
    owned_targets = set(_NB_OWNED_TARGET)
    for extra in (link_key_name, surrogate_key_column):
        if _s(extra):
            owned_targets.add(_s(extra).lower())

    candidates = {}
    for row in column_rows:
        source_col = _s(row.get("source_column_name"))
        target_col = _s(row.get("target_column_name"))
        if not target_col:
            reports["unmapped_rows"].append(source_col)
            continue
        if source_col.lower() in _NB_OWNED_SOURCE or target_col.lower() in owned_targets:
            reports["skipped_notebook_owned"].append({
                "source": source_col,
                "target": target_col,
                "reason": "the notebook generates this column itself",
            })
            continue
        if _split_source_cols(source_col) is not None:
            if not _has_amount_unpack(row.get("transformations")):
                reports["expression_rows_skipped"].append(
                    {"source": source_col, "target": target_col})
                continue
        elif is_expression(source_col):
            reports["expression_rows_skipped"].append(
                {"source": source_col, "target": target_col})
            continue
        candidates.setdefault(target_col.lower(), []).append(row)

    def _rank(row):
        try:
            version = int(row.get("version") or 0)
        except (TypeError, ValueError):
            version = 0
        return (-version, _s(row.get("source_column_name")).lower())

    column_mapping = []
    for target_key in sorted(candidates):
        ranked = sorted(candidates[target_key], key=_rank)
        winner = ranked[0]
        if len(ranked) > 1:
            reports["duplicate_targets"][_s(winner.get("target_column_name"))] = {
                "kept": _s(winner.get("source_column_name")),
                "dropped": [_s(r.get("source_column_name")) for r in ranked[1:]],
            }
        source_col = _s(winner.get("source_column_name"))
        target_col = _s(winner.get("target_column_name"))
        label = target_col
        tokens = _parse_transformations(winner.get("transformations"), label, reports["warnings"])
        steps = translate_transformations(
            tokens, label, reports["dropped_transformations"], reports["warnings"])
        data_type = render_data_type(winner, errors, label)
        if not data_type:
            continue
        pii = _tri_bool(winner.get("pii"))
        if pii is None:
            reports["pii_unclassified"].append(target_col)
        source_parts = _split_source_cols(source_col)
        if source_parts:
            entry = {
                "source_col": source_parts,
                "target_col": target_col,
                "transformations": steps,
                "data_type": data_type,
                "pii": pii,
                "description": _s(winner.get("column_desc")),
                "tags": _tags_with_pii(winner.get("tags"), pii),
            }
            divisor = _divisor_from_decimal(data_type)
            if divisor is not None:
                entry["divisor"] = divisor
            column_mapping.append(entry)
            continue
        divisor = None
        if "CAST_DIVIDE" in steps:
            divisor = _divisor_from_row(winner.get("divisor")) or _divisor_from_decimal(data_type)
            if divisor is None:
                steps = [s for s in steps if s != "CAST_DIVIDE"]
                reports["dropped_transformations"].setdefault(label, []).append(
                    "CAST_DIVIDE (no divisor)")
        mask = pii is True or "SHA2_MASK" in [t.upper() for t in tokens]
        if mask and pii is not True:
            reports["warnings"].append(
                f"{label}: SHA2_MASK is requested but pii is {pii!r}; masking anyway")
        if mask:
            data_type = "STRING"
            steps = [s for s in steps if s != "SHA2_MASK"]
            steps.append("SHA2_MASK")
        elif not any(s in _TERMINAL_TRANSFORMATIONS for s in steps):
            steps.append("DATE_CAST" if data_type == "DATE" else "CAST")
        entry = {
            "source_col": source_col,
            "target_col": target_col,
            "transformations": steps,
            "data_type": data_type,
            "pii": pii,
            "description": _s(winner.get("column_desc")),
            "tags": _tags_with_pii(winner.get("tags"), pii),
        }
        if divisor is not None and "CAST_DIVIDE" in steps:
            entry["divisor"] = divisor
        column_mapping.append(entry)
    return column_mapping


def resolve_etl_config(source_table, table_mapping, column_rows, configs,
                       source_catalog=None, source_schema=None):
    source_table = _s(source_table)
    errors = []
    reports = {
        "pii_unclassified": [],
        "dropped_transformations": {},
        "skipped_notebook_owned": [],
        "duplicate_targets": {},
        "expression_rows_skipped": [],
        "unmapped_rows": [],
        "warnings": [],
    }
    if not source_table:
        raise LakebaseConfigError("source_table is required")
    configs = {_s(k): v for k, v in (configs or {}).items()}

    if not table_mapping:
        errors.append(f"no active table_mappings row for {source_table!r}")
        target_table = ""
    else:
        target_table = _s(table_mapping.get("target_table_name"))
        if not target_table:
            errors.append(f"the active table_mappings row for {source_table!r} has no target_table_name")

    source_catalog = _s(source_catalog)
    source_schema = _s(source_schema)
    if not source_catalog:
        errors.append("sourceCatalogName is required — pass it as a Job Parameter")
    if not source_schema:
        errors.append("sourceSchemaName is required — pass it as a Job Parameter")

    source_system = source_system_for(source_table) or _s(configs.get("SOURCE_SYSTEM"))
    if not source_system:
        errors.append(
            f"cannot tell keka from netsuite for {source_table!r} — name must start with "
            "keka_, get_keka_, get_ns_, or ns_"
        )

    target_catalog = _s(configs.get("CATALOG_LAYER_1")) or _DEFAULT_CATALOG
    target_schema = _s(configs.get("SCHEMA_LAYER_1")) or _DEFAULT_SCHEMA
    audit_table = _s(configs.get("AUDIT_TABLE")) or _DEFAULT_AUDIT
    surrogate_key_column = _s(configs.get("SURROGATE_KEY_COLUMN")) or "silver_key"
    link_key_name = "silver_link_key"

    column_mapping = resolve_column_mapping(
        column_rows or [],
        link_key_name=link_key_name,
        surrogate_key_column=surrogate_key_column,
        reports=reports,
        errors=errors,
    )
    if not column_mapping and not errors:
        errors.append(f"no loadable column mappings resolved for {source_table!r}")
    if errors:
        raise LakebaseConfigError(
            f"ETL configuration for {source_table} is not usable:\n"
            + "\n".join(f"  {i + 1}. {e}" for i, e in enumerate(errors))
        )
    return {
        "sourceCatalogName": source_catalog,
        "sourceSchemaName": source_schema,
        "sourceTableName": source_table,
        "targetCatalogName": target_catalog,
        "targetSchemaName": target_schema,
        "targetTableName": target_table,
        "tableDescription": _s((table_mapping or {}).get("table_description")),
        "tableTags": _s((table_mapping or {}).get("tags")),
        "sourceSystem": source_system,
        "linkKeyName": link_key_name,
        "linkKeyColumns": link_key_columns_from_mapping(column_mapping),
        "columnMapping": column_mapping,
        "cycleIdentifierColumn": "",
        "surrogateKeyColumnName": surrogate_key_column,
        "auditTableName": audit_table,
        "provenance": {
            "table_mapping_id": _s((table_mapping or {}).get("id")),
            "table_mapping_version": (table_mapping or {}).get("version"),
            "approved_at": _s((table_mapping or {}).get("approved_at")),
            "active_column_rows": len(column_rows or []),
            "columns_resolved": len(column_mapping),
            "surrogate_key_column": surrogate_key_column,
        },
        "reports": reports,
    }


def lakebase_engine(project=None, branch=None, endpoint=None, database=None):
    global pg8000
    if pg8000 is None:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "pg8000", "--quiet"],
            check=True,
        )
        import pg8000  # noqa: F401
    from databricks.sdk import WorkspaceClient

    project = _s(project) or _s(os.getenv("LAKEBASE_INSTANCE_NAME")) or "standardization-agent"
    branch = _s(branch) or _s(os.getenv("LAKEBASE_BRANCH")) or "production"
    endpoint = _s(endpoint) or _s(os.getenv("LAKEBASE_ENDPOINT")) or "primary"
    database = _s(database) or _s(os.getenv("PGDATABASE")) or "databricks_postgres"
    client = WorkspaceClient()
    endpoint_path = f"projects/{project}/branches/{branch}/endpoints/{endpoint}"
    endpoint_info = client.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}")
    host = endpoint_info["status"]["hosts"]["host"]
    roles = client.api_client.do(
        "GET", f"/api/2.0/postgres/projects/{project}/branches/{branch}/roles"
    )["roles"]
    me = client.current_user.me().user_name
    user = next(
        (r["status"]["postgres_role"] for r in roles if r["status"]["postgres_role"] == me),
        me,
    )
    token = client.api_client.do(
        "POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_path}
    )["token"]
    return create_engine(
        f"postgresql+pg8000://{quote_plus(user)}:{quote_plus(token)}@{host}:5432/{database}",
        connect_args={"ssl_context": ssl.create_default_context()},
    )


def load_etl_config(source_table, schema="app", engine=None,
                    source_catalog=None, source_schema=None):
    own_engine = engine is None
    engine = engine or lakebase_engine()
    try:
        with engine.connect() as conn:
            table_mapping = fetch_active_table_mapping(conn, source_table, schema)
            column_rows = fetch_active_column_mappings(conn, source_table, schema)
            configs = fetch_configs(conn, schema)
    finally:
        if own_engine:
            engine.dispose()
    config = resolve_etl_config(
        source_table, table_mapping, column_rows, configs,
        source_catalog=source_catalog, source_schema=source_schema)
    print(describe_etl_config(config))
    return config


def describe_etl_config(config):
    reports = config.get("reports") or {}
    provenance = config.get("provenance") or {}
    lines = [
        "[lakebase_config] resolved configuration",
        f"  source        : {config['sourceCatalogName']}.{config['sourceSchemaName']}"
        f".{config['sourceTableName']}",
        f"  target        : {config['targetCatalogName']}.{config['targetSchemaName']}"
        f".{config['targetTableName']}",
        f"  description   : {config.get('tableDescription') or '(none)'}",
        f"  tags          : {config.get('tableTags') or '(none)'}",
        f"  source system : {config['sourceSystem']}",
        f"  link key      : {config['linkKeyName']} from {config['linkKeyColumns']}",
        f"  silver key    : {config['surrogateKeyColumnName']}",
        f"  audit table   : {config['auditTableName']}",
        f"  approved      : table_mapping version={provenance.get('table_mapping_version')}",
        f"  columns       : {provenance.get('columns_resolved')} resolved from "
        f"{provenance.get('active_column_rows')} active row(s)",
    ]
    if reports.get("skipped_notebook_owned"):
        lines.append("  NOTE notebook-owned column(s) skipped: "
                     + ", ".join(str(s["target"]) for s in reports["skipped_notebook_owned"]))
    if reports.get("duplicate_targets"):
        lines.append("  WARNING duplicate targets: " + json.dumps(reports["duplicate_targets"]))
    if reports.get("expression_rows_skipped"):
        lines.append("  WARNING expression mapping(s) skipped: "
                     + ", ".join(str(e["target"]) for e in reports["expression_rows_skipped"]))
    if reports.get("pii_unclassified"):
        lines.append("  WARNING pii unclassified: " + ", ".join(reports["pii_unclassified"]))
    if reports.get("dropped_transformations"):
        lines.append("  WARNING dropped transformations: "
                     + json.dumps(reports["dropped_transformations"]))
    for warning in reports.get("warnings") or []:
        lines.append(f"  WARNING {warning}")
    return "\n".join(lines)


print("Lakebase config loader ready: load_etl_config, resolve_etl_config, describe_etl_config")
