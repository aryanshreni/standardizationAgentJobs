# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformation Functions — Shared Library
# MAGIC
# MAGIC `%run` this notebook to make `wrap_string`, `apply_single_tx` and `build_column_expr`
# MAGIC available. Do not call this notebook directly — it has no widgets or entry point.

# COMMAND ----------

import base64
import gzip
import json
import re

# COMMAND ----------

def wrap_string(string, char):
    """Idempotently surround `string` with `char` — used to backtick-quote catalog, schema
    and table identifiers read from widgets before they are interpolated into SQL.

    Lives here rather than in the workspace's "initialize connections" notebook, which is
    where the ETL notebooks used to pick it up. That notebook also reads a key-vault secret
    scope that does not exist in every workspace, so `%run`-ing it just to get this four-line
    helper made the loaders fail at GetSecret before reading a single widget. Defining it in
    the module the loaders already `%run` removes that dependency; the definition is
    unchanged, so notebooks that still source it from elsewhere behave identically.
    """
    if not string.startswith(char):
        string = char + string
    if not string.endswith(char):
        string = string + char
    return string


# COMMAND ----------

# Anything that would make a generated COMMENT clause span lines or carry a control character.
# Descriptions are free text written by a reviewer or produced by the suggester, so a newline in
# the middle of one is ordinary rather than exceptional.
_COMMENT_WHITESPACE = re.compile(r"[\s\x00-\x1f\x7f]+")


def normalize_comment_text(description):
    """One description reduced to the text a comment actually carries, or `""` if there is none.

    Whitespace runs — including the newlines and tabs a multi-line description arrives with —
    collapse to single spaces and the result is trimmed, so the generated DDL stays one readable
    line per column.

    This is also the *comparison* form, and that is the reason it is a function of its own rather
    than three lines inside `sql_comment_literal`. Deciding whether a catalog comment still matches
    the description means comparing meaning, not spelling: the description a reviewer wrote as
    `"Customer's\\naccount"` is stored in the catalog as the single line `Customer's account`, and
    comparing the two raw would report a change on every load and rewrite every comment forever.
    Compare `normalize_comment_text(current) == normalize_comment_text(desired)`; escape only when
    a statement is actually being generated.
    """
    if description is None:
        return ""
    return _COMMENT_WHITESPACE.sub(" ", str(description)).strip()


def sql_string_literal(value):
    """`value` as a Databricks SQL string *literal* — the one place that escaping happens.

    Deliberately not `wrap_string`: that helper wraps an *identifier* in backticks and escapes
    nothing, so using it here would emit `` `Customer's account` `` and fail as an unresolved
    column rather than as a quoted string.

    Two characters have to be escaped, and both are ordinary in descriptions:

      apostrophe  `Customer's account` — closes the literal early, so the rest of the sentence is
                  parsed as SQL and the whole CREATE TABLE fails with a syntax error. Doubled to
                  `''`, which is the escape every SQL dialect accepts.
      backslash   `\\\\Server\\Share` — Databricks parses string literals with backslash as an
                  escape character (`spark.sql.parser.escapedStringLiterals` defaults to false),
                  so a lone `\\` silently eats the next character and a trailing `\\` escapes the
                  closing quote. Doubled to `\\\\`, which parses back to one backslash.

    Doing only the apostrophe — the obvious half of the job — leaves the backslash case broken,
    which is why both live in one function rather than at each call site.
    """
    text = "" if value is None else str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def sql_comment_literal(description):
    """One description as a Databricks SQL string literal, or `""` when there is nothing to say.

    Returns `""` — not `COMMENT ''` and never the text `None` — for None, a blank string, or a
    value that is nothing but whitespace. An empty comment is worse than no comment: it looks
    like a description was reviewed and left deliberately blank.
    """
    text = normalize_comment_text(description)
    return sql_string_literal(text) if text else ""


def sql_comment_clause(description):
    """The ` COMMENT '...'` fragment for a DDL column or table clause, or `""` if there is none.

    Keeps the caller free of `if description:` branching, so a column line is one f-string
    whether or not that column has been described.
    """
    literal = sql_comment_literal(description)
    return f" COMMENT {literal}" if literal else ""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tag reconciliation
# MAGIC
# MAGIC `SET TAGS` is additive: it adds and updates, and there is no statement in it that removes
# MAGIC anything. A load that only ever issues `SET TAGS` therefore makes the catalog the *union*
# MAGIC of every tag set that has ever been approved, so a tag a reviewer dropped stays on the
# MAGIC table forever and the catalog stops matching the approved configuration. These helpers
# MAGIC diff the approved set against what is in Unity Catalog now and emit the `UNSET TAGS` half
# MAGIC as well, so approval *replaces* the tag set rather than adding to it.

# COMMAND ----------

def parse_tags(raw):
    """Normalize a tag value to a list of strings however it arrived (list, JSON array, CSV).

    The approved tag set reaches a notebook three ways — as a Python list from a test-value
    cell, as the JSON text a JSON column round-trips to, and as the comma-separated string
    `app.table_mappings.tags` stores — so all three are accepted here rather than at each
    call site. Order is preserved and blanks are dropped; nothing else is changed, because a
    tag is a label a reviewer approved and not an identifier to be rewritten.
    """
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    text = str(raw or "").strip()
    if not text:
        return []
    if text[0] == "[":
        import json as _json
        try:
            return [str(t).strip() for t in _json.loads(text) if str(t).strip()]
        except ValueError:
            return []
    return [t.strip() for t in text.split(",") if t.strip()]


def normalize_tag_name(tag):
    """One tag reduced to its *comparison* form: trimmed and lower-cased.

    Unity Catalog tag names are case-insensitive, so `Account` in the catalog and `account`
    in the approved set are the same tag. Comparing them raw would report the first as stale
    and the second as new, and the load would UNSET and SET the same tag on every run.
    """
    return str(tag or "").strip().lower()


def unique_tags(tags):
    """`tags` with blanks and case-insensitive duplicates removed, first spelling kept."""
    out, seen = [], set()
    for tag in parse_tags(tags):
        key = normalize_tag_name(tag)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


def tags_clause(tags):
    """The `('a' = '', 'b' = '')` fragment for SET TAGS, or None when there is nothing to set.

    Tags are stored as simple labels, so every value is the empty string. Names go through
    sql_string_literal rather than being pasted in raw: a quote or backslash in a tag would
    otherwise end the literal early and take the statement — and with it the load — down.
    """
    pairs = [f"{sql_string_literal(t)} = ''" for t in tags if str(t).strip()]
    return f"({', '.join(pairs)})" if pairs else None


def tag_names_clause(tags):
    """The `('a', 'b')` fragment for UNSET TAGS, or None when there is nothing to remove.

    UNSET names the tags to drop and takes no values, so this is deliberately not
    `tags_clause`: passing `'a' = ''` to UNSET TAGS is a syntax error.
    """
    names = [sql_string_literal(t) for t in tags if str(t).strip()]
    return f"({', '.join(names)})" if names else None


def tag_sync_statements(qualified_table, desired_tags, current_tags, column=None):
    """The ALTER statements that make one object's Unity Catalog tags match the approved set.

    Returns `(statements, summary)`. `summary` carries `added`, `removed`, `kept` and a
    one-line `state` for the run log, so the caller prints what happened without re-deriving
    it. A pure function of its arguments: no Spark, so the diff is testable directly.

    Args:
        qualified_table : fully qualified target table, already backtick-quoted by the caller
        desired_tags    : the approved tag set (list, JSON text or CSV — see parse_tags)
        current_tags    : what Unity Catalog holds right now, or None when it could not be read
        column          : column name to tag, or None to tag the table itself

    Two cases deliberately do not remove anything:

      * `current_tags is None` — the catalog could not be read, so there is nothing to diff
        against and any "stale" list would be a guess. Falls back to additive-only, which is
        what this sync did before the diff existed, rather than failing the load.
      * `desired_tags` is empty — blank has never meant "remove this" anywhere in this
        product, and it is the value an unfilled field has. One un-populated config would
        otherwise strip every tag off the table. Reported in `state` so it is visible.

    A tag present under both names is left alone rather than re-set. That keeps steady state
    at zero statements, and preserves a value someone set on it by hand; the approved set only
    ever carries empty values, so re-setting it could only overwrite that value with nothing.
    """
    target = f"ALTER TABLE {qualified_table}"
    if column:
        target += f" ALTER COLUMN `{column}`"

    desired = unique_tags(desired_tags)
    if not desired:
        return [], {"added": [], "removed": [], "kept": [],
                    "state": "no approved tags — existing catalog tags left alone"}

    if current_tags is None:
        clause = tags_clause(desired)
        return ([f"{target} SET TAGS {clause}"] if clause else []), {
            "added": desired, "removed": [], "kept": [],
            "state": "current tags unreadable — added the approved set without removing anything"}

    current = unique_tags(current_tags)
    desired_keys = {normalize_tag_name(t) for t in desired}
    current_keys = {normalize_tag_name(t) for t in current}

    added   = [t for t in desired if normalize_tag_name(t) not in current_keys]
    kept    = [t for t in desired if normalize_tag_name(t) in current_keys]
    removed = [t for t in current if normalize_tag_name(t) not in desired_keys]

    statements = []
    set_clause = tags_clause(added)
    if set_clause:
        statements.append(f"{target} SET TAGS {set_clause}")
    unset_clause = tag_names_clause(removed)
    if unset_clause:
        statements.append(f"{target} UNSET TAGS {unset_clause}")

    if not statements:
        state = "already matches the approved set"
    else:
        state = f"added {len(added)}, removed {len(removed)}, unchanged {len(kept)}"
    return statements, {"added": added, "removed": removed, "kept": kept, "state": state}

# COMMAND ----------

def apply_single_tx(tx, expr, src, dtype, source_system, divisor=100):
    """Wrap an existing SQL expression with one transformation step.

    Args:
        tx            : transformation step name (case-insensitive)
        expr          : SQL expression string from the prior step (or raw column ref on first step)
        src           : mapping_entry["source_col"] — needed by AMOUNT_UNPACK for sign/mag cols
        dtype         : mapping_entry["data_type"].upper()
        source_system : literal source system name used in SHA2_MASK prefix
        divisor       : AMOUNT_UNPACK divisor (default 100 = 2dp currency)

    Every numeric/date coercion here uses TRY_CAST, never CAST, so a blank or malformed
    value becomes NULL instead of failing the whole load.
    """
    tx = tx.upper()

    if tx == "TRIM":
        return f"TRIM({expr})"

    elif tx == "CAST":
        return f"TRY_CAST({expr} AS {dtype})"

    elif tx == "DATE_CAST":
        return f"TRY_CAST(NULLIF(TRIM({expr}), '') AS DATE)"

    elif tx == "DATE_JULIAN":
        return f"TRY_CAST(julian_to_gregorian(NULLIF(TRIM({expr}), '')) AS DATE)"

    elif tx == "DATE_MMDDYY":
        return (
            f"TRY_CAST(convert_to_yyyy_mm_dd("
            f"CAST(TRY_CAST(NULLIF(TRIM({expr}), '') AS DECIMAL(38,0)) AS STRING), 'MMddyy') AS DATE)"
        )

    elif tx == "DATE_MMDDYYYY":
        return (
            f"TRY_CAST(convert_to_yyyy_mm_dd("
            f"CAST(TRY_CAST(NULLIF(TRIM({expr}), '') AS DECIMAL(38,0)) AS STRING), 'MMddyyyy') AS DATE)"
        )

    elif tx == "AMOUNT_UNPACK":
        # src must be [sign_col, mag_col] — only valid as a single-step transformation
        sign_col, mag_col = src[0], src[1]
        if divisor == 1:
            return f"TRY_CAST(CONCAT(`{sign_col}`, `{mag_col}`) AS {dtype})"
        else:
            return f"TRY_CAST(TRY_CAST(CONCAT(`{sign_col}`, `{mag_col}`) AS DOUBLE) / {divisor} AS {dtype})"

    elif tx == "LPAD_ZIP5":
        return (
            f"CASE WHEN {expr} IS NULL THEN NULL "
            f"WHEN TRY_CAST({expr} AS DECIMAL(38,0)) = 0 THEN NULL "
            f"ELSE LPAD(CAST(TRY_CAST({expr} AS DECIMAL(38,0)) AS STRING), 5, '0') END"
        )

    elif tx == "LPAD_ZIP4":
        return (
            f"CASE WHEN {expr} IS NULL THEN NULL "
            f"WHEN TRY_CAST({expr} AS DECIMAL(38,0)) = 0 THEN NULL "
            f"ELSE LPAD(CAST(TRY_CAST({expr} AS DECIMAL(38,0)) AS STRING), 4, '0') END"
        )

    elif tx == "CAST_DIVIDE":
        return f"TRY_CAST(TRY_CAST({expr} AS DECIMAL(15,0)) / {divisor} AS {dtype})"

    elif tx == "SHA2_MASK":
        return f"sha2(concat('{source_system}_', COALESCE(CAST({expr} AS STRING), '')), 256)"

    elif tx == "TRIM_CAST":
        # Legacy combined step — kept for backwards compatibility
        return f"TRIM(COALESCE(CAST({expr} AS STRING), 'NA'))"

    else:
        raise ValueError(f"Unknown transformation step '{tx}'")


def build_column_expr(mapping_entry, source_system):
    """Build a complete SQL column expression from one columnMapping entry.

    Returns a string like: <expr> AS `target_col`
    """
    src     = mapping_entry["source_col"]
    tgt     = mapping_entry["target_col"]
    dtype   = mapping_entry["data_type"].upper()
    tx_list = [t.upper() for t in mapping_entry["transformations"]]
    divisor = mapping_entry.get("divisor", 100)

    # Start with the raw column reference
    if isinstance(src, list):
        # AMOUNT_UNPACK — sign+mag pair; apply_single_tx handles the concat
        expr = f"`{src[0]}`"
    else:
        expr = f"`{src}`"

    # Chain each transformation step left-to-right
    for tx in tx_list:
        expr = apply_single_tx(tx, expr, src, dtype, source_system, divisor)

    return f"{expr} AS `{tgt}`"


# COMMAND ----------

def build_etl_metadata_columns(link_key_name, link_key_columns, target_table, cycle_column,
                               column_mapping):
    """Metadata columns the ETL generates itself, described and tagged.

    `cycle_column` is unused: Keka / NetSuite loads write the current source table and do
    not use a cycle date. The argument stays so older callers keep working.
    """
    key_columns = ", ".join(
        str(c).strip() for c in (link_key_columns or []) if str(c).strip())
    columns = [
        {"target_col": link_key_name,
         "description": f"Generated link key for {target_table}: a non-negative BIGINT "
                        f"hash (xxhash64) of {key_columns or 'the configured key columns'} "
                        f"joined with '|'. Not a source column.",
         "tags": ["business_key"]},
        {"target_col": "source_system",
         "description": "Source system for this row: keka or netsuite.",
         "tags": ["lineage"]},
        {"target_col": "TargetETLDate",
         "description": "Timestamp this row was written into thoughtfocus_nsk.silver_standardized.",
         "tags": ["technical_metadata", "audit"]},
        {"target_col": "JobRun_ID",
         "description": "Identifier of the Databricks job run that wrote this row.",
         "tags": ["technical_metadata", "audit"]},
    ]

    # A mapped column always wins. If a reviewer has described a column of this name in Lakebase,
    # that description is the approved one and this default must not overwrite it. FileDate and
    # SourceETLDate are the realistic collisions, since they do exist in the source. No collision
    # exists today, so this drops nothing — it is here so that adding such a mapping later cannot
    # silently override reviewer-approved metadata.
    mapped = {(m.get("target_col") or "").strip().lower() for m in (column_mapping or [])}
    return [c for c in columns if (c["target_col"] or "").strip().lower() not in mapped]


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task-value handover
# MAGIC
# MAGIC The Jobs control plane stores a task value **as JSON**, so the size it measures against its
# MAGIC 48 KiB per-key limit is the JSON *encoding* of the string, not the string: every `"` in the
# MAGIC payload becomes `\"` on the way in. A 169-column `etl_config` is 47,381 bytes of JSON and
# MAGIC 52,193 bytes once escaped — under the limit by its own length and 3,041 bytes over it as
# MAGIC stored. `len(payload)` therefore passed the loader's guard and `SetTaskValue` rejected it
# MAGIC with `INVALID_PARAMETER_VALUE: The task value is too large`, which failed `load_config`,
# MAGIC skipped the ETL task with `UPSTREAM_FAILED` and left `xaa_result_balance`'s six generated
# MAGIC metadata columns with no comment and no tag while the other eight tables had both.
# MAGIC
# MAGIC The pair below lives in this module because the two halves run on different compute and
# MAGIC must not drift: `encode_task_value` runs in `config_loader` on serverless, and
# MAGIC `decode_task_value` runs in `etl_config_bootstrap` on the classic cluster.

# COMMAND ----------

_TASK_VALUE_LIMIT = 48 * 1024
# An explicit marker rather than "try base64 and see": a resolved etl_config is always a JSON
# object, so `{` versus this prefix tells the two forms apart with no guessing, and a reader old
# enough not to know the prefix fails loudly on it instead of parsing garbage.
_TASK_VALUE_GZIP_PREFIX = "gzip+base64:"


def task_value_stored_size(value):
    """Bytes the Jobs service stores for `value` — its JSON encoding, not its own length.

    This is the number the 48 KiB per-key limit applies to. Measuring `len(value.encode())`
    instead understates a quote-heavy payload by roughly one byte per quote character.
    """
    return len(json.dumps(value).encode("utf-8"))


def task_value_is_compressed(value):
    """True when `value` was produced by the compressed branch of `encode_task_value`."""
    return (value or "").strip().startswith(_TASK_VALUE_GZIP_PREFIX)


def encode_task_value(payload, limit=_TASK_VALUE_LIMIT, label=None):
    """`payload` in a form the Jobs service will accept, compressed only if it has to be.

    A payload that already fits is returned exactly as it arrived, so the tables that fit today
    keep publishing readable JSON — that is what makes the value the loader prints paste-able
    into the `debug_etl_config` widget, and it keeps the common path byte-for-byte unchanged.

    Only a payload the service would reject is gzipped and base64'd. `mtime=0` keeps that
    deterministic: the same configuration produces the same task value on every run, so a
    re-run is comparable rather than merely equivalent.
    """
    payload = "" if payload is None else str(payload)
    stored = task_value_stored_size(payload)
    if stored <= limit:
        return payload

    compressed = _TASK_VALUE_GZIP_PREFIX + base64.b64encode(
        gzip.compress(payload.encode("utf-8"), compresslevel=9, mtime=0)).decode("ascii")
    compressed_stored = task_value_stored_size(compressed)
    if compressed_stored > limit:
        raise ValueError(
            f"etl_config for {label or 'this table'} is {stored} bytes as a stored task value "
            f"and still {compressed_stored} bytes gzipped, over the {limit}-byte taskValues "
            "per-key limit. Hand it over through a UC Volume file instead of a task value."
        )
    return compressed


def decode_task_value(value):
    """The payload a `load_config` task published, whichever of the two forms it used.

    Plain JSON — every value published before this existed, and every value a reviewer pastes
    into the `debug_etl_config` widget — passes straight through, so neither path changes.
    """
    text = (value or "").strip()
    if not task_value_is_compressed(text):
        return text

    # Whitespace is tolerated inside the encoded half because a reviewer pasting a long value
    # into a widget can get it wrapped; a task value read from the Jobs service never is.
    encoded = "".join(text[len(_TASK_VALUE_GZIP_PREFIX):].split())
    try:
        return gzip.decompress(base64.b64decode(encoded)).decode("utf-8")
    except Exception as exc:
        raise ValueError(
            f"the task value is marked '{_TASK_VALUE_GZIP_PREFIX}' but did not decompress "
            f"({type(exc).__name__}: {exc}) — the value was truncated, or this notebook's "
            "workspace copy of utils/transformation_functions is older than the one the "
            "load_config task ran. Re-import both."
        ) from exc


# COMMAND ----------

print("Transformation functions loaded: wrap_string, apply_single_tx, build_column_expr, "
      "build_etl_metadata_columns, tag_sync_statements, encode_task_value, decode_task_value")
