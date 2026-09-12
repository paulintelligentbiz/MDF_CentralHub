# Fabric notebook source


# MARKDOWN ********************

# # nb_CopyTableToLakehouse
# 
# Generic, parameterized MDF task notebook: copies one source table into a
# Bronze Delta table in whatever lakehouse the Task points it at. Invoked by
# `pl_Task_Executor` for any `orch.Tasks` row with `TaskType = "Notebook"`
# and `ObjectName` pointing at this notebook; `ParametersJson` supplies the
# per-table source *and destination* -- source schema/table plus destination
# workspace/lakehouse/table. Nothing about the destination is configured on
# the notebook itself (no lakehouse needs to be attached here), matching the
# Wave Runner MDF framework's design: every source and destination is data
# in the Tasks table, not notebook configuration.
# 
# **One-time setup before this will run:**
# 1. The identity this notebook runs as (your account for manual testing; the
#    pipeline's identity once scheduled) needs write access to whatever
#    destination lakehouse(s) the Tasks you run point it at -- there's no
#    attached-lakehouse shortcut, so that access has to exist explicitly.
# 2. That same identity needs an AAD-based login/user on the source database
#    (`ContosoDW-DEV`) -- `CREATE USER [...] FROM EXTERNAL PROVIDER` there, with
#    read access to whatever tables the Tasks you run point it at.
# 3. Make sure `pyodbc` and the **ODBC Driver 18 for SQL Server** are available in the
#    attached environment (`%pip install pyodbc` in a cell if needed).
# 4. `pl_Task_Executor`'s `Run Notebook` activity needs to forward `ParametersJson` as
#    a base parameter (see the accompanying pipeline fix) so this notebook actually
#    receives the table name at runtime.
# 
# **Auth note:** connects with an AAD access token for whatever identity this
# notebook is running as (your account manually; the pipeline's run-as identity
# when scheduled) -- same pattern `nb_RefreshObjectIDs` uses for `db_Metadata`,
# via `notebookutils.credentials.getToken(...)`, no interactive sign-in and no
# stored secret. POC-only shortcut: that identity still needs an AAD-based login
# on `ContosoDW-DEV` itself (`CREATE USER [...] FROM EXTERNAL PROVIDER` there,
# with the needed grants) -- this doesn't create that, it only avoids a
# browser prompt or a secret to present at connect time. (Previously used
# `Authentication=Active Directory Interactive`, which opens a browser sign-in --
# fine running this manually, but it just hangs waiting for a sign-in nobody is
# there to complete when invoked unattended from a pipeline, failing with a
# login timeout.)


# PARAMETERS CELL ********************

# Default parameters -- overridden by pl_Task_Executor's base-parameters mapping
# when this notebook is invoked as a "Notebook" Task. Runnable standalone with
# these defaults for manual testing.
#
# Optional incremental keys (both omitted below -> full load, unchanged behavior):
#   "watermarkColumn": the source column to filter/track on (must match a name in
#     orch.TaskWatermark.WatermarkColumn for whichever Task calls this notebook).
#   "watermarkValue": the last high-water value to read forward from -- an
#     ISO-8601 string for a DateTime watermark, a plain number for a Numeric one
#     (matching orch.WatermarkDataType). Typically supplied by pl_Task_Executor
#     from orch.TaskWatermark, not hardcoded here.
ParametersJson = (
    '{"sourceSchema": "dbo", "sourceTable": "REPLACE_ME", '
    '"destWorkspaceId": "947d3136-33ac-458a-be73-ac7dc38afaa5", '
    '"destLakehouseId": "649b7795-2e22-4627-8b25-9749a6f492f0", '
    '"destSchema": "dbo"}'
)


# CELL ********************

import json
import pandas as pd

params = json.loads(ParametersJson)
SOURCE_SCHEMA = params.get("sourceSchema", "dbo")
SOURCE_TABLE = params["sourceTable"]
DEST_WORKSPACE_ID = params["destWorkspaceId"]
DEST_LAKEHOUSE_ID = params["destLakehouseId"]
DEST_TABLE = params.get("destTable", SOURCE_TABLE)
# Destination lakehouse is schema-enabled -- a table written to Tables/<name>
# directly (no schema segment) lands outside any schema and Fabric can't
# recognize it as a table ("Unable to identify these objects as tables or
# views"). "dbo" is the default schema unless ParametersJson says otherwise.
DEST_SCHEMA = params.get("destSchema", "dbo")

# Both present -> incremental load, filtered on this column/value and appended
# rather than overwritten. Either one missing -> full load, exactly as before.
WATERMARK_COLUMN = params.get("watermarkColumn")
WATERMARK_VALUE = params.get("watermarkValue")
INCREMENTAL = WATERMARK_COLUMN is not None and WATERMARK_VALUE is not None

# "source connection" (ContosoDW-DEV) -- see /topics/database-connections.md
SOURCE_SERVER = "paulsdemos.database.windows.net"
SOURCE_DATABASE = "ContosoDW-DEV"
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"

if INCREMENTAL:
    print(f"Copying {SOURCE_SCHEMA}.{SOURCE_TABLE} -> {DEST_LAKEHOUSE_ID}/Tables/{DEST_SCHEMA}/{DEST_TABLE} "
          f"(incremental: {WATERMARK_COLUMN} > {WATERMARK_VALUE!r})")
else:
    print(f"Copying {SOURCE_SCHEMA}.{SOURCE_TABLE} -> {DEST_LAKEHOUSE_ID}/Tables/{DEST_SCHEMA}/{DEST_TABLE} (full load)")


# CELL ********************

import struct

try:
    from notebookutils import credentials as _nb_credentials
except ImportError:
    _nb_credentials = None  # allows py_compile / unit tests outside a Fabric runtime

SQL_COPT_SS_ACCESS_TOKEN = 1256

def connect_with_aad_token(server, database, driver=ODBC_DRIVER):
    """AAD-token connection: authenticates as whatever identity this notebook
    is running as, with no interactive prompt and no stored secret -- POC
    shortcut, not a substitute for a real service-principal/MSI setup once
    this moves past manual testing. Requires that identity to already have an
    AAD-based login/user on the target Azure SQL database."""
    token = _nb_credentials.getToken("https://database.windows.net/").encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    connstr = (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(connstr, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


# CELL ********************

import pyodbc

# ODBC SQL type -151 (SQL_SS_UDT) covers geography/geometry/hierarchyid -- these
# are CLR-based types that pyodbc/pandas can't decode from a plain SELECT *
# ("ODBC SQL type -151 is not yet supported"). Any such column is explicitly
# CONVERTed to its string form (WKT for spatial types, the string path for
# hierarchyid) before the read, so this notebook can still land the table.
UDT_TYPES = {"geography", "geometry", "hierarchyid"}

with connect_with_aad_token(SOURCE_SERVER, SOURCE_DATABASE) as conn:
    cols_df = pd.read_sql(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        conn, params=[SOURCE_SCHEMA, SOURCE_TABLE],
    )
    select_list = ", ".join(
        f"CONVERT(NVARCHAR(MAX), [{r.COLUMN_NAME}]) AS [{r.COLUMN_NAME}]"
        if r.DATA_TYPE in UDT_TYPES else f"[{r.COLUMN_NAME}]"
        for r in cols_df.itertuples()
    )
    query = f"SELECT {select_list} FROM [{SOURCE_SCHEMA}].[{SOURCE_TABLE}]"
    read_params = []
    if INCREMENTAL:
        # WATERMARK_COLUMN names an identifier (bracket-quoted like SOURCE_SCHEMA/
        # SOURCE_TABLE above, both driven by the same trusted orch.Tasks metadata,
        # not end-user input); WATERMARK_VALUE is a value, so it's bound as a
        # parameter rather than interpolated.
        query += f" WHERE [{WATERMARK_COLUMN}] > ?"
        read_params.append(WATERMARK_VALUE)
    df = pd.read_sql(query, conn, params=read_params or None)

print(f"Read {len(df)} rows, {len(df.columns)} columns from {SOURCE_SCHEMA}.{SOURCE_TABLE}")


# CELL ********************

# Full overwrite for a full load; append for an incremental one (the rows read
# above are already filtered to just the new/changed ones in that case, so
# overwriting would discard everything already landed). A future upsert
# version could MERGE on write instead, driven by extra ParametersJson fields,
# for sources where the same key can reappear with an updated watermark value.
#
# Written to an explicit OneLake path -- not saveAsTable against an attached
# default lakehouse -- so this notebook can target *any* lakehouse the
# running identity has access to, entirely driven by ParametersJson. That's
# the point of the Wave Runner MDF framework: source and destination are
# metadata in the Tasks table, not notebook configuration.
dest_path = (
    f"abfss://{DEST_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{DEST_LAKEHOUSE_ID}/Tables/{DEST_SCHEMA}/{DEST_TABLE}"
)
WRITE_MODE = "append" if INCREMENTAL else "overwrite"
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode(WRITE_MODE).save(dest_path)

print(f"Wrote {DEST_TABLE} to {dest_path} ({spark_df.count()} rows, mode={WRITE_MODE})")


# CELL ********************

# Hand the new high-water value back to the caller. This notebook only computes
# it -- it has no connection to db_Metadata and doesn't write orch.TaskWatermark
# itself (that stays owned by pipeline activities, same as every other
# orch.*/log.* read or write in this framework). Advancing the stored watermark
# from this exit value, and only after the Task's overall run is confirmed
# successful (see the Previous*/current split on orch.TaskWatermark), is
# follow-up pipeline work, not implemented yet.
try:
    import notebookutils
except ImportError:
    notebookutils = None  # allows py_compile / unit tests outside a Fabric runtime

if INCREMENTAL:
    if len(df) > 0:
        new_watermark_value = df[WATERMARK_COLUMN].max()
        # pandas/numpy scalars (Timestamp, int64, ...) aren't JSON-serializable
        # as-is -- normalize to a plain str/int/float first.
        if hasattr(new_watermark_value, "isoformat"):
            new_watermark_value = new_watermark_value.isoformat()
        elif hasattr(new_watermark_value, "item"):
            new_watermark_value = new_watermark_value.item()
        print(f"New watermark candidate for {WATERMARK_COLUMN}: {new_watermark_value}")
    else:
        new_watermark_value = None
        print("No rows read -- watermark unchanged.")

    exit_payload = json.dumps({
        "watermarkColumn": WATERMARK_COLUMN,
        "previousWatermarkValue": WATERMARK_VALUE,
        "newWatermarkValue": new_watermark_value,
    })
    if notebookutils is not None:
        notebookutils.notebook.exit(exit_payload)
    else:
        print(f"notebookutils unavailable (not running in a Fabric session) -- would exit with: {exit_payload}")


# MARKDOWN ********************

# ## Next steps
# 
# - Register this notebook once in `orch.ObjectIDs` (WorkspaceName/ObjectName -> its
#   real notebook ID + workspace ID, from the Fabric portal after import).
# - One `orch.Tasks` row per source table, all with the same `ObjectName` (this
#   notebook), each with its own `ParametersJson`
#   (`{"sourceSchema":"dbo","sourceTable":"...","destWorkspaceId":"...","destLakehouseId":"...","destSchema":"dbo"}`) --
#   destination is per-Task data, not something set on the notebook.
# - If the destination lakehouse is schema-enabled, `destSchema` (default "dbo")
#   must be set correctly, or Fabric won't recognize the written table at all --
#   it'll show as an "Unidentified" orphan folder instead of a table. Any table
#   already written before this fix landed at the old `Tables/<name>` path (no
#   schema segment) and needs that stale folder deleted before re-running, since
#   `mode("overwrite")` targets a path, not a table name -- writing to the
#   correct `Tables/dbo/<name>` path won't clean up the old orphaned one.
# - Incremental loads: pass `watermarkColumn`/`watermarkValue` in `ParametersJson` and this
#   notebook filters the read, appends instead of overwriting, and exits with the new
#   high-water value as JSON. `orch.TaskWatermark` (with its `Previous*` columns) is where
#   that value should land, but nothing yet calls a proc to actually store it there --
#   `pl_Task_Executor` needs a step added that reads this notebook's exit value and
#   advances `orch.TaskWatermark` only after the Task's overall run succeeds.
# - Upsert follow-up: `MERGE` on write, for sources where a previously-seen key can
#   reappear with a newer watermark value instead of always being a new row.

