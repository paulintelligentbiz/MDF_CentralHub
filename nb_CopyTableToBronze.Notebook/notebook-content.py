# Fabric notebook source


# MARKDOWN ********************

# # nb_CopyTableToBronze
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
ParametersJson = (
    '{"sourceSchema": "dbo", "sourceTable": "REPLACE_ME", '
    '"destWorkspaceId": "947d3136-33ac-458a-be73-ac7dc38afaa5", '
    '"destLakehouseId": "649b7795-2e22-4627-8b25-9749a6f492f0"}'
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

# "source connection" (ContosoDW-DEV) -- see /topics/database-connections.md
SOURCE_SERVER = "paulsdemos.database.windows.net"
SOURCE_DATABASE = "ContosoDW-DEV"
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"

print(f"Copying {SOURCE_SCHEMA}.{SOURCE_TABLE} -> {DEST_LAKEHOUSE_ID}/Tables/{DEST_TABLE}")


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
    df = pd.read_sql(f"SELECT {select_list} FROM [{SOURCE_SCHEMA}].[{SOURCE_TABLE}]", conn)

print(f"Read {len(df)} rows, {len(df.columns)} columns from {SOURCE_SCHEMA}.{SOURCE_TABLE}")


# CELL ********************

# Full overwrite for this initial test. A future incremental version of this
# notebook would branch here: append + watermark filter on the read side, or
# a MERGE/upsert on write, driven by extra fields in ParametersJson.
#
# Written to an explicit OneLake path -- not saveAsTable against an attached
# default lakehouse -- so this notebook can target *any* lakehouse the
# running identity has access to, entirely driven by ParametersJson. That's
# the point of the Wave Runner MDF framework: source and destination are
# metadata in the Tasks table, not notebook configuration.
dest_path = (
    f"abfss://{DEST_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{DEST_LAKEHOUSE_ID}/Tables/{DEST_TABLE}"
)
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite").save(dest_path)

print(f"Wrote {DEST_TABLE} to {dest_path} ({spark_df.count()} rows)")


# MARKDOWN ********************

# ## Next steps
# 
# - Register this notebook once in `orch.ObjectIDs` (WorkspaceName/ObjectName -> its
#   real notebook ID + workspace ID, from the Fabric portal after import).
# - One `orch.Tasks` row per source table, all with the same `ObjectName` (this
#   notebook), each with its own `ParametersJson`
#   (`{"sourceSchema":"dbo","sourceTable":"...","destWorkspaceId":"...","destLakehouseId":"..."}`) --
#   destination is per-Task data, not something set on the notebook.
# - Incremental follow-up: add a watermark column + last-value tracking (a small
#   control table, or `MERGE` on write) once the full-load test is confirmed working.

