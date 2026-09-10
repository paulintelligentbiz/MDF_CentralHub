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
# 2. Make sure `pyodbc` and the **ODBC Driver 18 for SQL Server** are available in the
#    attached environment (`%pip install pyodbc` in a cell if needed).
# 3. `pl_Task_Executor`'s `Run Notebook` activity needs to forward `ParametersJson` as
#    a base parameter (see the accompanying pipeline fix) so this notebook actually
#    receives the table name at runtime.
# 
# **Auth note:** `Authentication=Active Directory Interactive` (below) opens a
# browser sign-in and is fine for you running this manually while signed in, which
# is exactly this test. It is **not** suitable for an unattended/scheduled pipeline
# run -- for that, swap the connection to `ActiveDirectoryServicePrincipal` or
# `ActiveDirectoryMsi` once this framework moves past manual testing.


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
SOURCE_CONNECTION_STRING = (
    "Data Source=paulsdemos.database.windows.net;"
    "Initial Catalog=ContosoDW-DEV;"
    "Persist Security Info=False;"
    "User ID=paul@intelligentbiz.net;"
    "Pooling=False;"
    "MultipleActiveResultSets=False;"
    "Connect Timeout=30;"
    "Encrypt=True;"
    "Trust Server Certificate=True;"
    "Authentication=Active Directory Interactive;"
    "Command Timeout=0"
)

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"

print(f"Copying {SOURCE_SCHEMA}.{SOURCE_TABLE} -> {DEST_LAKEHOUSE_ID}/Tables/{DEST_TABLE}")


# CELL ********************

def parse_ado_connection_string(conn_str):
    parts = {}
    for chunk in conn_str.split(";"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        parts[key.strip().lower()] = value.strip()
    return parts

def build_pyodbc_connection_string(ado_conn_str, driver=ODBC_DRIVER):
    p = parse_ado_connection_string(ado_conn_str)
    server = p.get("data source")
    database = p.get("initial catalog")
    encrypt = "yes" if p.get("encrypt", "true").lower() == "true" else "no"
    trust_cert = "yes" if p.get("trust server certificate", "false").lower() == "true" else "no"
    timeout = p.get("connect timeout", "30")
    auth = p.get("authentication", "Active Directory Interactive").replace(" ", "")

    return (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server};"
        f"Database={database};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
        f"Authentication={auth};"
    )


# CELL ********************

import pyodbc

pyodbc_connstr = build_pyodbc_connection_string(SOURCE_CONNECTION_STRING)
with pyodbc.connect(pyodbc_connstr) as conn:
    df = pd.read_sql(f"SELECT * FROM [{SOURCE_SCHEMA}].[{SOURCE_TABLE}]", conn)

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

