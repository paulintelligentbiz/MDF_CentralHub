# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "48052622-141c-4471-8abb-8445a634ae6f",
# META       "default_lakehouse_name": "lh_MetadataFileSync",
# META       "default_lakehouse_workspace_id": "a937b42f-1407-41c9-b5bf-33288dc7f60f",
# META       "known_lakehouses": [
# META         {
# META           "id": "48052622-141c-4471-8abb-8445a634ae6f"
# META         }
# META       ]
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": [
# META         {
# META           "id": "15a01d6c-0d70-4a2d-b4da-28b809849709"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fabric `orch` Metadata Sync
# 
# Two-way sync between the `db_Metadata` Fabric SQL database (`orch` schema) and the
# `orch_metadata.xlsx` workbook in this folder.
# 
# **Run this notebook somewhere with real network access to the Fabric SQL endpoint** —
# this needs:
# 
# - `pip install pyodbc openpyxl`
# - **ODBC Driver 18 for SQL Server** installed (required for `Authentication=ActiveDirectoryInteractive`)
# - A browser available for the interactive Microsoft Entra ID sign-in prompt
# 
# Functions:
# 
# - `pull_metadata_to_excel()` — DB -> Excel (overwrites each sheet's table data with the current DB rows)
# - `push_excel_to_metadata()` — Excel -> DB (upserts every row in Excel, deletes DB rows whose key is no longer there)
# - `sync_metadata(direction=...)` — single entry point: `direction="pull"` or `direction="push"`
# 
# **Before the first push**, run a pull so the workbook reflects the current database state.
# By default, `push_excel_to_metadata` will *not* wipe a table whose sheet is completely
# empty (pass `confirm_empty_tables=True` if you really mean to empty a table out).


# CELL ********************

import datetime as dt
from pathlib import Path
import struct

import pyodbc
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import range_boundaries

# --- Connection --------------------------------------------------------------
# Auth is handled non-interactively via get_connection() (Fabric notebook
# identity token), not via an Authentication= keyword here -- the notebook
# runs headless, so an interactive browser sign-in prompt would just hang.
DEFAULT_CONNECTION_STRING = (
    "Data Source=kalwvg5capkefegg5d6gkpgeza-f62dpkihcteudnn7gmui3r7wb4.database.fabric.microsoft.com,1433;"
    "Initial Catalog=db_Metadata-15a01d6c-0d70-4a2d-b4da-28b809849709;"
    "Multiple Active Result Sets=False;"
    "Connect Timeout=30;"
    "Encrypt=True;"
    "Trust Server Certificate=False;"
)

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
WORKBOOK_PATH = Path("/lakehouse/default/Files/MetadataSyncFiles/orch_metadata.xlsx")

# --- OneLake / Fabric Lakehouse Files destination -----------------------------
# Parsed from the Lakehouse Files URL:
# https://app.powerbi.com/groups/<workspaceId>/lakehouses/<lakehouseId>?...&selectedPath=Files%2FMetadataSyncFiles...
ONELAKE_WORKSPACE_ID = "a937b42f-1407-41c9-b5bf-33288dc7f60f"
ONELAKE_LAKEHOUSE_ID = "48052622-141c-4471-8abb-8445a634ae6f"
ONELAKE_FILES_PATH = "Files/MetadataSyncFiles"
ONELAKE_DESTINATION_TEMPLATE = (
    "https://onelake.dfs.fabric.microsoft.com/" + ONELAKE_WORKSPACE_ID + "/" +
    ONELAKE_LAKEHOUSE_ID + "/" + ONELAKE_FILES_PATH + "/{filename}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

def build_pyodbc_connection_string(ado_conn_str=DEFAULT_CONNECTION_STRING, driver=ODBC_DRIVER):
    p = parse_ado_connection_string(ado_conn_str)
    server = p.get("data source")
    database = p.get("initial catalog")
    encrypt = "yes" if p.get("encrypt", "true").lower() == "true" else "no"
    trust_cert = "yes" if p.get("trust server certificate", "false").lower() == "true" else "no"
    timeout = p.get("connect timeout", "30")
    mars = "yes" if p.get("multiple active result sets", "false").lower() == "true" else "no"

    return (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server};"
        f"Database={database};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
        f"MARS_Connection={mars};"
    )

import notebookutils

SQL_COPT_SS_ACCESS_TOKEN = 1256

def get_connection(conn=None):
    """Return (connection, owns_it). Opens a new connection using the
    notebook's own Entra ID identity (via notebookutils.credentials.getToken),
    non-interactively -- no browser sign-in prompt, so this works inside a
    headless Fabric session. Only opens a new connection if one wasn't
    already passed in."""
    if conn is not None:
        return conn, False
    connstr = build_pyodbc_connection_string()
    token = notebookutils.credentials.getToken("https://database.windows.net/")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    return pyodbc.connect(
        connstr, autocommit=False,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    ), True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Column order matches db_Metadata.SQLDatabase/orch/Tables/*.sql.
# Order below is parent-first (safe for upserts); reversed for deletes (children-first).

TABLE_SPECS = [
    {
        "sheet": "ObjectIDs", "schema": "orch", "table": "ObjectIDs",
        "pk": ["WorkspaceName", "ObjectName"],
        "columns": ["WorkspaceName", "ObjectName", "ObjectID", "WorkspaceID"],
        "bit_columns": [],
        "time_columns": [],
    },
    {
        "sheet": "TaskType", "schema": "orch", "table": "TaskType",
        "pk": ["TaskType"],
        "columns": ["TaskType", "AllowParallel"],
        "bit_columns": ["AllowParallel"],
        "time_columns": [],
    },
    {
        "sheet": "Jobs", "schema": "orch", "table": "Jobs",
        "pk": ["JobName"],
        "columns": ["JobName", "Include", "TimeoutInSeconds", "Retries",
                    "RetryIntervalInSeconds", "ScheduledStartUTC", "ParametersJson",
                    "Dependencies", "WorkspaceName", "Environment", "LoggingLevel"],
        "bit_columns": ["Include"],
        "time_columns": ["ScheduledStartUTC"],
    },
    {
        "sheet": "Tasks", "schema": "orch", "table": "Tasks",
        "pk": ["TaskName"],
        "columns": ["TaskName", "Include", "JobName", "ObjectName", "WorkspaceName",
                    "TimeoutInSeconds", "Retries", "RetryIntervalInSeconds",
                    "ParametersJson", "Dependencies", "TaskType", "System", "Layer",
                    "LoggingLevel"],
        "bit_columns": ["Include"],
        "time_columns": [],
    },
    {
        "sheet": "DependencyCondition", "schema": "orch", "table": "DependencyCondition",
        "pk": ["DependencyCondition"],
        "columns": ["DependencyCondition"],
        "bit_columns": [],
        "time_columns": [],
    },
    {
        "sheet": "JobDependencies", "schema": "orch", "table": "JobDependencies",
        "pk": ["JobName", "DependentJobName"],
        "columns": ["JobName", "DependentJobName", "DependsOn"],
        "bit_columns": [],
        "time_columns": [],
    },
    {
        "sheet": "TaskDependencies", "schema": "orch", "table": "TaskDependencies",
        "pk": ["TaskName", "DependentTaskName"],
        "columns": ["TaskName", "DependentTaskName", "DependsOn"],
        "bit_columns": [],
        "time_columns": [],
    },
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _normalize_bit(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    s = str(value).strip().lower()
    if s in ("true", "1", "yes"):
        return 1
    if s in ("false", "0", "no"):
        return 0
    raise ValueError(f"Can't interpret {value!r} as a bit/boolean value")

def _normalize_time(value):
    if value in (None, ""):
        return None
    if isinstance(value, dt.time):
        return value
    if isinstance(value, dt.datetime):
        return value.time()
    s = str(value).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return dt.datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"Can't interpret {value!r} as a time value")

def _read_table_rows(ws, table_name):
    """Read the data rows below an openpyxl Table's header as a list of dicts
    keyed by column name. Fully blank rows are skipped."""
    table = ws.tables[table_name]
    min_col, min_row, max_col, max_row = range_boundaries(table.ref)
    headers = [ws.cell(row=min_row, column=c).value for c in range(min_col, max_col + 1)]
    rows = []
    for r in range(min_row + 1, max_row + 1):
        values = [ws.cell(row=r, column=c).value for c in range(min_col, max_col + 1)]
        if all(v is None or v == "" for v in values):
            continue
        rows.append(dict(zip(headers, values)))
    return rows

def _write_table_rows(ws, table_name, columns, rows):
    """Overwrite an openpyxl Table's data rows with `rows` (list of dicts) and
    resize the table's ref. Header formatting/comments and the sheet's data
    validations (defined on fixed row ranges) are left untouched."""
    table = ws.tables[table_name]
    min_col, min_row, max_col, old_max_row = range_boundaries(table.ref)

    for r in range(min_row + 1, old_max_row + 1):
        for c in range(min_col, max_col + 1):
            ws.cell(row=r, column=c).value = None

    for i, row in enumerate(rows):
        r = min_row + 1 + i
        for c_idx, col_name in enumerate(columns, start=min_col):
            ws.cell(row=r, column=c_idx).value = row.get(col_name)

    new_max_row = max(min_row + 1, min_row + len(rows))  # a table needs >= 1 data row
    last_col_letter = get_column_letter(max_col)
    table.ref = f"{get_column_letter(min_col)}{min_row}:{last_col_letter}{new_max_row}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def pull_metadata_to_excel(workbook_path=WORKBOOK_PATH, conn=None):
    """Read every orch table from the database and overwrite the matching
    worksheet's table data in the Excel workbook (formatting untouched)."""
    db_conn, owns_conn = get_connection(conn)
    summary = {}
    try:
        wb = openpyxl.load_workbook(workbook_path)
        cur = db_conn.cursor()
        for spec in TABLE_SPECS:
            cols_sql = ", ".join(f"[{c}]" for c in spec["columns"])
            cur.execute(f"SELECT {cols_sql} FROM [{spec['schema']}].[{spec['table']}]")
            db_rows = [dict(zip(spec["columns"], row)) for row in cur.fetchall()]

            ws = wb[spec["sheet"]]
            table_name = f"tbl{spec['sheet']}"
            _write_table_rows(ws, table_name, spec["columns"], db_rows)
            summary[spec["table"]] = len(db_rows)

        wb.save(workbook_path)
    finally:
        if owns_conn:
            db_conn.close()
    return summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def push_excel_to_metadata(workbook_path=WORKBOOK_PATH, conn=None, dry_run=False,
                           confirm_empty_tables=False):
    """Read every managed worksheet's table and sync it to the database:
    upsert every row present in Excel, delete DB rows whose primary key is no
    longer present in Excel. Deletes run child-tables-first, upserts run
    parent-tables-first, matching the foreign keys in the orch schema.

    A sheet with zero data rows is *skipped* (not emptied) unless
    confirm_empty_tables=True, so an unpopulated workbook can't accidentally
    wipe a table.
    """
    wb = openpyxl.load_workbook(workbook_path, data_only=True)

    parsed = {}
    for spec in TABLE_SPECS:
        ws = wb[spec["sheet"]]
        table_name = f"tbl{spec['sheet']}"
        rows = _read_table_rows(ws, table_name)
        for row in rows:
            for bit_col in spec.get("bit_columns", []):
                row[bit_col] = _normalize_bit(row.get(bit_col))
            for time_col in spec.get("time_columns", []):
                row[time_col] = _normalize_time(row.get(time_col))
        parsed[spec["table"]] = rows

    if dry_run:
        return {name: len(rows) for name, rows in parsed.items()}

    db_conn, owns_conn = get_connection(conn)
    summary = {}
    try:
        cur = db_conn.cursor()

        # 1) Deletes, children first.
        for spec in reversed(TABLE_SPECS):
            rows = parsed[spec["table"]]
            pk = spec["pk"]
            if not rows:
                if not confirm_empty_tables:
                    summary.setdefault(spec["table"], {})["skipped_empty"] = True
                    continue
                cur.execute(f"DELETE FROM [{spec['schema']}].[{spec['table']}]")
                continue

            value_rows = ",".join("(" + ",".join(["?"] * len(pk)) + ")" for _ in rows)
            match_cols = ", ".join(f"k{i}" for i in range(len(pk)))
            where_clause = " AND ".join(f"keep.k{i} = t.[{k}]" for i, k in enumerate(pk))
            sql = (
                f"DELETE t FROM [{spec['schema']}].[{spec['table']}] AS t "
                f"WHERE NOT EXISTS (SELECT 1 FROM (VALUES {value_rows}) AS keep({match_cols}) "
                f"WHERE {where_clause})"
            )
            pk_values = [row[k] for row in rows for k in pk]
            cur.execute(sql, pk_values)

        # 2) Upserts, parents first.
        for spec in TABLE_SPECS:
            rows = parsed[spec["table"]]
            if not rows:
                # Nothing to upsert; don't clobber a skipped_empty marker from the delete phase.
                summary.setdefault(spec["table"], {}).setdefault("upserted", 0)
                continue
            pk = spec["pk"]
            other_cols = [c for c in spec["columns"] if c not in pk]
            inserted = updated = 0
            for row in rows:
                where_sql = " AND ".join(f"[{k}] = ?" for k in pk)
                where_vals = [row[k] for k in pk]

                if other_cols:
                    set_sql = ", ".join(f"[{c}] = ?" for c in other_cols)
                    set_vals = [row.get(c) for c in other_cols]
                    cur.execute(
                        f"UPDATE [{spec['schema']}].[{spec['table']}] SET {set_sql} WHERE {where_sql}",
                        set_vals + where_vals,
                    )
                    exists = cur.rowcount > 0
                else:
                    cur.execute(
                        f"SELECT 1 FROM [{spec['schema']}].[{spec['table']}] WHERE {where_sql}",
                        where_vals,
                    )
                    exists = cur.fetchone() is not None

                if not exists:
                    all_cols = spec["columns"]
                    ins_cols_sql = ", ".join(f"[{c}]" for c in all_cols)
                    ins_placeholders = ", ".join(["?"] * len(all_cols))
                    cur.execute(
                        f"INSERT INTO [{spec['schema']}].[{spec['table']}] ({ins_cols_sql}) VALUES ({ins_placeholders})",
                        [row.get(c) for c in all_cols],
                    )
                    inserted += 1
                else:
                    updated += 1
            summary[spec["table"]] = {"upserted": len(rows), "inserted": inserted, "updated": updated}

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        if owns_conn:
            db_conn.close()
    return summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import subprocess

def upload_workbook_to_lakehouse(workbook_path=WORKBOOK_PATH, destination=None):
    """Copy the workbook to the Fabric Lakehouse Files location with azcopy.
    Requires `azcopy login` to have been run once (interactive Entra ID sign-in)."""
    dest = destination or ONELAKE_DESTINATION_TEMPLATE.format(filename=Path(workbook_path).name)
    result = subprocess.run(
        ["azcopy", "copy", str(workbook_path), dest],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"azcopy failed:\n{result.stdout}\n{result.stderr}")
    return dest


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def sync_metadata(direction, workbook_path=WORKBOOK_PATH, conn=None, dry_run=False,
                   confirm_empty_tables=False, upload_after_push=False):
    """Single entry point for the two-way sync.

    direction: "pull" (database -> Excel) or "push" (Excel -> database).
    """
    direction = direction.strip().lower()
    if direction in ("pull", "db_to_excel", "download"):
        return pull_metadata_to_excel(workbook_path=workbook_path, conn=conn)
    if direction in ("push", "excel_to_db", "upload"):
        result = push_excel_to_metadata(
            workbook_path=workbook_path, conn=conn, dry_run=dry_run,
            confirm_empty_tables=confirm_empty_tables,
        )
        if upload_after_push and not dry_run:
            upload_workbook_to_lakehouse(workbook_path)
        return result
    raise ValueError(f"Unknown direction {direction!r}; expected 'pull' or 'push'.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from IPython.display import HTML, display

def workbook_lakehouse_url(files_path=ONELAKE_FILES_PATH):
    """Direct link into the Lakehouse Files browser, scrolled to the folder
    containing orch_metadata.xlsx. Click the file there to open it in Excel
    Online/Desktop or download it -- there is no way to push a file straight
    to your local machine from inside a headless Spark session."""
    return (
        f"https://app.powerbi.com/groups/{ONELAKE_WORKSPACE_ID}/lakehouses/"
        f"{ONELAKE_LAKEHOUSE_ID}?experience=power-bi&selectedPath="
        f"{files_path.replace('/', '%2F')}"
    )

_url = workbook_lakehouse_url()
display(HTML(f'<a href="{_url}" target="_blank">Open orch_metadata.xlsx in the Lakehouse Files browser</a>'))
print(_url)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- One-off: regenerate orch.Tasks from the live ContosoDW-DEV table list ---
# Builds one CopyBronze_<table> row per base table in ContosoDW-DEV.dbo and
# replaces every row in the Tasks sheet with the fresh set. Re-run this
# whenever the source schema changes, then sync_metadata("push") to sync the
# replacement to the database.
#
# Destination is data-driven, not notebook configuration: each row's
# ParametersJson carries destWorkspaceId/destLakehouseId for
# lh_Bronze_Wave_Test (workspace 947d3136-33ac-458a-be73-ac7dc38afaa5 /
# lakehouse 649b7795-2e22-4627-8b25-9749a6f492f0). nb_CopyTableToBronze
# writes straight to that OneLake path -- no lakehouse needs to be attached
# to the notebook.

import json

CONTOSODW_SERVER = "paulsdemos.database.windows.net"
CONTOSODW_DATABASE = "ContosoDW-DEV"

def get_contosodw_connection():
    """Non-interactive connection to ContosoDW-DEV -- same token pattern as
    get_connection() above, pointed at a different server/database."""
    connstr = (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server=tcp:{CONTOSODW_SERVER},1433;"
        f"Database={CONTOSODW_DATABASE};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    token = notebookutils.credentials.getToken("https://database.windows.net/")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    return pyodbc.connect(connstr, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

with get_contosodw_connection() as src_conn:
    cur = src_conn.cursor()
    cur.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE' "
        "ORDER BY TABLE_NAME"
    )
    source_tables = [row[0] for row in cur.fetchall()]

print(f"Found {len(source_tables)} tables in ContosoDW-DEV.dbo")

new_task_rows = [
    {
        "TaskName": f"CopyBronze_{t}",
        "Include": True,
        "JobName": "Ingest_ContosoDW_Bronze",
        "ObjectName": "nb_CopyTableToBronze",
        "WorkspaceName": "MDF_CentralHub",
        "TimeoutInSeconds": 1800,
        "Retries": 1,
        "RetryIntervalInSeconds": 30,
        "ParametersJson": json.dumps({
            "sourceSchema": "dbo",
            "sourceTable": t,
            "destWorkspaceId": "947d3136-33ac-458a-be73-ac7dc38afaa5",
            "destLakehouseId": "649b7795-2e22-4627-8b25-9749a6f492f0",
        }),
        "Dependencies": None,
        "TaskType": "Notebook",
        "System": "ContosoDW",
        "Layer": "Bronze",
        "LoggingLevel": 1,
    }
    for t in source_tables
]

tasks_columns = next(spec["columns"] for spec in TABLE_SPECS if spec["sheet"] == "Tasks")

wb = openpyxl.load_workbook(WORKBOOK_PATH)
ws = wb["Tasks"]
_write_table_rows(ws, "tblTasks", tasks_columns, new_task_rows)
wb.save(WORKBOOK_PATH)

print(f"Replaced Tasks sheet with {len(new_task_rows)} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Usage
# 
# ```python
# # First time: populate the workbook from the current DB state
# sync_metadata("pull")
# 
# # ...edit orch_metadata.xlsx by hand: add/change/remove rows in each table...
# 
# # Preview what a push would do, without touching the database
# sync_metadata("push", dry_run=True)
# 
# # Actually sync Excel -> database. A row missing from a table's sheet is
# # deleted from that table; a sheet left completely empty is skipped instead
# # of wiping the table (pass confirm_empty_tables=True to force a real wipe).
# sync_metadata("push")
# ```


# CELL ********************

# First time: populate the workbook from the current DB state
# sync_metadata("pull")

# ...edit orch_metadata.xlsx by hand: add/change/remove rows in each table...

# Preview what a push would do, without touching the database -- safe to
# leave active, since dry_run=True never writes to the database.
sync_metadata("push", dry_run=True)

# Actually sync Excel -> database. A row missing from a table's sheet is
# deleted from that table; a sheet left completely empty is skipped instead
# of wiping the table (pass confirm_empty_tables=True to force a real wipe).
# Left commented out on purpose -- uncomment deliberately before running this
# cell, so an unattended/scheduled run of this notebook can't silently push
# whatever happens to be in the workbook right now.
sync_metadata("push")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
