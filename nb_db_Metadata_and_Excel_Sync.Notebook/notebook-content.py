# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a4fdbdb8-cb64-4977-80e7-7b48d17bb176",
# META       "default_lakehouse_name": "lh_MetadataFileSync",
# META       "default_lakehouse_workspace_id": "b02fe846-4992-48f9-ab2a-d46eae118816",
# META       "known_lakehouses": [
# META         {
# META           "id": "a4fdbdb8-cb64-4977-80e7-7b48d17bb176"
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

# # nb_db_Metadata_and_Excel_Sync
# Two-way sync between the `db_Metadata` Fabric SQL database (`orch` schema) and the
# `orch_metadata.xlsx` workbook in this folder.
# This replaces `nb_Metadata_Sync` -- recreated under this name rather than renamed
# in place, since Fabric's Git integration doesn't have a documented, reliable
# rename path for notebooks and community reports show real cases of a
# git-connected rename desyncing the workspace from the repo. `nb_Metadata_Sync`
# wasn't referenced by Object ID anywhere (no pipeline activity, no `orch.Tasks`/
# `orch.Jobs` row pointed at it), so nothing needed remapping by GUID -- only its
# two name-only mentions (the data dictionary and its own `.ipynb` mirror) moved
# over. Delete the old `nb_Metadata_Sync.Notebook` folder's workspace item once
# this one is confirmed working.
# 
# ### This needs:
# - `pip install pyodbc openpyxl`
# - **ODBC Driver 18 for SQL Server** installed (required for `Authentication=ActiveDirectoryInteractive`)
# - A browser available for the interactive Microsoft Entra ID sign-in prompt
# Three functions, each covering one direction (or neither, for the comparison):
# - `SyncSQLToExcel()` -- DB -> Excel. Clears each managed sheet's table and
#   repopulates it from the matching `orch` table. Full overwrite of the workbook.
# - `SyncExcelToSQL()` -- Excel -> DB. Deletes every row in each managed `orch`
#   table and reinserts every row from the matching sheet. Full overwrite of the
#   database; requires `confirm=True` since there's no partial/dry-run mode.
# - `CompareSQLAndExcel()` -- read-only. Reports what differs between the two
#   sides per table (rows only in SQL, rows only in Excel, rows present in both
#   with different column values) without changing either one.
# Earlier versions of this notebook (`pull_metadata_to_excel` / `push_excel_to_metadata`)
# did an incremental upsert+diff instead of a full replace, and also carried a
# one-off cell that regenerated `orch.Tasks` from a specific source database
# (`ContosoDW-DEV`). Both were dropped here to keep this notebook to exactly its
# stated job -- ask if you want that one-off relocated somewhere instead of lost.
# **Not managed here:** `orch.TaskWatermark` (runtime execution state written by
# the pipelines) and the `log.TaskRunEvent` run-history table are not
# hand-authored metadata, so neither has a sheet in this workbook. No table in
# the `log` schema carries a foreign key into `orch.Jobs`/`orch.Tasks` anymore --
# `FK_JobRunEvent_JobName`, `FK_TaskRunEvent_JobName`, and `FK_TaskRunEvent_TaskName`
# were all dropped from the schema, so those columns are denormalized/unenforced
# now, same convention as `ActivityRunEvent.TaskName` always was. The one
# survivor is `orch.TaskWatermark.TaskName`, which still carries a real foreign
# key into `orch.Tasks`. Rather than relaxing that constraint around the
# delete+reinsert below, `SyncExcelToSQL` deletes every `orch.TaskWatermark` row
# outright before deleting `Tasks`/`Jobs` -- a full resync wipes watermark
# history for every Task, not just ones that were renamed/removed, but
# `orch.spAdvanceTaskWatermark` now auto-seeds a fresh initial row the next time
# each watermark-tracked Task runs, so this is a one-time reset per sync rather
# than lost functionality.
# If a Task was actually renamed or removed in Excel and `TaskWatermark` still
# references the old name, that re-validation fails and the
# whole sync rolls back -- clean up (or restore) the old name and retry.


# CELL ********************

import datetime as dt
import struct

import pyodbc
import requests
import notebookutils
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import range_boundaries

# --- Connection --------------------------------------------------------------
# Auth is handled non-interactively via get_connection() (Fabric notebook
# identity token), not via an Authentication= keyword here -- the notebook
# runs headless, so an interactive browser sign-in prompt would just hang.
#
# Resolved dynamically against whichever workspace this notebook is actually
# running in, via the Fabric REST API -- this used to be a hardcoded string
# pointing at one specific workspace's db_Metadata (Wave MDF CentralHub's),
# which silently cross-connected this notebook to the wrong database whenever
# it ran anywhere else (e.g. when this same notebook was deployed into
# DMI Sensing CentralHub DEV).
def _resolve_default_connection_string(database_name="db_Metadata"):
    workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sqlDatabases",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    matches = [db for db in resp.json()["value"] if db["displayName"] == database_name]
    if not matches:
        raise RuntimeError(
            f"No SQL database named '{database_name}' found in workspace {workspace_id}."
        )
    return matches[0]["properties"]["connectionString"]

DEFAULT_CONNECTION_STRING = _resolve_default_connection_string()

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
WORKBOOK_PATH = "/lakehouse/default/Files/MetadataSyncFiles/orch_metadata.xlsx"


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


def _resolve_object_ids(distinct_pairs):
    """Resolve each (WorkspaceName, ObjectName) pair to its real Fabric
    WorkspaceID/ObjectID via the REST API, by display name. Raises if a
    workspace or object name doesn't resolve to exactly one match, rather
    than silently writing a wrong/placeholder ID -- ObjectIDs is only ever
    machine-derived (see SyncExcelToSQL), never hand-authored, so a bad
    lookup here should stop the sync, not produce bad data downstream."""
    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
    headers = {"Authorization": f"Bearer {token}"}
    workspace_ids = {}
    resolved = []
    for workspace_name, object_name in distinct_pairs:
        if workspace_name not in workspace_ids:
            resp = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers)
            resp.raise_for_status()
            matches = [w for w in resp.json()["value"] if w["displayName"] == workspace_name]
            if len(matches) != 1:
                raise RuntimeError(
                    f"Expected exactly one workspace named '{workspace_name}', found {len(matches)}."
                )
            workspace_ids[workspace_name] = matches[0]["id"]
        workspace_id = workspace_ids[workspace_name]

        resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items", headers=headers)
        resp.raise_for_status()
        matches = [i for i in resp.json()["value"] if i["displayName"] == object_name]
        if len(matches) != 1:
            raise RuntimeError(
                f"Expected exactly one item named '{object_name}' in workspace "
                f"'{workspace_name}', found {len(matches)}."
            )

        resolved.append({
            "WorkspaceName": workspace_name,
            "ObjectName": object_name,
            "ObjectID": matches[0]["id"],
            "WorkspaceID": workspace_id,
        })
    return resolved


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Column order matches db_Metadata.SQLDatabase/orch/Tables/*.sql. This order is
# parent-first -- safe for SyncExcelToSQL's inserts; reversed, it's child-first
# -- safe for its deletes. (Doesn't need to match the workbook's sheet tab
# order, which is Jobs/Tasks/JobDependencies/TaskDependencies/DependencyCondition/
# TaskType/ObjectIDs -- sheets are looked up by name, not position.)

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
        "sheet": "DependencyCondition", "schema": "orch", "table": "DependencyCondition",
        "pk": ["DependencyCondition"],
        "columns": ["DependencyCondition"],
        "bit_columns": [],
        "time_columns": [],
    },
    {
        "sheet": "Jobs", "schema": "orch", "table": "Jobs",
        "pk": ["JobName"],
        "columns": ["JobName", "Include", "TimeoutInSeconds", "Retries",
                    "RetryIntervalInSeconds", "ParallelBatchLimit", "ScheduledStartUTC",
                    "ParametersJson", "Dependencies", "WorkspaceName", "Environment",
                    "LoggingLevel"],
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

def _replace_table_rows(ws, table_name, columns, rows):
    """Clear an openpyxl Table's existing data rows and write `rows` (list of
    dicts) in their place, resizing the table's ref to fit. Header
    formatting/comments and the sheet's data validations (defined on fixed row
    ranges) are left untouched. A full replace, not a merge -- whatever was
    there before is gone."""
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

def SyncSQLToExcel(workbook_path=WORKBOOK_PATH, conn=None):
    """DB -> Excel. Clears the contents of every managed sheet in the workbook
    and repopulates each one from its matching orch table. Full overwrite --
    there's no merge, so any hand edits made to the workbook since the last
    sync are discarded, not preserved."""
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
            _replace_table_rows(ws, f"tbl{spec['sheet']}", spec["columns"], db_rows)
            summary[spec["table"]] = len(db_rows)

        wb.save(workbook_path)
    finally:
        if owns_conn:
            db_conn.close()
    print(summary)
    return summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def SyncExcelToSQL(workbook_path=WORKBOOK_PATH, conn=None, confirm=False):
    """Excel -> DB. Deletes every row in every managed orch table, then
    inserts every row currently in the matching worksheet. Full replace, not
    an incremental diff -- an empty sheet means an empty table.

    SQL Server refuses a literal TRUNCATE TABLE on any table referenced by a
    FOREIGN KEY constraint (true of most tables here, regardless of whether
    the referencing table currently has rows), so this uses DELETE FROM with
    no WHERE clause in FK-safe child-first order instead -- same net effect.

    orch.ObjectIDs is the one managed table not taken from its own sheet as
    written: whatever's in the ObjectIDs sheet is ignored, and the sheet's
    rows are replaced by fresh ones -- one per distinct (WorkspaceName,
    ObjectName) pair actually referenced by Tasks, each resolved to its real
    Fabric IDs live via the REST API (_resolve_object_ids). Hand-maintained
    ObjectIDs rows drift silently (stale, missing, or -- as found the first
    time this ran -- entirely empty while every Task still referenced a row
    that was never added), so this table is now always machine-derived
    instead, cleared and rebuilt on every sync.

    Because this unconditionally discards the database's current contents for
    every managed table, it refuses to run unless confirm=True.

    No table in the log schema carries an FK into orch.Jobs/orch.Tasks anymore
    (FK_JobRunEvent_JobName, FK_TaskRunEvent_JobName, and FK_TaskRunEvent_TaskName
    were all dropped -- those columns are now denormalized/unenforced, same
    convention as ActivityRunEvent.TaskName always was), so log-schema history
    never blocks this function's deletes. orch.TaskWatermark is the one
    remaining external FK, and it isn't managed by this notebook (see the notes
    at the top) -- rather than relaxing FK_TaskWatermark_Task around the
    delete+reinsert below, every row in orch.TaskWatermark is deleted outright
    before Tasks/Jobs, the same as if it were just another child table in
    TABLE_SPECS. This wipes watermark history for every Task on every full
    resync, not just ones renamed/removed in Excel -- orch.spAdvanceTaskWatermark
    auto-seeds a fresh initial row the next time each watermark-tracked Task
    runs, so this is a one-time reset rather than lost functionality.
    """
    if not confirm:
        raise ValueError(
            "SyncExcelToSQL replaces every row in every managed orch table with "
            "whatever's currently in the workbook. Pass confirm=True to actually run it."
        )

    wb = openpyxl.load_workbook(workbook_path, data_only=True)
    parsed = {}
    for spec in TABLE_SPECS:
        ws = wb[spec["sheet"]]
        rows = _read_table_rows(ws, f"tbl{spec['sheet']}")
        for row in rows:
            for bit_col in spec.get("bit_columns", []):
                row[bit_col] = _normalize_bit(row.get(bit_col))
            for time_col in spec.get("time_columns", []):
                row[time_col] = _normalize_time(row.get(time_col))
        parsed[spec["table"]] = rows

    distinct_pairs = sorted({
        (row.get("WorkspaceName"), row.get("ObjectName"))
        for row in parsed["Tasks"]
        if row.get("WorkspaceName") and row.get("ObjectName")
    })
    parsed["ObjectIDs"] = _resolve_object_ids(distinct_pairs)

    db_conn, owns_conn = get_connection(conn)
    summary = {}
    try:
        cur = db_conn.cursor()

        # orch.TaskWatermark isn't part of TABLE_SPECS (it's runtime state, not
        # hand-authored metadata) but still carries a real FK_TaskWatermark_Task into
        # orch.Tasks, so it has to be cleared before Tasks/Jobs are deleted below, not after.
        cur.execute("DELETE FROM [orch].[TaskWatermark]")

        # Delete-all, child-first (reverse of TABLE_SPECS' parent-first order).
        for spec in reversed(TABLE_SPECS):
            cur.execute(f"DELETE FROM [{spec['schema']}].[{spec['table']}]")

        # Insert-all, parent-first.
        for spec in TABLE_SPECS:
            rows = parsed[spec["table"]]
            if not rows:
                summary[spec["table"]] = 0
                continue
            cols = spec["columns"]
            cols_sql = ", ".join(f"[{c}]" for c in cols)
            placeholders = ", ".join(["?"] * len(cols))
            cur.executemany(
                f"INSERT INTO [{spec['schema']}].[{spec['table']}] ({cols_sql}) VALUES ({placeholders})",
                [[row.get(c) for c in cols] for row in rows],
            )
            summary[spec["table"]] = len(rows)

        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        if owns_conn:
            db_conn.close()
    print(summary)
    return summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def CompareSQLAndExcel(workbook_path=WORKBOOK_PATH, conn=None):
    """Read-only. For each managed table, diffs the database's current rows
    against the workbook's, keyed by primary key, and returns:

        {table_name: {"only_in_sql": [...], "only_in_excel": [...],
                       "differing": [{"pk": {...}, "changes": {col: (sql_val, excel_val)}}]}}

    Prints a one-line summary per table. Never writes to either side."""
    wb = openpyxl.load_workbook(workbook_path, data_only=True)
    db_conn, owns_conn = get_connection(conn)
    diffs = {}
    try:
        cur = db_conn.cursor()
        for spec in TABLE_SPECS:
            pk = spec["pk"]
            cols = spec["columns"]
            cols_sql = ", ".join(f"[{c}]" for c in cols)
            cur.execute(f"SELECT {cols_sql} FROM [{spec['schema']}].[{spec['table']}]")
            sql_rows = [dict(zip(cols, row)) for row in cur.fetchall()]

            ws = wb[spec["sheet"]]
            excel_rows = _read_table_rows(ws, f"tbl{spec['sheet']}")
            for row in excel_rows:
                for bit_col in spec.get("bit_columns", []):
                    row[bit_col] = _normalize_bit(row.get(bit_col))
                for time_col in spec.get("time_columns", []):
                    row[time_col] = _normalize_time(row.get(time_col))

            def _key(row, pk=pk):
                return tuple(row.get(k) for k in pk)

            sql_by_key = {_key(r): r for r in sql_rows}
            excel_by_key = {_key(r): r for r in excel_rows}

            only_in_sql = [sql_by_key[k] for k in sql_by_key.keys() - excel_by_key.keys()]
            only_in_excel = [excel_by_key[k] for k in excel_by_key.keys() - sql_by_key.keys()]
            differing = []
            for k in sql_by_key.keys() & excel_by_key.keys():
                sql_row, excel_row = sql_by_key[k], excel_by_key[k]
                changes = {
                    c: (sql_row.get(c), excel_row.get(c))
                    for c in cols
                    if c not in pk and sql_row.get(c) != excel_row.get(c)
                }
                if changes:
                    differing.append({"pk": dict(zip(pk, k)), "changes": changes})

            diffs[spec["table"]] = {
                "only_in_sql": only_in_sql,
                "only_in_excel": only_in_excel,
                "differing": differing,
            }
            print(f"{spec['table']}: {len(only_in_sql)} only in SQL, "
                  f"{len(only_in_excel)} only in Excel, {len(differing)} differing")
    finally:
        if owns_conn:
            db_conn.close()
    return diffs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ShowExcelJobTasks(workbook_path=WORKBOOK_PATH):
    """Read-only. Displays the workbook's Jobs and Tasks sheets as two
    separate tables, exactly as they currently exist in orch_metadata.xlsx --
    doesn't touch the database or the workbook."""
    import pandas as pd

    wb = openpyxl.load_workbook(workbook_path, data_only=True)
    for table_name in ("Jobs", "Tasks"):
        spec = next(s for s in TABLE_SPECS if s["table"] == table_name)
        ws = wb[spec["sheet"]]
        rows = _read_table_rows(ws, f"tbl{spec['sheet']}")
        df = pd.DataFrame(rows, columns=spec["columns"])
        print(f"--- {table_name} ({len(df)} rows) ---")
        display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Usage - Comment / un-comment one function as needed

# CELL ********************

# See what's actually different before touching either side.

#CompareSQLAndExcel()

# Database -> Excel: overwrite the workbook with the database's current state.
#SyncSQLToExcel()

# Edit orch_metadata.xlsx by hand: add/change/remove rows in each sheet...
# Excel -> database: overwrite the database with the workbook's current state.
# Left requiring confirm=True on purpose -- there's no dry_run here, so an
# unattended/scheduled run of this notebook can't silently wipe the database
# just because a cell executed.
SyncExcelToSQL(confirm=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
