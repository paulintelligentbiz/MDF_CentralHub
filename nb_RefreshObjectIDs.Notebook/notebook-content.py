# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
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

# # nb_RefreshObjectIDs
# 
# Utility functions for resolving **any Fabric object's GUID by display name** --
# a workspace, or an item inside one (Notebook, DataPipeline, CopyJob, Dataflow,
# Lakehouse, ...) -- plus a routine that uses them to refresh `orch.ObjectIDs` in
# `db_Metadata` from whatever `(WorkspaceName, ObjectName)` pairs are actually
# referenced by `orch.Tasks` (JobNames aren't included -- a Job is just a
# grouping key, not a Fabric item anything looks up by GUID).
# 
# Meant to be invoked as an activity in `pl_Orchestrator_Top_Level` (see the
# pipeline change alongside this notebook) so `ObjectIDs` stays in sync with
# reality before each run, instead of being hand-maintained.
# 
# **Setup:**
# 1. `%pip install pyodbc requests` if either isn't already on the environment.
# 2. No interactive sign-in needed -- this uses `notebookutils.credentials.getToken('pbi')`
#    to get a token for both the Fabric REST API and the `db_Metadata` SQL
#    endpoint, so it's safe to run unattended from a pipeline.
# 3. The calling identity (you, or the pipeline's run-as identity) needs at least
#    Viewer on any workspace being looked up, and read/write on `db_Metadata`.


# PARAMETERS CELL ********************

# Base parameters -- set by pl_Task_Executor/pl_Orchestrator when invoked as an
# activity. onlyWorkspaceName narrows the refresh to one workspace; leave null
# to refresh everything orch.Tasks currently references.
ParametersJson = '{"onlyWorkspaceName": null}'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import struct

import requests

try:
    from notebookutils import credentials as _nb_credentials
except ImportError:
    _nb_credentials = None  # allows py_compile / unit tests outside a Fabric runtime

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_REQUEST_TIMEOUT_SECONDS = 30  # fail fast rather than hang indefinitely -- requests has no default timeout
_token_cache = {}

def _get_pbi_token():
    """Entra token for the Fabric/Power BI REST API audience (also used for the
    db_Metadata SQL endpoint below) -- cached for the notebook session."""
    if "pbi" not in _token_cache:
        _token_cache["pbi"] = _nb_credentials.getToken("pbi")
    return _token_cache["pbi"]

def _fabric_get(path, params=None):
    """GET against the Fabric REST API, following continuationToken pagination.
    Returns the concatenated "value" list across all pages."""
    token = _get_pbi_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{FABRIC_API_BASE}{path}"
    items = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
        params = None  # continuationUri already carries the query string
    return items

def _fabric_get_one(path):
    """GET a single (non-paginated) Fabric REST API resource, e.g. one workspace
    by ID -- much cheaper than _fabric_get's tenant-wide list-and-filter when
    the caller already knows exactly which resource it wants."""
    token = _get_pbi_token()
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{FABRIC_API_BASE}{path}", headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALID_ITEM_TYPES = {
    "Dashboard", "Report", "SemanticModel", "PaginatedReport", "Datamart", "Lakehouse",
    "Eventhouse", "Environment", "KQLDatabase", "KQLQueryset", "KQLDashboard", "DataPipeline",
    "Notebook", "SparkJobDefinition", "MLExperiment", "MLModel", "Warehouse", "Eventstream",
    "SQLEndpoint", "MirroredWarehouse", "MirroredDatabase", "Reflex", "GraphQLApi",
    "MountedDataFactory", "SQLDatabase", "CopyJob", "VariableLibrary", "Dataflow",
}

def get_workspace_id(workspace_name, current_workspace_id=None):
    """Resolve a workspace's GUID from its display name.

    Checks the notebook's own workspace (`current_workspace_id`) first, with a
    single cheap GET on that one workspace -- every object this framework
    currently resolves lives in the workspace the pipeline itself runs in, so
    this fast path is normally the only Fabric API call this function makes at
    all. Only when the name doesn't match the current workspace (or none was
    given) does it fall back to listing every workspace the caller can see and
    filtering by display name -- a tenant-wide scan that gets slower with every
    workspace in the tenant, and was previously the *only* path this function
    had, which is why a refresh could take minutes even though everything it
    was ever looking for was in the workspace it was already running in.

    Workspace names aren't guaranteed unique tenant-wide: in that slow-path
    fallback, if more than one matches, prefers `current_workspace_id` when
    it's among the matches, else returns the first match and prints a warning
    listing all of them so the caller can disambiguate."""
    if current_workspace_id:
        current = _fabric_get_one(f"/workspaces/{current_workspace_id}")
        if current.get("displayName") == workspace_name:
            return current_workspace_id

    matches = [w for w in _fabric_get("/workspaces") if w.get("displayName") == workspace_name]
    if not matches:
        return None
    if len(matches) > 1:
        ids = [w["id"] for w in matches]
        if current_workspace_id in ids:
            return current_workspace_id
        print(f"WARNING: {len(matches)} workspaces named {workspace_name!r}: {ids}. "
              f"Using {ids[0]} -- pass current_workspace_id to disambiguate if that's wrong.")
    return matches[0]["id"]

def get_item_id(workspace_id, item_name, item_type=None):
    """Resolve an item's GUID by display name within a workspace, optionally
    filtered by Fabric item type (e.g. 'Notebook', 'DataPipeline', 'CopyJob',
    'Dataflow', 'Lakehouse', ...). Returns None if not found; prints a warning
    and returns the first match if the name is ambiguous."""
    if item_type is not None and item_type not in VALID_ITEM_TYPES:
        raise ValueError(f"Unknown Fabric item type {item_type!r}")
    params = {"type": item_type} if item_type else None
    items = _fabric_get(f"/workspaces/{workspace_id}/items", params=params)
    matches = [i for i in items if i.get("displayName") == item_name]
    if not matches:
        return None
    if len(matches) > 1:
        print(f"WARNING: {len(matches)} items named {item_name!r} in workspace {workspace_id} "
              f"(type filter={item_type!r}): {[m['id'] for m in matches]}. Using the first.")
    return matches[0]["id"]

def get_fabric_object_id(workspace_name, object_name=None, item_type=None, current_workspace_id=None):
    """Resolve the GUID for any Fabric object by name: a workspace on its own
    (object_name=None), or an item inside it (object_name given, optionally
    narrowed by item_type). Returns None if the workspace or the item isn't found."""
    ws_id = get_workspace_id(workspace_name, current_workspace_id=current_workspace_id)
    if ws_id is None or object_name is None:
        return ws_id
    return get_item_id(ws_id, object_name, item_type=item_type)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyodbc

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
SQL_COPT_SS_ACCESS_TOKEN = 1256

def _resolve_db_metadata_connection(current_workspace_id, database_name="db_Metadata"):
    """db_Metadata's live server/database for whichever workspace this
    notebook is actually running in, via the Fabric REST API -- this used to
    be a hardcoded server/database naming one specific workspace's copy (see
    this notebook's own Deployment notes below, written against Wave MDF
    CentralHub), which produced a SQL login failure ("Verify the user has the
    Read item permission") whenever this ran from any other workspace,
    including pl_Orchestrator_Top_Level's "Refresh Object IDs" activity here."""
    if not current_workspace_id:
        raise RuntimeError(
            "connect_to_db_metadata() needs current_workspace_id to resolve "
            "db_Metadata's connection -- pass through spark.conf.get('trident.workspace.id')."
        )
    matches = [
        db for db in _fabric_get(f"/workspaces/{current_workspace_id}/sqlDatabases")
        if db.get("displayName") == database_name
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one SQL database named '{database_name}' in "
            f"workspace {current_workspace_id}, found {len(matches)}."
        )
    props = matches[0]["properties"]
    return props["serverFqdn"], props["databaseName"]

def connect_to_db_metadata(current_workspace_id):
    """pyodbc connection to db_Metadata using the notebook's own Entra token --
    no interactive sign-in, safe to run unattended from a pipeline."""
    server, database = _resolve_db_metadata_connection(current_workspace_id)
    token = _get_pbi_token().encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    connstr = (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server=tcp:{server};"
        f"Database={database};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(connstr, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Which orch.Tasks TaskTypes correspond to an actual Fabric item to look up.
# StoredProcedure tasks aren't Fabric items (ObjectName is the proc name
# directly, per pl_Task_Executor) so they're skipped here entirely.
TASK_TYPE_TO_ITEM_TYPE = {
    "Notebook": "Notebook",
    "CopyJob": "CopyJob",
    "Dataflow": "Dataflow",
    "Pipeline": "DataPipeline",
}

def discover_object_refs(conn, only_workspace_name=None):
    """Every distinct (WorkspaceName, ObjectName) pair referenced by orch.Tasks,
    paired with the Fabric item type to search for (None for unmapped
    TaskTypes -- these fall back to a type-unfiltered name search).

    Jobs aren't included here: a JobName is just a grouping key in orch.Jobs,
    not a Fabric item anything looks up by GUID (orch.spGetNextWave joins
    orch.ObjectIDs on the Task's own ObjectName only), so there's nothing to
    resolve for it."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT WorkspaceName, ObjectName, TaskType
        FROM orch.Tasks
        WHERE WorkspaceName IS NOT NULL AND TaskType <> 'StoredProcedure'
    """)
    refs = []
    for workspace_name, object_name, task_type in cur.fetchall():
        if only_workspace_name and workspace_name != only_workspace_name:
            continue
        item_type = TASK_TYPE_TO_ITEM_TYPE.get(task_type)
        refs.append((workspace_name, object_name, item_type))
    return refs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_object_id(conn, workspace_name, object_name, object_id, workspace_id):
    cur = conn.cursor()
    cur.execute(
        "UPDATE orch.ObjectIDs SET ObjectID = ?, WorkspaceID = ? "
        "WHERE WorkspaceName = ? AND ObjectName = ?",
        [object_id, workspace_id, workspace_name, object_name],
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO orch.ObjectIDs (WorkspaceName, ObjectName, ObjectID, WorkspaceID) "
            "VALUES (?, ?, ?, ?)",
            [workspace_name, object_name, object_id, workspace_id],
        )
        return "inserted"
    return "updated"

def refresh_object_ids(current_workspace_id=None, only_workspace_name=None):
    """Resolve every (WorkspaceName, ObjectName) referenced by orch.Tasks
    and upsert the result into orch.ObjectIDs. Returns a summary dict; prints
    anything that couldn't be resolved instead of silently skipping it.

    Whatever DOES resolve is committed regardless -- a name that can't be found
    doesn't hold back everything else that could. But if anything is left
    unresolved, this raises RuntimeError right after that commit, so a
    NOT FOUND is no longer just a printed line the caller can miss: it fails
    this notebook's cell, which fails the pipeline activity that invoked it
    (pl_Orchestrator_Top_Level's "Refresh Object IDs"), instead of leaving the
    run looking green while orch.ObjectIDs is still silently stale. An
    unresolved reference almost always means orch.Tasks names a
    WorkspaceName/ObjectName that doesn't match anything actually deployed --
    that's a metadata bug worth stopping the run over, not a transient
    condition worth retrying past."""
    conn = connect_to_db_metadata(current_workspace_id)
    try:
        refs = discover_object_refs(conn, only_workspace_name=only_workspace_name)
        resolved = 0
        not_found = []
        ws_id_cache = {}
        for workspace_name, object_name, item_type in refs:
            if workspace_name not in ws_id_cache:
                ws_id_cache[workspace_name] = get_workspace_id(
                    workspace_name, current_workspace_id=current_workspace_id
                )
            ws_id = ws_id_cache[workspace_name]
            if ws_id is None:
                print(f"NOT FOUND: workspace {workspace_name!r}")
                not_found.append((workspace_name, object_name))
                continue
            obj_id = get_item_id(ws_id, object_name, item_type=item_type)
            if obj_id is None:
                print(f"NOT FOUND: {workspace_name!r} / {object_name!r} (type={item_type!r})")
                not_found.append((workspace_name, object_name))
                continue
            action = upsert_object_id(conn, workspace_name, object_name, obj_id, ws_id)
            resolved += 1
            print(f"{action}: {workspace_name}/{object_name} -> {obj_id}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    summary = {"resolved": resolved, "not_found": not_found, "total": len(refs)}
    print(summary)
    if not_found:
        raise RuntimeError(
            f"{len(not_found)} of {len(refs)} object reference(s) could not be resolved in "
            f"Fabric and are left at their previous ObjectID: {not_found}. orch.Tasks "
            "likely names a WorkspaceName/ObjectName that doesn't match anything actually "
            "deployed -- fix the metadata (or deploy the missing item) and re-run."
        )
    return summary


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params = json.loads(ParametersJson)
_current_workspace_id = None
try:
    _current_workspace_id = spark.conf.get("trident.workspace.id")
except Exception:
    pass  # not running in a Fabric Spark session (e.g. a local syntax check)

refresh_object_ids(
    current_workspace_id=_current_workspace_id,
    only_workspace_name=params.get("onlyWorkspaceName"),
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deployment
# 
# 1. Import this notebook into the workspace (**New item -> Import notebook**).
# 2. Grab its notebook ID (from the URL, or the Fabric REST API) and the
#    workspace ID, and put them in the new `Refresh ObjectIDs` activity added to
#    `pl_Orchestrator_Top_Level` (currently a placeholder -- see that pipeline's
#    change notes).
# 3. Optionally add a row for this notebook itself to `orch.ObjectIDs`
#    (`WorkspaceName="Wave MDF CentralHub"`, `ObjectName="nb_RefreshObjectIDs"`) so it's
#    tracked the same way as everything else, in case something later invokes it
#    as a Task too.

