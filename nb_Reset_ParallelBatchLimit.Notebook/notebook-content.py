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

# # nb_Reset_ParallelBatchLimit
# # Invoked directly by `pl_Orchestrator_Top_Level`'s "Reset Parallel Batch Limit"
# activity -- a `TridentNotebook` activity with a hardcoded `notebookId` (not
# looked up via `orch.ObjectIDs`, same convention as that pipeline's "Refresh
# Object IDs" activity / `nb_RefreshObjectIDs`) that only fires when
# `orch.Jobs.UpdateParallelBatchLimit = 1` for the running Job.
# # **What this does today:** takes the Job's current `orch.Jobs.ParallelBatchLimit`
# value (already fetched by "Get Job Info" and passed in as the `ParallelBatchLimit`
# parameter) and writes it into `pl_Task_Wave_Runner_Parallel`'s "ForEach Task
# Parallel" activity as that activity's `batchCount` -- Fabric/ADF pipelines don't
# support a dynamic expression for `batchCount` (it's a plain design-time int, not
# an `{"value":..., "type":"Expression"}` property), so the only way to change it
# programmatically is to fetch the pipeline's item definition over the Fabric REST
# API, edit the JSON, and push the definition back. It then clears
# `orch.Jobs.UpdateParallelBatchLimit` back to 0 for this Job -- this is a one-shot
# "apply my new batch count" request, not a per-run toggle, so leaving the flag set
# would just re-patch the pipeline with the same value on every future run for no
# reason.
# # **What this does NOT do yet:** compute a new `ParallelBatchLimit` value itself.
# It only *applies* whatever value is already sitting in `orch.Jobs.ParallelBatchLimit`
# (set by hand in the workbook today). A capacity-aware auto-detect function is
# written below but deliberately left commented out / uncalled -- see
# `estimate_batch_limit_from_capacity()` -- until that behavior is actually wanted.
# # **Setup:**
# 1. `%pip install pyodbc requests` if either isn't already on the environment.
# 2. No interactive sign-in needed -- uses `notebookutils.credentials.getToken('pbi')`
#    for both the Fabric REST API and the `db_Metadata` SQL endpoint, so it's safe
#    to run unattended from a pipeline.
# 3. The pipeline's run-as identity needs at least Contributor on this workspace
#    (patching a pipeline's definition needs write access to that item, not just
#    read/write on `db_Metadata`) plus a Fabric capacity assigned to the workspace
#    for `getDefinition`/`updateDefinition` to succeed.
# 4. This notebook itself needs a row added to `orch.ObjectIDs`? No -- like
#    `nb_RefreshObjectIDs`, it's invoked by a hardcoded `notebookId` directly in
#    `pl_Orchestrator_Top_Level`'s JSON, not through the metadata-driven Task path,
#    so it's never looked up by name at runtime.


# PARAMETERS CELL ********************

# Base parameters -- set by pl_Orchestrator_Top_Level's "Reset Parallel Batch
# Limit" activity. These are Fabric-native notebook parameters (plain typed
# variables the pipeline activity assigns directly), not the ParametersJson
# string convention the metadata-driven Task framework uses elsewhere.
JobName = ""
ParallelBatchLimit = 4


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import struct
import time

import requests

try:
    from notebookutils import credentials as _nb_credentials
except ImportError:
    _nb_credentials = None  # allows py_compile / unit tests outside a Fabric runtime

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_REQUEST_TIMEOUT_SECONDS = 30  # fail fast rather than hang indefinitely -- requests has no default timeout
_LRO_POLL_INTERVAL_SECONDS = 5
_LRO_MAX_WAIT_SECONDS = 120
_token_cache = {}

def _get_pbi_token():
    """Entra token for the Fabric REST API (also used for the db_Metadata SQL
    endpoint below) -- cached for the notebook session."""
    if "pbi" not in _token_cache:
        _token_cache["pbi"] = _nb_credentials.getToken("pbi")
    return _token_cache["pbi"]

def _fabric_headers():
    return {"Authorization": f"Bearer {_get_pbi_token()}"}

def _fabric_get(path, params=None):
    """GET against the Fabric REST API, following continuationToken pagination.
    Returns the concatenated "value" list across all pages."""
    url = f"{FABRIC_API_BASE}{path}"
    items = []
    while url:
        resp = requests.get(url, headers=_fabric_headers(), params=params, timeout=_REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        payload = resp.json()
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
        params = None  # continuationUri already carries the query string
    return items

def _fabric_post_lro(path, body=None):
    """POST a long-running Fabric operation (getDefinition/updateDefinition both
    work this way) and poll until it finishes. Returns the final result body
    (None for an operation with no result payload, e.g. a plain updateDefinition)."""
    resp = requests.post(
        f"{FABRIC_API_BASE}{path}", headers=_fabric_headers(), json=body, timeout=_REQUEST_TIMEOUT_SECONDS
    )
    if resp.status_code == 200:
        return resp.json() if resp.content else None
    if resp.status_code != 202:
        resp.raise_for_status()

    operation_url = resp.headers["Location"]
    waited = 0
    while waited < _LRO_MAX_WAIT_SECONDS:
        time.sleep(_LRO_POLL_INTERVAL_SECONDS)
        waited += _LRO_POLL_INTERVAL_SECONDS
        status_resp = requests.get(operation_url, headers=_fabric_headers(), timeout=_REQUEST_TIMEOUT_SECONDS)
        status_resp.raise_for_status()
        status = status_resp.json()
        if status.get("status") == "Succeeded":
            result_resp = requests.get(
                f"{operation_url}/result", headers=_fabric_headers(), timeout=_REQUEST_TIMEOUT_SECONDS
            )
            if result_resp.status_code == 200 and result_resp.content:
                return result_resp.json()
            return None
        if status.get("status") == "Failed":
            raise RuntimeError(f"Fabric operation failed: {status}")
    raise TimeoutError(f"Fabric operation didn't finish within {_LRO_MAX_WAIT_SECONDS}s: {operation_url}")

def get_item_id(workspace_id, item_name, item_type=None):
    """Resolve a Fabric item's GUID by display name within a workspace, optionally
    filtered by item type (e.g. 'DataPipeline'). Returns None if not found; prints
    a warning and returns the first match if the name is ambiguous."""
    params = {"type": item_type} if item_type else None
    items = _fabric_get(f"/workspaces/{workspace_id}/items", params=params)
    matches = [i for i in items if i.get("displayName") == item_name]
    if not matches:
        return None
    if len(matches) > 1:
        print(f"WARNING: {len(matches)} items named {item_name!r} (type filter={item_type!r}): "
              f"{[m['id'] for m in matches]}. Using the first.")
    return matches[0]["id"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Not used yet -- left here for when a capacity-aware auto-detect is wanted ---
#
# def estimate_batch_limit_from_capacity(capacity_id, floor=2, ceiling=16):
#     """Sketch of a capacity-aware alternative to writing a fixed number into
#     orch.Jobs.ParallelBatchLimit by hand: look up the workspace's assigned
#     Fabric capacity SKU and derive a reasonable parallel batch size from its
#     Spark VCore allotment, instead of a human picking a constant. NOT called
#     anywhere below -- ParallelBatchLimit is still whatever's already in
#     orch.Jobs today. Wire this in (replace the plain `ParallelBatchLimit`
#     variable used further down) once auto-detection is actually wanted, and
#     test the capacity math against a real capacity before trusting it.
#     capacities = _fabric_get("/capacities")
#     capacity = next((c for c in capacities if c["id"] == capacity_id), None)
#     if capacity is None:
#         raise ValueError(f"Capacity {capacity_id!r} not found or not visible to this identity")
#     sku = capacity.get("sku", "")  # e.g. "F64" -- trailing digits are the capacity units
#     digits = "".join(ch for ch in sku if ch.isdigit())
#     capacity_units = int(digits) if digits else 0
#     # Very rough heuristic -- refine against observed Spark pool concurrency limits
#     # for each SKU before relying on this: 1 batch slot per ~8 capacity units.
#     estimated = max(floor, min(ceiling, capacity_units // 8 or floor))
#     return estimated


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WAVE_RUNNER_PARALLEL_NAME = "pl_Task_Wave_Runner_Parallel"
FOREACH_ACTIVITY_NAME = "ForEach Task Parallel"

def set_wave_runner_batch_count(workspace_id, new_batch_count):
    """Fetch pl_Task_Wave_Runner_Parallel's own item definition, set its
    "ForEach Task Parallel" activity's batchCount to new_batch_count, and push
    the edited definition back. Returns (old_batch_count, item_id).

    batchCount has no dynamic-expression form in Fabric/ADF pipelines -- it's a
    plain design-time int -- so this is the only way to change it short of
    hand-editing the pipeline in the portal every time."""
    item_id = get_item_id(workspace_id, WAVE_RUNNER_PARALLEL_NAME, item_type="DataPipeline")
    if item_id is None:
        raise RuntimeError(
            f"Couldn't find a DataPipeline named {WAVE_RUNNER_PARALLEL_NAME!r} in workspace {workspace_id!r}"
        )

    definition = _fabric_post_lro(f"/workspaces/{workspace_id}/items/{item_id}/getDefinition")
    parts = definition["definition"]["parts"]
    content_part = next(p for p in parts if p["path"] == "pipeline-content.json")
    pipeline_json = json.loads(base64.b64decode(content_part["payload"]).decode("utf-8"))

    def find_foreach(obj):
        if isinstance(obj, dict):
            if obj.get("name") == FOREACH_ACTIVITY_NAME and obj.get("type") == "ForEach":
                return obj
            for v in obj.values():
                found = find_foreach(v)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = find_foreach(item)
                if found is not None:
                    return found
        return None

    foreach_activity = find_foreach(pipeline_json)
    if foreach_activity is None:
        raise RuntimeError(
            f"Couldn't find a ForEach activity named {FOREACH_ACTIVITY_NAME!r} in {WAVE_RUNNER_PARALLEL_NAME}'s "
            "definition -- has it been renamed?"
        )

    old_batch_count = foreach_activity["typeProperties"]["batchCount"]
    if old_batch_count == new_batch_count:
        print(f"{WAVE_RUNNER_PARALLEL_NAME}: batchCount already {new_batch_count}, nothing to push.")
        return old_batch_count, item_id

    foreach_activity["typeProperties"]["batchCount"] = new_batch_count
    content_part["payload"] = base64.b64encode(json.dumps(pipeline_json, indent=2).encode("utf-8")).decode("ascii")

    _fabric_post_lro(
        f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
        body={"definition": {"parts": parts}},
    )
    return old_batch_count, item_id


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyodbc

# db_Metadata's own Fabric SQL endpoint (same database this framework's orch/log
# schemas live in) -- same connection convention as nb_RefreshObjectIDs /
# nb_db_Metadata_and_Excel_Sync.
DB_METADATA_SERVER = "kalwvg5capkefegg5d6gkpgeza-f62dpkihcteudnn7gmui3r7wb4.database.fabric.microsoft.com,1433"
DB_METADATA_DATABASE = "db_Metadata-15a01d6c-0d70-4a2d-b4da-28b809849709"
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
SQL_COPT_SS_ACCESS_TOKEN = 1256

def connect_to_db_metadata():
    token = _get_pbi_token().encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    connstr = (
        f"Driver={{{ODBC_DRIVER}}};"
        f"Server=tcp:{DB_METADATA_SERVER};"
        f"Database={DB_METADATA_DATABASE};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(connstr, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

def clear_update_parallel_batch_limit(job_name):
    """One-shot flag: this Job asked for its current ParallelBatchLimit to be
    applied to the wave runner's batchCount, we just did that, so clear the
    request rather than re-patching the pipeline with the same value on every
    future run of this Job."""
    conn = connect_to_db_metadata()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE orch.Jobs SET UpdateParallelBatchLimit = 0 WHERE JobName = ?",
            [job_name],
        )
        if cur.rowcount == 0:
            conn.rollback()
            raise RuntimeError(f"No orch.Jobs row found for JobName={job_name!r} -- nothing was cleared.")
        conn.commit()
    finally:
        conn.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not JobName:
    raise ValueError("JobName parameter is required -- pl_Orchestrator_Top_Level should always pass it.")

_current_workspace_id = None
try:
    _current_workspace_id = spark.conf.get("trident.workspace.id")
except Exception:
    pass  # not running in a Fabric Spark session (e.g. a local syntax check)

if _current_workspace_id is None:
    raise RuntimeError("Couldn't resolve the current workspace id (spark.conf 'trident.workspace.id') -- "
                       "are we actually running inside a Fabric Spark session?")

old_batch_count, wave_runner_item_id = set_wave_runner_batch_count(_current_workspace_id, ParallelBatchLimit)
clear_update_parallel_batch_limit(JobName)

print(f"{JobName}: {WAVE_RUNNER_PARALLEL_NAME}.{FOREACH_ACTIVITY_NAME}.batchCount {old_batch_count} -> "
      f"{ParallelBatchLimit}; orch.Jobs.UpdateParallelBatchLimit cleared to 0.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deployment
# # 1. Import this notebook into the workspace (**New item -> Import notebook**).
# 2. Grab its notebook ID (from the URL, or the Fabric REST API) and put it in
#    `pl_Orchestrator_Top_Level`'s "Reset Parallel Batch Limit" activity, replacing
#    the placeholder `notebookId` (same step as was done for `nb_RefreshObjectIDs`'s
#    "Refresh Object IDs" activity).
# 3. Add a `JobName` parameter to that same activity (`@pipeline().parameters.JobName`,
#    type String) -- it isn't there yet; this notebook needs it to know which
#    `orch.Jobs` row to clear.
# 4. Confirm the pipeline's run-as identity has Contributor (not just Viewer) on
#    this workspace -- `updateDefinition` needs write access to the
#    `pl_Task_Wave_Runner_Parallel` item, which is more than `nb_RefreshObjectIDs`
#    needs.
# 5. Until this is imported and wired in, leave `orch.Jobs.UpdateParallelBatchLimit`
#    at 0 for every Job -- the activity still has a placeholder `notebookId` and
#    will fail immediately if it fires.
