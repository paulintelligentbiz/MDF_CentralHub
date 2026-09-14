# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_Reset_ParallelBatchLimit
# # Sets the `batchCount` on `pl_Task_Wave_Runner_Parallel`'s `ForEach Task Parallel`
# activity to whatever `ParallelBatchLimit` is passed in, by patching that
# pipeline's item definition through the Fabric REST API.
# # This exists because a ForEach activity's `batchCount` has no dynamic-content /
# expression support in the pipeline authoring model (unlike `Items`) -- so a
# per-job batch limit can only be applied by rewriting the pipeline's saved
# definition before each run, not by parameterizing the activity itself.
# # Meant to be invoked as an activity in `pl_Orchestrator_Top_Level` (see the
# pipeline change alongside this notebook), right after `Get Job Info`, with
# `ParallelBatchLimit` coming from that job's `orch.Jobs.ParallelBatchLimit`
# column.
# # **Setup:**
# 1. `%pip install requests` if it isn't already on the environment.
# 2. No interactive sign-in needed -- this uses `notebookutils.credentials.getToken('pbi')`
#    to get a token for the Fabric REST API, so it's safe to run unattended from
#    a pipeline.
# 3. The calling identity (you, or the pipeline's run-as identity) needs at
#    least Contributor on this workspace (Get Item Definition / Update Item
#    Definition both require read+write on the target item).
# # **Git-connected workspace note:** this notebook writes directly to
# `pl_Task_Wave_Runner_Parallel`'s live item definition, bypassing Git. That
# shows up as an uncommitted change against the connected branch, and gets
# silently reverted the next time someone does *Update from Git* without
# committing it first. That's expected -- this is meant as a per-run override,
# not a way to make `batchCount` durably dynamic. The repo's checked-in value
# (currently `4`) stays the source of truth for anyone re-syncing from Git.


# PARAMETERS CELL ********************

# Base parameter -- set by pl_Orchestrator_Top_Level when invoked as an
# activity, from the running job's orch.Jobs.ParallelBatchLimit column.
ParallelBatchLimit = 4


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import time

import requests

try:
    from notebookutils import credentials as _nb_credentials
except ImportError:
    _nb_credentials = None  # allows py_compile / unit tests outside a Fabric runtime

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_token_cache = {}

def _get_pbi_token():
    """Entra token for the Fabric/Power BI REST API audience -- cached for the
    notebook session."""
    if "pbi" not in _token_cache:
        _token_cache["pbi"] = _nb_credentials.getToken("pbi")
    return _token_cache["pbi"]

def _fabric_request(method, path, json_body=None, params=None):
    """Call the Fabric REST API, transparently handling both the synchronous
    (200) and long-running-operation (202 Accepted) response shapes: on a 202,
    polls /operations/{id} until it reaches a terminal state and, once
    Succeeded, fetches /operations/{id}/result. Returns the parsed JSON body,
    or None where there isn't one (e.g. a completed Update Item Definition)."""
    token = _get_pbi_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{FABRIC_API_BASE}{path}"
    resp = requests.request(method, url, headers=headers, params=params, json=json_body)
    resp.raise_for_status()

    if resp.status_code == 202:
        operation_id = resp.headers.get("x-ms-operation-id")
        status_url = f"{FABRIC_API_BASE}/operations/{operation_id}"
        retry_after = int(resp.headers.get("Retry-After", 5))
        for _ in range(60):  # ~ generous timeout at the observed Retry-After cadence
            time.sleep(retry_after)
            state_resp = requests.get(status_url, headers=headers)
            state_resp.raise_for_status()
            state = state_resp.json()
            status = state.get("status")
            if status == "Succeeded":
                result_resp = requests.get(f"{status_url}/result", headers=headers)
                if result_resp.status_code == 200 and result_resp.content:
                    return result_resp.json()
                return None
            if status == "Failed":
                raise RuntimeError(f"Fabric operation {operation_id} failed: {state.get('error')}")
            retry_after = int(state_resp.headers.get("Retry-After", retry_after))
        raise TimeoutError(f"Fabric operation {operation_id} did not complete in time")

    if resp.content:
        return resp.json()
    return None

def _fabric_get(path, params=None):
    """GET against the Fabric REST API, following continuationToken pagination.
    Returns the concatenated "value" list across all pages."""
    token = _get_pbi_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{FABRIC_API_BASE}{path}"
    items = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        payload = resp.json()
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
        params = None  # continuationUri already carries the query string
    return items


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_item_id(workspace_id, item_name, item_type=None):
    """Resolve an item's GUID by display name within a workspace, optionally
    filtered by Fabric item type. Returns None if not found; prints a warning
    and returns the first match if the name is ambiguous."""
    params = {"type": item_type} if item_type else None
    items = _fabric_get(f"/workspaces/{workspace_id}/items", params=params)
    matches = [i for i in items if i.get("displayName") == item_name]
    if not matches:
        return None
    if len(matches) > 1:
        print(f"WARNING: {len(matches)} items named {item_name!r} in workspace {workspace_id} "
              f"(type filter={item_type!r}): {[m['id'] for m in matches]}. Using the first.")
    return matches[0]["id"]

def get_item_definition(workspace_id, item_id):
    """Current definition parts (path/payload/payloadType) for a Fabric item."""
    body = _fabric_request("POST", f"/workspaces/{workspace_id}/items/{item_id}/getDefinition")
    return body["definition"]["parts"]

def update_item_definition(workspace_id, item_id, parts):
    _fabric_request(
        "POST",
        f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
        json_body={"definition": {"parts": parts}},
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def set_foreach_batch_count(parts, new_batch_count):
    """Return `parts` with every ForEach activity's typeProperties.batchCount
    in pipeline-content.json set to new_batch_count. Raises if that part, or a
    ForEach activity inside it, isn't found. All other parts pass through
    unchanged."""
    updated_parts = []
    found_pipeline_json = False
    for part in parts:
        if part["path"] != "pipeline-content.json":
            updated_parts.append(part)
            continue
        found_pipeline_json = True
        raw = base64.b64decode(part["payload"]).decode("utf-8")
        content = json.loads(raw)
        foreach_activities = [
            a for a in content.get("properties", {}).get("activities", [])
            if a.get("type") == "ForEach"
        ]
        if not foreach_activities:
            raise ValueError("No ForEach activity found in pipeline-content.json")
        if len(foreach_activities) > 1:
            names = [a.get("name") for a in foreach_activities]
            print(f"WARNING: {len(foreach_activities)} ForEach activities found ({names}); "
                  f"setting batchCount={new_batch_count} on all of them.")
        for activity in foreach_activities:
            activity.setdefault("typeProperties", {})["batchCount"] = new_batch_count
        new_payload = base64.b64encode(json.dumps(content, indent=2).encode("utf-8")).decode("ascii")
        updated_parts.append({"path": part["path"], "payload": new_payload, "payloadType": "InlineBase64"})
    if not found_pipeline_json:
        raise ValueError("pipeline-content.json not found in item definition")
    return updated_parts

def reset_parallel_batch_limit(pipeline_name, new_batch_count, current_workspace_id=None):
    """Patch pipeline_name's ForEach batchCount to new_batch_count in place, via
    the Fabric REST API. pipeline_name must be a DataPipeline item in the same
    workspace as this notebook."""
    if not isinstance(new_batch_count, int) or isinstance(new_batch_count, bool) or new_batch_count < 1:
        raise ValueError(f"ParallelBatchLimit must be a positive integer, got {new_batch_count!r}")
    if new_batch_count > 50:
        # Fabric's documented ForEach batchCount ceiling -- not enforced client-side,
        # so a too-high value would otherwise fail obscurely at pipeline run time.
        print(f"WARNING: ParallelBatchLimit={new_batch_count} exceeds the ForEach activity's "
              f"documented batchCount maximum of 50; the pipeline may reject it at run time.")

    workspace_id = current_workspace_id or spark.conf.get("trident.workspace.id")
    item_id = get_item_id(workspace_id, pipeline_name, item_type="DataPipeline")
    if item_id is None:
        raise ValueError(f"Pipeline {pipeline_name!r} not found in workspace {workspace_id}")

    parts = get_item_definition(workspace_id, item_id)
    updated_parts = set_foreach_batch_count(parts, new_batch_count)
    update_item_definition(workspace_id, item_id, updated_parts)
    print(f"{pipeline_name}: ForEach batchCount set to {new_batch_count}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The wave-runner pipeline this notebook patches -- a fixed part of the
# CentralHub framework, always in this same workspace.
PARALLEL_WAVE_RUNNER_PIPELINE = "pl_Task_Wave_Runner_Parallel"

_current_workspace_id = None
try:
    _current_workspace_id = spark.conf.get("trident.workspace.id")
except Exception:
    pass  # not running in a Fabric Spark session (e.g. a local syntax check)

reset_parallel_batch_limit(
    PARALLEL_WAVE_RUNNER_PIPELINE,
    ParallelBatchLimit,
    current_workspace_id=_current_workspace_id,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deployment
# # 1. Import this notebook into the workspace (**New item -> Import notebook**),
#    or let it arrive via the Git sync that carries this file in.
# 2. Grab its notebook ID (from the URL, or the Fabric REST API) and put it in
#    the `Reset Parallel Batch Limit` activity added to `pl_Orchestrator_Top_Level`
#    (currently a placeholder string, `PLACEHOLDER-REPLACE-WITH-nb_Reset_ParallelBatchLimit-OBJECT-ID`
#    -- see that pipeline's change notes). The workspace ID is already filled
#    in, since this notebook always lives in the same CentralHub workspace.
# 3. Optionally add a row for this notebook itself to `orch.ObjectIDs` so it's
#    tracked the same way as everything else, in case something later invokes
#    it as a Task too.
