# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_WeatherGovApi_ToLakehouse
# # Generic, parameterized MDF task notebook: pulls the current daily forecast for
# one geography from the National Weather Service's public API (`api.weather.gov`)
# and lands it as a Bronze Delta table in whatever lakehouse the Task points it
# at. Same shape as `nb_CopyTableToLakehouse` -- invoked by `pl_Task_Executor` for
# any `orch.Tasks` row with `TaskType = "Notebook"` and `ObjectName` pointing at
# this notebook, with `ParametersJson` supplying the per-run geography *and*
# destination. Nothing about the destination is configured on the notebook
# itself, matching the Wave Runner MDF framework's design: every source and
# destination is data in the Tasks table, not notebook configuration.
# # **How this differs from `nb_CopyTableToLakehouse`:**
# - The "source" is a public REST API, not a SQL database -- no ODBC driver, no
#   AAD login on a source system, nothing to provision beyond write access to
#   the destination lakehouse. `api.weather.gov` needs no API key/token at all,
#   just a descriptive `User-Agent` header identifying the caller.
# - There's no watermark concept here. `/points` + `/forecast` always return
#   "the next several day/night periods from right now" -- it isn't a
#   monotonically increasing feed the way a source table's identity/date column
#   is, so there's nothing to track a high-water mark against. This notebook
#   never calls `notebookutils.notebook.exit(...)`, and a Task invoking it
#   should have no `orch.TaskWatermark` row. `updateOption` (`Append`/
#   `Overwrite`) is still honored, but it controls whether daily pulls
#   accumulate into a forecast-history table or always replace the destination
#   with just the latest pull -- not whether the read is filtered.
# - `/forecast` (not `/forecast/hourly`) is deliberately the endpoint used: NWS
#   already buckets it into one row per ~12-hour day/night period, which is the
#   "daily sample" this notebook is meant to return, with no resampling needed.
# # **One-time setup before this will run:**
# 1. The identity this notebook runs as (your account for manual testing; the
#    pipeline's identity once scheduled) needs write access to whatever
#    destination lakehouse(s) the Tasks you run point it at -- same requirement
#    as `nb_CopyTableToLakehouse`, no attached-lakehouse shortcut.
# 2. Set `userAgent` in `ParametersJson` to something that identifies this
#    deployment and a real contact (email or URL) -- NWS's API docs ask for
#    this and note that requests without one are more likely to be throttled.
#    The default below is a placeholder; replace it before relying on this for
#    anything beyond a one-off manual test.
# 3. `requests` ships with the standard Fabric Python runtime -- nothing extra
#    to install, unlike `nb_CopyTableToLakehouse`'s `pyodbc`/ODBC driver.
# 4. `pl_Task_Executor`'s `Run Notebook` activity needs to forward
#    `ParametersJson` as a base parameter (already true for every Task of this
#    shape -- see `nb_CopyTableToLakehouse`'s setup notes).


# PARAMETERS CELL ********************

# Default parameters -- overridden by pl_Task_Executor's base-parameters mapping
# when this notebook is invoked as a "Notebook" Task. Every key below has a
# default, so this is runnable standalone with zero overrides and still lands a
# real daily sample.
#
# "latitude"/"longitude": the point NWS resolves into a forecast office + grid
#   via /points/{lat},{lon}. Default is Kansas City, MO (39.7456, -94.6238) --
#   the same point NWS's own API documentation uses as its example.
# "userAgent": sent as the User-Agent header on every request -- see setup note
#   2 above. Replace the placeholder contact before relying on this.
# "destWorkspaceId"/"destLakehouseId"/"destSchema"/"destTable": same meaning as
#   in nb_CopyTableToLakehouse. Defaults point at the same demo lakehouse.
# "updateOption": "Append" (default) accumulates each pull's forecast periods on
#   top of the last, building a forecast-history table over repeated daily
#   runs; "Overwrite" replaces the destination with just the latest pull. See
#   the module docstring above for why there's no watermark column involved.
ParametersJson = (
    '{"latitude": 39.7456, "longitude": -94.6238, '
    '"userAgent": "MDF_CentralHub-demo (REPLACE_ME@yourdomain.example)", '
    '"destWorkspaceId": "947d3136-33ac-458a-be73-ac7dc38afaa5", '
    '"destLakehouseId": "649b7795-2e22-4627-8b25-9749a6f492f0", '
    '"destSchema": "dbo", "destTable": "WeatherGovDailyForecast", '
    '"updateOption": "Append"}'
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
import json

import pandas as pd
import requests

params = json.loads(ParametersJson)
LATITUDE = params.get("latitude", 39.7456)
LONGITUDE = params.get("longitude", -94.6238)
USER_AGENT = params.get("userAgent", "MDF_CentralHub-demo (REPLACE_ME@yourdomain.example)")

DEST_WORKSPACE_ID = params.get("destWorkspaceId", "947d3136-33ac-458a-be73-ac7dc38afaa5")
DEST_LAKEHOUSE_ID = params.get("destLakehouseId", "649b7795-2e22-4627-8b25-9749a6f492f0")
DEST_SCHEMA = params.get("destSchema", "dbo")
DEST_TABLE = params.get("destTable", "WeatherGovDailyForecast")

# "Append" (default) accumulates a forecast-history table across repeated daily
# runs; "Overwrite" replaces the destination with just this run's pull. Unlike
# nb_CopyTableToLakehouse there's no watermark/incremental filter to derive
# from this -- see the module docstring for why. Matches orch.Tasks.UpdateOption
# when this notebook is invoked as a Task.
UPDATE_OPTION = params.get("updateOption", "Append")
if UPDATE_OPTION not in ("Append", "Overwrite"):
    raise ValueError(f"Unrecognized updateOption {UPDATE_OPTION!r} -- expected 'Append' or 'Overwrite'")

if "REPLACE_ME" in USER_AGENT:
    print("Warning: userAgent still has its placeholder contact -- NWS's API docs ask for a "
          "real one (email or URL) and note that requests without one are more likely to be "
          "throttled. Fine for a one-off manual test, not for anything scheduled.")

print(f"Pulling daily forecast for ({LATITUDE}, {LONGITUDE}) -> "
      f"{DEST_LAKEHOUSE_ID}/Tables/{DEST_SCHEMA}/{DEST_TABLE} (updateOption={UPDATE_OPTION})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# No API key/token -- api.weather.gov is fully public. The only thing NWS asks
# for is a descriptive User-Agent identifying the caller (see setup note 2
# above); there's no auth header to attach at all, unlike
# nb_CopyTableToLakehouse's AAD-token SQL connection.
NWS_BASE = "https://api.weather.gov"
_REQUEST_TIMEOUT_SECONDS = 30

def _nws_get(path_or_url):
    """GET an api.weather.gov path (relative, starting with '/') or a full URL
    -- the /points response hands back absolute URLs for the next hop (the
    actual forecast endpoint), so this accepts either."""
    url = path_or_url if path_or_url.startswith("http") else f"{NWS_BASE}{path_or_url}"
    resp = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"},
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()

point = _nws_get(f"/points/{LATITUDE},{LONGITUDE}")
grid_props = point["properties"]
forecast_url = grid_props["forecast"]
GRID_OFFICE = grid_props["gridId"]
GRID_X = grid_props["gridX"]
GRID_Y = grid_props["gridY"]

# /forecast (not /forecast/hourly) -- NWS already buckets this into one row per
# ~12-hour day/night period, which is the "daily sample" this notebook returns.
forecast = _nws_get(forecast_url)
periods = forecast["properties"]["periods"]

print(f"Resolved ({LATITUDE}, {LONGITUDE}) -> office {GRID_OFFICE}, grid ({GRID_X},{GRID_Y}); "
      f"{len(periods)} forecast periods returned")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten NWS's period objects into one row per period. RetrievedUtc tags every
# row with when this pull happened -- on repeated Append runs that's what turns
# a single day's snapshot into a forecast-history table (the same day/period
# pulled on different days will carry different RetrievedUtc values and,
# usually, different forecast details as NWS updates its outlook).
retrieved_utc = dt.datetime.now(dt.timezone.utc).isoformat()
rows = []
for p in periods:
    precip = p.get("probabilityOfPrecipitation") or {}
    rows.append({
        "RetrievedUtc": retrieved_utc,
        "Latitude": LATITUDE,
        "Longitude": LONGITUDE,
        "GridOffice": GRID_OFFICE,
        "GridX": GRID_X,
        "GridY": GRID_Y,
        "PeriodNumber": p.get("number"),
        "PeriodName": p.get("name"),
        "StartTime": p.get("startTime"),
        "EndTime": p.get("endTime"),
        "IsDaytime": p.get("isDaytime"),
        "TemperatureF": p.get("temperature"),
        "TemperatureUnit": p.get("temperatureUnit"),
        "WindSpeed": p.get("windSpeed"),
        "WindDirection": p.get("windDirection"),
        "ProbabilityOfPrecipitationPct": precip.get("value"),
        "ShortForecast": p.get("shortForecast"),
        "DetailedForecast": p.get("detailedForecast"),
    })

df = pd.DataFrame(rows)
print(f"Built {len(df)} rows for {DEST_SCHEMA}.{DEST_TABLE}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Written to an explicit OneLake path -- not saveAsTable against an attached
# default lakehouse -- so this notebook can target *any* lakehouse the running
# identity has access to, entirely driven by ParametersJson. Same convention as
# nb_CopyTableToLakehouse: source and destination are metadata in the Tasks
# table, not notebook configuration.
dest_path = (
    f"abfss://{DEST_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{DEST_LAKEHOUSE_ID}/Tables/{DEST_SCHEMA}/{DEST_TABLE}"
)
WRITE_MODE = "append" if UPDATE_OPTION == "Append" else "overwrite"
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode(WRITE_MODE).save(dest_path)

print(f"Wrote {DEST_TABLE} to {dest_path} ({spark_df.count()} rows, mode={WRITE_MODE})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next steps
# # - Register this notebook once in `orch.ObjectIDs` (WorkspaceName/ObjectName ->
#   its real notebook ID + workspace ID, from the Fabric portal after import) --
#   `FK_Tasks_ObjectIDs_Object` requires that row to exist before any `orch.Tasks`
#   row can reference this notebook as its `ObjectName`.
# - One `orch.Tasks` row per geography, all with the same `ObjectName` (this
#   notebook), each with its own `ParametersJson`
#   (`{"latitude":...,"longitude":...,"userAgent":"...","destWorkspaceId":"...","destLakehouseId":"...","destSchema":"dbo","destTable":"..."}`)
#   -- geography and destination are per-Task data, not something set on the
#   notebook. Give each geography its own `destTable` (or at least tag rows with
#   a geography label) if they're meant to land in the same lakehouse without
#   overwriting each other.
# - No `orch.TaskWatermark` row for Tasks that invoke this notebook -- there's
#   nothing to track a watermark against (see the module docstring). If one
#   exists for some other reason, `orch.spGetTaskParametersJson` will merge in
#   `watermarkColumn`/`watermarkValue` anyway; this notebook simply never reads
#   them.
# - Set a real `userAgent` (see setup note 2) before scheduling this instead of
#   running it manually.
# - Possible follow-up: also land `/gridpoints/{office}/{x},{y}/stations` and
#   `/stations/{id}/observations/latest` for actual (not forecast) daily
#   observations, if a demo calls for recent-history rather than outlook data.
