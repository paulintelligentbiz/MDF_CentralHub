-- Prerequisite: FK_Tasks_ObjectIDs_Object requires a matching (WorkspaceName, ObjectName)
-- row in orch.ObjectIDs before any orch.Tasks row can reference this notebook as its
-- ObjectName. Placeholder ObjectID, same convention as nb_CopyTableToLakehouse and
-- nb_RefreshObjectIDs's existing rows -- replace once the notebook is actually imported
-- into the workspace and its real Fabric item GUID is known.
INSERT INTO orch.ObjectIDs (WorkspaceName, ObjectName, ObjectID, WorkspaceID)
VALUES (
    'Wave MDF CentralHub',
    'nb_WeatherGovApi_ToLakehouse',
    'PENDING_DEPLOYMENT_REPLACE_ME_nb_WeatherGovApi_ToLakehouse',
    'a937b42f-1407-41c9-b5bf-33288dc7f60f'
);

-- The Task itself. JobName references the only Job currently in the metadata
-- (Ingest_ContosoDW_Bronze) -- swap it for a different orch.Jobs row (or add a new one
-- first) if you'd rather this run under its own Job instead of alongside the ContosoDW
-- copy tasks. ParametersJson's defaults match the notebook's own defaults (Kansas City,
-- MO -- the same point NWS's own API docs use as their example); change latitude/
-- longitude/destTable per geography if you add more than one of these Tasks.
INSERT INTO orch.Tasks (
    TaskName, Include, JobName, ObjectName, WorkspaceName,
    TimeoutInSeconds, Retries, RetryIntervalInSeconds, UpdateOption,
    ParametersJson, Dependencies, TaskType, System, Layer, LoggingLevel
)
VALUES (
    'CopyBronze_WeatherGovDailyForecast',
    1,
    'Ingest_ContosoDW_Bronze',
    'nb_WeatherGovApi_ToLakehouse',
    'Wave MDF CentralHub',
    300,
    1,
    30,
    'Append',
    '{"latitude": 39.7456, "longitude": -94.6238, ' +
    '"userAgent": "MDF_CentralHub-demo (REPLACE_ME@yourdomain.example)", ' +
    '"destWorkspaceId": "947d3136-33ac-458a-be73-ac7dc38afaa5", ' +
    '"destLakehouseId": "649b7795-2e22-4627-8b25-9749a6f492f0", ' +
    '"destSchema": "dbo", "destTable": "WeatherGovDailyForecast", ' +
    '"updateOption": "Append"}',
    NULL,
    'Notebook',
    'NWS',
    'Bronze',
    1
);
