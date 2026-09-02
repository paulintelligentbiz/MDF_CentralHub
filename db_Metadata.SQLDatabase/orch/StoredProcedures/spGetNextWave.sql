CREATE   PROCEDURE orch.spGetNextWave
    @JobName varchar(200),
        @RunID nvarchar(200)
        AS
        BEGIN
            SET NOCOUNT ON;
                ;WITH Completed AS (
                        SELECT DISTINCT TaskName FROM log.RunLog
                                WHERE RunID = @RunID AND JobName = @JobName AND Status = 'Succeeded'
                                    ),
                                        Ready AS (
                                                SELECT t.* FROM orch.Tasks t
                                                        WHERE t.JobName = @JobName AND t.Include = 1
                                                                  AND t.TaskName NOT IN (SELECT TaskName FROM Completed)
                                                                            AND NOT EXISTS (
                                                                                          SELECT 1 FROM OPENJSON(ISNULL(t.Dependencies, '[]')) d
                                                                                                        WHERE d.value NOT IN (SELECT TaskName FROM Completed)
                                                                                                                  )
                                                                                                                      )
                                                                                                                          SELECT
                                                                                                                                  CASE WHEN EXISTS (SELECT 1 FROM Ready r JOIN orch.TaskType tt ON tt.TaskType = r.TaskType WHERE tt.AllowParallel = 0)
                                                                                                                                               THEN 'Sequential' ELSE 'Parallel' END AS ExecutionMode,
                                                                                                                                                       (SELECT COUNT(*) FROM Ready) AS TaskCount,
                                                                                                                                                               (SELECT r.TaskName, r.ObjectName, r.WorkspaceName, r.TaskType, r.ParametersJson,
                                                                                                                                                                               r.TimeoutInSeconds, r.Retries, r.RetryIntervalInSeconds, r.LoggingLevel, r.System, r.Layer,
                                                                                                                                                                                               ISNULL(oid.ObjectID, '') AS ObjectID, ISNULL(oid.WorkspaceID, '') AS WorkspaceID
                                                                                                                                                                                                        FROM Ready r
                                                                                                                                                                                                                 LEFT JOIN orch.ObjectIDs oid ON oid.WorkspaceName = r.WorkspaceName AND oid.ObjectName = r.ObjectName
                                                                                                                                                                                                                          FOR JSON PATH) AS TasksJson;
                                                                                                                                                                                                                          END;

GO

