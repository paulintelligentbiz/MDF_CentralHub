CREATE   PROCEDURE log.spLogTaskRunEvent
    -- RunLog fields
    @RunID          nvarchar(200)  = NULL,
    @JobName        varchar(200),
    @TaskName       varchar(200),
    @TaskType       nvarchar(50)   = NULL,
    @ObjectPath     nvarchar(400)  = NULL,
    @StartTime      datetime2(0)   = NULL,
    @Status         nvarchar(50)   = NULL,
    -- TaskRunEvent fields
    @LoggingLevel   tinyint        = 1,
    @EventStatus    nvarchar(50)   = NULL,
    @StartTimeUtc   datetime2      = NULL,
    @RunLogId       bigint         = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));

    -- Opening entry in the RunLog ledger -- consumed by orch.spGetNextWave / orch.spGetRunStatus
    -- to determine which tasks have started/succeeded/failed for this RunID.
    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, Status)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @Status);

    SET @RunLogId = SCOPE_IDENTITY();

    -- Single TaskRunEvent row for this task run -- opened here, closed by spInsertTaskRunEvent.
    INSERT INTO log.TaskRunEvent (RunLogId, JobName, TaskName, LoggingLevel, Status, StartTimeUtc)
    VALUES (@RunLogId, @JobName, @TaskName, @LoggingLevel, @EventStatus, @StartTimeUtc);

    SELECT @RunLogId AS RunLogId;
END;

GO

