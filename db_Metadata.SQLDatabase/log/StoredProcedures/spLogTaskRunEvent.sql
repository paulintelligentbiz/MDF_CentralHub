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
    -- Pass the TaskRunEventId an ancestor event (e.g. a parent Job or Pipeline run) already
    -- opened to fold this Task under that same run instead of minting an unrelated one --
    -- see spLogActivityRunEvent for the same pattern, which is how pl_Task_Executor keeps
    -- TaskRunEvent and its ActivityRunEvent row on one shared RunLog entry.
    @ExistingTaskRunEventId bigint = NULL,
    -- The Job run this task executed under, if any -- stored as a real FK on TaskRunEvent
    -- rather than relied on via a shared RunLogId value.
    @JobRunEventId  bigint         = NULL,
    @TaskRunEventId bigint         = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));

    IF @ExistingTaskRunEventId IS NULL
    BEGIN
        -- Opening entry in the RunLog ledger -- consumed by orch.spGetNextWave / orch.spGetRunStatus
        -- to determine which tasks have started/succeeded/failed for this RunID.
        INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, Status)
        VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @Status);

        SET @TaskRunEventId = SCOPE_IDENTITY();
    END
    ELSE
        SET @TaskRunEventId = @ExistingTaskRunEventId;

    -- Single TaskRunEvent row for this task run -- opened here, closed by spInsertTaskRunEvent.
    INSERT INTO log.TaskRunEvent (TaskRunEventId, JobRunEventId, JobName, TaskName, LoggingLevel, Status, StartTimeUtc)
    VALUES (@TaskRunEventId, @JobRunEventId, @JobName, @TaskName, @LoggingLevel, @EventStatus, @StartTimeUtc);

    SELECT @TaskRunEventId AS TaskRunEventId;
END;

GO

