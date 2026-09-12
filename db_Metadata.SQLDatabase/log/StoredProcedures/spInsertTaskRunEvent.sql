CREATE   PROCEDURE log.spInsertTaskRunEvent
    @TaskRunEventId bigint,  -- required -- the TaskRunEvent row opened by spLogTaskRunEvent
    @RunID         nvarchar(200),
    @JobName       varchar(200),
    @TaskName      varchar(200),
    @TaskType      nvarchar(50)   = NULL,
    @Status        nvarchar(50)   = NULL,
    @EndTimeUtc    datetime2      = NULL,
    @ExitValue     nvarchar(max)  = NULL,
    @ErrorMessage  nvarchar(max)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Regardless of Status (Succeeded or Failed), stamp an end time -- never leave a closed
    -- record's end time NULL just because a caller omitted it.
    SET @EndTimeUtc = ISNULL(@EndTimeUtc, SYSUTCDATETIME());

    -- Closing entry in the RunLog ledger -- this is what orch.spGetNextWave / orch.spGetRunStatus
    -- actually query (by TaskName + Status) to know a task succeeded or failed.
    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, StopTime, Status, ExitValue, ErrorMessage)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @EndTimeUtc, @Status, @ExitValue, @ErrorMessage);

    -- Close out the single TaskRunEvent row opened by spLogTaskRunEvent.
    UPDATE log.TaskRunEvent
    SET Status = @Status,
        EndTimeUtc = @EndTimeUtc
    WHERE TaskRunEventId = @TaskRunEventId;
END;

GO

