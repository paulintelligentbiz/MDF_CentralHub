CREATE   PROCEDURE log.spInsertActivityRunEvent
    -- Identifies the row to close. RunLogId can be shared with a parent Task/Pipeline/Job run
    -- (see spLogActivityRunEvent) and so is no longer unique to one ActivityRunEvent row --
    -- ActivityRunId is what pins the close to the specific row spLogActivityRunEvent opened,
    -- not every row under that RunLogId. Pass back exactly the ActivityRunId it returned.
    @ActivityRunId           nvarchar(100),
    @RunID                   nvarchar(200)  = NULL,
    @JobName                 nvarchar(200)  = NULL,
    @TaskName                nvarchar(200)  = NULL,
    @TaskType                nvarchar(50)   = NULL,
    @Status                  nvarchar(50)   = NULL,
    @ActivityRunEnd          datetime2      = NULL,
    @DurationInMs            int            = NULL,
    @OutputJson              nvarchar(max)  = NULL,
    @ErrorCode               nvarchar(100)  = NULL,
    @ErrorMessage            nvarchar(max)  = NULL,
    @ErrorFailureType        nvarchar(100)  = NULL,
    @ErrorTarget             nvarchar(200)  = NULL,
    @ErrorDetails            nvarchar(max)  = NULL,
    @RetryAttempt            int            = NULL,
    @RecoveryStatus          nvarchar(50)   = NULL,
    @ExecutionDetailsJson    nvarchar(max)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Regardless of Status (Succeeded or Failed), stamp an end time -- never leave a closed
    -- record's end time NULL just because a caller omitted it.
    SET @ActivityRunEnd = ISNULL(@ActivityRunEnd, SYSUTCDATETIME());

    -- Closing entry in the RunLog ledger.
    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, StopTime, Status, ErrorMessage)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @ActivityRunEnd, @Status, @ErrorMessage);

    -- Close out the single ActivityRunEvent row opened by spLogActivityRunEvent, identified by
    -- ActivityRunId -- not RunLogId, which other activities under the same parent run now share.
    UPDATE log.ActivityRunEvent
    SET Status = @Status,
        ActivityRunEnd = @ActivityRunEnd,
        DurationInMs = @DurationInMs,
        OutputJson = @OutputJson,
        ErrorCode = @ErrorCode,
        ErrorMessage = @ErrorMessage,
        ErrorFailureType = @ErrorFailureType,
        ErrorTarget = @ErrorTarget,
        ErrorDetails = @ErrorDetails,
        RetryAttempt = @RetryAttempt,
        RecoveryStatus = @RecoveryStatus,
        ExecutionDetailsJson = @ExecutionDetailsJson
    WHERE ActivityRunId = @ActivityRunId;
END;

GO

