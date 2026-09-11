CREATE   PROCEDURE log.spInsertActivityRunEvent
    @RunLogId                bigint,  -- required -- the ActivityRunEvent row opened by spLogActivityRunEvent
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

    -- Closing entry in the RunLog ledger.
    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, StopTime, Status, ErrorMessage)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @ActivityRunEnd, @Status, @ErrorMessage);

    -- Close out the single ActivityRunEvent row opened by spLogActivityRunEvent.
    -- (Previously an INSERT here; changed to UPDATE to avoid duplicating/orphaning rows
    -- once callers started reusing the RunLogId captured at activity start.)
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
    WHERE RunLogId = @RunLogId;
END;

GO

