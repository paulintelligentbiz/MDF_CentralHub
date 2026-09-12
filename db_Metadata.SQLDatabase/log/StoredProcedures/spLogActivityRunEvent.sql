CREATE   PROCEDURE log.spLogActivityRunEvent
    -- RunLog fields
    @RunID          nvarchar(200)  = NULL,
    @JobName        nvarchar(200)  = NULL,
    @TaskName       nvarchar(200)  = NULL,
    @TaskType       nvarchar(50)   = NULL,
    @ObjectPath     nvarchar(400)  = NULL,
    @StartTime      datetime2(0)   = NULL,
    @StopTime       datetime2(0)   = NULL,
    @ExitValue      nvarchar(max)  = NULL,
    @ErrorMessage   nvarchar(max)  = NULL,
    @Status         nvarchar(50)   = NULL,
    -- ActivityRunEvent fields
    @LoggingLevel             tinyint        = 1,
    @ActivityRunId            nvarchar(100)  = NULL,
    @EventTaskName            nvarchar(200)  = NULL,
    @TaskInstanceId           nvarchar(100)  = NULL,
    @ActivityName             nvarchar(200)  = NULL,
    @ActivityType             nvarchar(100)  = NULL,
    @LinkedServiceName        nvarchar(200)  = NULL,
    @EventStatus              nvarchar(50)   = NULL,
    @ActivityRunStart         datetime2      = NULL,
    @ActivityRunEnd           datetime2      = NULL,
    @DurationInMs             int            = NULL,
    @InputJson                nvarchar(max)  = NULL,
    @OutputJson               nvarchar(max)  = NULL,
    @ErrorCode                nvarchar(100)  = NULL,
    @EventErrorMessage        nvarchar(max)  = NULL,
    @ErrorFailureType         nvarchar(100)  = NULL,
    @ErrorTarget              nvarchar(200)  = NULL,
    @ErrorDetails             nvarchar(max)  = NULL,
    @RetryAttempt             int            = NULL,
    @IterationHash            nvarchar(200)  = NULL,
    @UserPropertiesJson       nvarchar(max)  = NULL,
    @RecoveryStatus           nvarchar(50)   = NULL,
    @IntegrationRuntimeNames  nvarchar(max)  = NULL,
    @ExecutionDetailsJson     nvarchar(max)  = NULL,
    @ResourceId               nvarchar(1000) = NULL,
    -- Pass the RunLogId a parent Task (or Pipeline/Job) run already opened -- e.g. the value
    -- spLogTaskRunEvent handed back -- so this Activity's row lands under that same run
    -- instead of a disconnected one of its own. ActivityRunEvent's PK is ActivityRunId, not
    -- RunLogId, precisely so several activities can share one parent RunLogId this way.
    @ExistingRunLogId         bigint         = NULL,
    -- The Job run this activity executed under, if any -- stored as a real FK on
    -- ActivityRunEvent rather than relied on via a shared RunLogId value.
    @JobRunEventId            bigint         = NULL,
    @RunLogId                 bigint         = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));
    SET @ActivityRunId = ISNULL(@ActivityRunId, CONVERT(nvarchar(100), NEWID()));

    IF @ExistingRunLogId IS NULL
    BEGIN
        INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, StopTime, ExitValue, ErrorMessage, Status)
        VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @StopTime, @ExitValue, @ErrorMessage, @Status);

        SET @RunLogId = SCOPE_IDENTITY();
    END
    ELSE
        SET @RunLogId = @ExistingRunLogId;

    INSERT INTO log.ActivityRunEvent (
        RunLogId, JobRunEventId, LoggingLevel, ActivityRunId, TaskName, TaskInstanceId, ActivityName, ActivityType,
        LinkedServiceName, Status, ActivityRunStart, ActivityRunEnd, DurationInMs, InputJson, OutputJson,
        ErrorCode, ErrorMessage, ErrorFailureType, ErrorTarget, ErrorDetails, RetryAttempt, IterationHash,
        UserPropertiesJson, RecoveryStatus, IntegrationRuntimeNames, ExecutionDetailsJson, ResourceId
    )
    VALUES (
        @RunLogId, @JobRunEventId, @LoggingLevel, @ActivityRunId, @EventTaskName, @TaskInstanceId, @ActivityName, @ActivityType,
        @LinkedServiceName, @EventStatus, @ActivityRunStart, @ActivityRunEnd, @DurationInMs, @InputJson, @OutputJson,
        @ErrorCode, @EventErrorMessage, @ErrorFailureType, @ErrorTarget, @ErrorDetails, @RetryAttempt, @IterationHash,
        @UserPropertiesJson, @RecoveryStatus, @IntegrationRuntimeNames, @ExecutionDetailsJson, @ResourceId
    );

    -- ActivityRunId returned alongside RunLogId: needed by spInsertActivityRunEvent's caller
    -- to close out exactly this row, since RunLogId is no longer unique to one activity once
    -- it's shared with a parent Task/Pipeline/Job run.
    SELECT @RunLogId AS RunLogId, @ActivityRunId AS ActivityRunId;
END;

GO

