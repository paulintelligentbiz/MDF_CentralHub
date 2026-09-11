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
    @RunLogId                 bigint         = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));
    SET @ActivityRunId = ISNULL(@ActivityRunId, CONVERT(nvarchar(100), NEWID()));

    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, StopTime, ExitValue, ErrorMessage, Status)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @StopTime, @ExitValue, @ErrorMessage, @Status);

    SET @RunLogId = SCOPE_IDENTITY();

    INSERT INTO log.ActivityRunEvent (
        RunLogId, LoggingLevel, ActivityRunId, TaskName, TaskInstanceId, ActivityName, ActivityType,
        LinkedServiceName, Status, ActivityRunStart, ActivityRunEnd, DurationInMs, InputJson, OutputJson,
        ErrorCode, ErrorMessage, ErrorFailureType, ErrorTarget, ErrorDetails, RetryAttempt, IterationHash,
        UserPropertiesJson, RecoveryStatus, IntegrationRuntimeNames, ExecutionDetailsJson, ResourceId
    )
    VALUES (
        @RunLogId, @LoggingLevel, @ActivityRunId, @EventTaskName, @TaskInstanceId, @ActivityName, @ActivityType,
        @LinkedServiceName, @EventStatus, @ActivityRunStart, @ActivityRunEnd, @DurationInMs, @InputJson, @OutputJson,
        @ErrorCode, @EventErrorMessage, @ErrorFailureType, @ErrorTarget, @ErrorDetails, @RetryAttempt, @IterationHash,
        @UserPropertiesJson, @RecoveryStatus, @IntegrationRuntimeNames, @ExecutionDetailsJson, @ResourceId
    );
END;

GO

