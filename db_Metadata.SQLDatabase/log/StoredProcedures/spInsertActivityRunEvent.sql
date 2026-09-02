CREATE   PROCEDURE log.spInsertActivityRunEvent
    @RunLogId                bigint,  -- required -- must reference an existing log.RunLog row
    @LoggingLevel            tinyint        = 1,
    @ActivityRunId           nvarchar(100)  = NULL,
    @PipelineName            nvarchar(200)  = NULL,
    @PipelineRunId           nvarchar(100)  = NULL,
    @ActivityName            nvarchar(200)  = NULL,
    @ActivityType            nvarchar(100)  = NULL,
    @LinkedServiceName       nvarchar(200)  = NULL,
    @Status                  nvarchar(50)   = NULL,
    @ActivityRunStart        datetime2      = NULL,
    @ActivityRunEnd          datetime2      = NULL,
    @DurationInMs            int            = NULL,
    @InputJson               nvarchar(max)  = NULL,
    @OutputJson              nvarchar(max)  = NULL,
    @ErrorCode               nvarchar(100)  = NULL,
    @ErrorMessage            nvarchar(max)  = NULL,
    @ErrorFailureType        nvarchar(100)  = NULL,
    @ErrorTarget             nvarchar(200)  = NULL,
    @ErrorDetails            nvarchar(max)  = NULL,
    @RetryAttempt            int            = NULL,
    @IterationHash           nvarchar(200)  = NULL,
    @UserPropertiesJson      nvarchar(max)  = NULL,
    @RecoveryStatus          nvarchar(50)   = NULL,
    @IntegrationRuntimeNames nvarchar(max)  = NULL,
    @ExecutionDetailsJson    nvarchar(max)  = NULL,
    @ResourceId              nvarchar(1000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @ActivityRunId = ISNULL(@ActivityRunId, CONVERT(nvarchar(100), NEWID()));

    INSERT INTO log.ActivityRunEvent (
        RunLogId, LoggingLevel, ActivityRunId, PipelineName, PipelineRunId, ActivityName, ActivityType,
        LinkedServiceName, Status, ActivityRunStart, ActivityRunEnd, DurationInMs, InputJson, OutputJson,
        ErrorCode, ErrorMessage, ErrorFailureType, ErrorTarget, ErrorDetails, RetryAttempt, IterationHash,
        UserPropertiesJson, RecoveryStatus, IntegrationRuntimeNames, ExecutionDetailsJson, ResourceId
    )
    VALUES (
        @RunLogId, @LoggingLevel, @ActivityRunId, @PipelineName, @PipelineRunId, @ActivityName, @ActivityType,
        @LinkedServiceName, @Status, @ActivityRunStart, @ActivityRunEnd, @DurationInMs, @InputJson, @OutputJson,
        @ErrorCode, @ErrorMessage, @ErrorFailureType, @ErrorTarget, @ErrorDetails, @RetryAttempt, @IterationHash,
        @UserPropertiesJson, @RecoveryStatus, @IntegrationRuntimeNames, @ExecutionDetailsJson, @ResourceId
    );
END;

GO

