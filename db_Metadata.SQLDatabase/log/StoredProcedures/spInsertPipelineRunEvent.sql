CREATE   PROCEDURE log.spInsertPipelineRunEvent
    @RunLogId       bigint,  -- required -- must reference an existing log.RunLog row
    @LoggingLevel   tinyint       = 1,
    @JobInstanceId  nvarchar(100) = NULL,
    @ItemId         nvarchar(100) = NULL,
    @JobType        nvarchar(50)  = NULL,
    @InvokeType     nvarchar(50)  = NULL,
    @Status         nvarchar(50)  = NULL,
    @RootActivityId nvarchar(100) = NULL,
    @StartTimeUtc   datetime2     = NULL,
    @EndTimeUtc     datetime2     = NULL,
    @FailureReason  nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.PipelineRunEvent (RunLogId, LoggingLevel, JobInstanceId, ItemId, JobType, InvokeType, Status, RootActivityId, StartTimeUtc, EndTimeUtc, FailureReason)
    VALUES (@RunLogId, @LoggingLevel, @JobInstanceId, @ItemId, @JobType, @InvokeType, @Status, @RootActivityId, @StartTimeUtc, @EndTimeUtc, @FailureReason);
END;

GO

