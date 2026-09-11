CREATE   PROCEDURE log.spLogPipelineRunEvent
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
    -- PipelineRunEvent fields
    @LoggingLevel    tinyint       = 1,
    @JobInstanceId   nvarchar(100) = NULL,
    @ItemId          nvarchar(100) = NULL,
    @JobType         nvarchar(50)  = NULL,
    @InvokeType      nvarchar(50)  = NULL,
    @EventStatus     nvarchar(50)  = NULL,
    @RootActivityId  nvarchar(100) = NULL,
    @StartTimeUtc    datetime2     = NULL,
    @EndTimeUtc      datetime2     = NULL,
    @FailureReason   nvarchar(max) = NULL,
    @RunLogId        bigint        = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));

    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, StopTime, ExitValue, ErrorMessage, Status)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @StopTime, @ExitValue, @ErrorMessage, @Status);

    SET @RunLogId = SCOPE_IDENTITY();

    INSERT INTO log.JobRunEvent (RunLogId, LoggingLevel, JobInstanceId, ItemId, JobType, InvokeType, Status, RootActivityId, StartTimeUtc, EndTimeUtc, FailureReason)
    VALUES (@RunLogId, @LoggingLevel, @JobInstanceId, @ItemId, @JobType, @InvokeType, @EventStatus, @RootActivityId, @StartTimeUtc, @EndTimeUtc, @FailureReason);

    SELECT @RunLogId AS RunLogId;
END;

GO

