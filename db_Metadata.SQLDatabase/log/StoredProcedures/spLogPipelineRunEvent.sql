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
    -- Pass the PipelineRunEventId an ancestor event already opened to fold this Pipeline run
    -- under that same run instead of minting an unrelated one -- see spLogActivityRunEvent
    -- for the pattern this is modeled on.
    @ExistingPipelineRunEventId bigint = NULL,
    -- The Job run that invoked this pipeline, if any -- stored as a real FK on
    -- PipelineRunEvent rather than relied on via a shared RunLogId value.
    @JobRunEventId   bigint        = NULL,
    @PipelineRunEventId bigint     = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));

    IF @ExistingPipelineRunEventId IS NULL
    BEGIN
        INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, StopTime, ExitValue, ErrorMessage, Status)
        VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @StopTime, @ExitValue, @ErrorMessage, @Status);

        SET @PipelineRunEventId = SCOPE_IDENTITY();
    END
    ELSE
        SET @PipelineRunEventId = @ExistingPipelineRunEventId;

    -- Was inserting into log.JobRunEvent -- a copy-paste leftover from spLogJobRunEvent that
    -- went unnoticed because no pipeline calls this proc yet. Fixed to the correct table.
    INSERT INTO log.PipelineRunEvent (PipelineRunEventId, JobRunEventId, LoggingLevel, JobInstanceId, ItemId, JobType, InvokeType, Status, RootActivityId, StartTimeUtc, EndTimeUtc, FailureReason)
    VALUES (@PipelineRunEventId, @JobRunEventId, @LoggingLevel, @JobInstanceId, @ItemId, @JobType, @InvokeType, @EventStatus, @RootActivityId, @StartTimeUtc, @EndTimeUtc, @FailureReason);

    SELECT @PipelineRunEventId AS PipelineRunEventId;
END;

GO

