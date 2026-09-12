CREATE   PROCEDURE log.spLogJobRunEvent
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
    -- JobRunEvent fields
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
    -- Pass the JobRunEventId an ancestor event already opened to fold this Job under that
    -- same run instead of minting an unrelated one -- see spLogActivityRunEvent for the
    -- pattern this is modeled on.
    @ExistingJobRunEventId bigint  = NULL,
    @JobRunEventId   bigint        = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @RunID = ISNULL(@RunID, CONVERT(nvarchar(200), NEWID()));

    IF @ExistingJobRunEventId IS NULL
    BEGIN
        INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, ObjectPath, StartTime, StopTime, ExitValue, ErrorMessage, Status)
        VALUES (@RunID, @JobName, @TaskName, @TaskType, @ObjectPath, @StartTime, @StopTime, @ExitValue, @ErrorMessage, @Status);

        SET @JobRunEventId = SCOPE_IDENTITY();
    END
    ELSE
        SET @JobRunEventId = @ExistingJobRunEventId;

    INSERT INTO log.JobRunEvent (JobRunEventId, JobName, LoggingLevel, JobInstanceId, ItemId, JobType, InvokeType, Status, RootActivityId, StartTimeUtc, EndTimeUtc, FailureReason)
    VALUES (@JobRunEventId, @JobName, @LoggingLevel, @JobInstanceId, @ItemId, @JobType, @InvokeType, @EventStatus, @RootActivityId, @StartTimeUtc, @EndTimeUtc, @FailureReason);

    SELECT @JobRunEventId AS JobRunEventId;
END;

GO

