CREATE   PROCEDURE log.spInsertPipelineRunEvent
    @PipelineRunEventId bigint,  -- required -- the PipelineRunEvent row opened by spLogPipelineRunEvent
    @RunID          nvarchar(200)  = NULL,
    @JobName        nvarchar(200)  = NULL,
    @TaskName       nvarchar(200)  = NULL,
    @TaskType       nvarchar(50)   = NULL,
    @Status         nvarchar(50)   = NULL,
    @EndTimeUtc     datetime2      = NULL,
    @ExitValue      nvarchar(max)  = NULL,
    @FailureReason  nvarchar(max)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Regardless of Status (Succeeded or Failed), stamp an end time -- never leave a closed
    -- record's end time NULL just because a caller omitted it.
    SET @EndTimeUtc = ISNULL(@EndTimeUtc, SYSUTCDATETIME());

    -- Closing entry in the RunLog ledger -- matches the pattern spInsertJobRunEvent /
    -- spInsertTaskRunEvent / spInsertActivityRunEvent already use.
    INSERT INTO log.RunLog (RunID, JobName, TaskName, TaskType, StopTime, Status, ExitValue, ErrorMessage)
    VALUES (@RunID, @JobName, @TaskName, @TaskType, @EndTimeUtc, @Status, @ExitValue, @FailureReason);

    -- Close out the single PipelineRunEvent row opened by spLogPipelineRunEvent.
    -- (This used to be a plain INSERT, which would have duplicated the RunLogId already used
    -- by spLogPipelineRunEvent and violated PipelineRunEvent's primary key -- fixed to UPDATE,
    -- matching the same fix already applied to JobRunEvent/ActivityRunEvent/TaskRunEvent.)
    UPDATE log.PipelineRunEvent
    SET Status = @Status,
        EndTimeUtc = @EndTimeUtc,
        FailureReason = @FailureReason
    WHERE PipelineRunEventId = @PipelineRunEventId;
END;

GO
