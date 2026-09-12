CREATE   PROCEDURE log.spInsertJobRunEvent
    @JobRunEventId  bigint,  -- required -- the JobRunEvent row opened by spLogJobRunEvent
    @RunID          nvarchar(200),
    @JobName        varchar(200),
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

    -- Closing entry in the RunLog ledger.
    INSERT INTO log.RunLog (RunID, JobName, StopTime, Status, ExitValue, ErrorMessage)
    VALUES (@RunID, @JobName, @EndTimeUtc, @Status, @ExitValue, @FailureReason);

    -- Close out the single JobRunEvent row opened by spLogJobRunEvent.
    -- (Previously an INSERT here, which duplicated the RunLogId already used by
    -- spLogJobRunEvent and violated JobRunEvent's primary key -- fixed to UPDATE.)
    UPDATE log.JobRunEvent
    SET Status = @Status,
        EndTimeUtc = @EndTimeUtc,
        FailureReason = @FailureReason
    WHERE JobRunEventId = @JobRunEventId;
END;

GO

