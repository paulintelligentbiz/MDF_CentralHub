CREATE   PROCEDURE log.spInsertJobRunEvent
    @RunLogId       bigint,  -- required -- the JobRunEvent row opened by spLogJobRunEvent
    @RunID          nvarchar(200),
    @JobName        varchar(200),
    @Status         nvarchar(50)   = NULL,
    @EndTimeUtc     datetime2      = NULL,
    @ExitValue      nvarchar(max)  = NULL,
    @FailureReason  nvarchar(max)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

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
    WHERE RunLogId = @RunLogId;
END;

GO

