CREATE   PROCEDURE orch.spGetRunStatus
    @JobName varchar(200),
    @RunID nvarchar(200)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TotalTasks int = (SELECT COUNT(*) FROM orch.Tasks WHERE JobName = @JobName AND Include = 1);
    DECLARE @SucceededTasks int = (
        SELECT COUNT(DISTINCT TaskName) FROM log.RunLog
        WHERE RunID = @RunID AND JobName = @JobName AND Status = 'Succeeded'
    );
    DECLARE @FailedTasks int = (
        SELECT COUNT(DISTINCT TaskName) FROM log.RunLog
        WHERE RunID = @RunID AND JobName = @JobName AND Status = 'Failed'
    );

    SELECT
        @TotalTasks AS TotalTasks,
        @SucceededTasks AS SucceededTasks,
        @FailedTasks AS FailedTasks,
        CASE
            WHEN @FailedTasks > 0 THEN 'Failed'
            WHEN @SucceededTasks >= @TotalTasks THEN 'Complete'
            ELSE 'Running'
        END AS RunStatus;
END;

GO

