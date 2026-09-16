CREATE   PROCEDURE orch.spRenameJob
    @OldJobName varchar(200),
    @NewJobName varchar(200)
AS
BEGIN
    SET NOCOUNT ON;

    IF @OldJobName = @NewJobName
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM orch.Jobs WHERE JobName = @OldJobName)
        THROW 50000, 'spRenameJob: @OldJobName does not exist in orch.Jobs.', 1;

    IF EXISTS (SELECT 1 FROM orch.Jobs WHERE JobName = @NewJobName)
        THROW 50000, 'spRenameJob: @NewJobName already exists in orch.Jobs.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Jobs.JobName is a clustered PK with NO ACTION foreign keys pointing at it (Tasks,
        -- JobDependencies), so it can't be UPDATEd in place while those children still
        -- reference the old value. Insert the renamed row first, repoint every reference to
        -- it, then delete the old row once nothing points at it anymore.
        INSERT INTO orch.Jobs (JobName, Include, TimeoutInSeconds, Retries, RetryIntervalInSeconds,
            UpdateObjectIDs, UpdateParallelBatchLimit, ParallelBatchLimit, ScheduledStartUTC,
            ParametersJson, Dependencies, WorkspaceName, Environment, LoggingLevel)
        SELECT @NewJobName, Include, TimeoutInSeconds, Retries, RetryIntervalInSeconds,
            UpdateObjectIDs, UpdateParallelBatchLimit, ParallelBatchLimit, ScheduledStartUTC,
            ParametersJson, Dependencies, WorkspaceName, Environment, LoggingLevel
        FROM orch.Jobs
        WHERE JobName = @OldJobName;

        UPDATE orch.Tasks SET JobName = @NewJobName WHERE JobName = @OldJobName;
        UPDATE orch.JobDependencies SET JobName = @NewJobName WHERE JobName = @OldJobName;
        UPDATE orch.JobDependencies SET DependentJobName = @NewJobName WHERE DependentJobName = @OldJobName;

        -- orch.Jobs.Dependencies is the older JSON-array form of job-to-job precedence
        -- (coexists with the relational orch.JobDependencies table) -- rewrite any other
        -- job's array that names the old JobName. Matching on the quoted value avoids a
        -- partial match against a longer name that merely starts with @OldJobName.
        UPDATE orch.Jobs
        SET Dependencies = REPLACE(Dependencies, '"' + @OldJobName + '"', '"' + @NewJobName + '"')
        WHERE Dependencies LIKE '%"' + @OldJobName + '"%';

        -- Denormalized, unenforced JobName references in run history -- kept in sync so past
        -- runs still join/filter correctly under the job's new name.
        UPDATE log.RunLog SET JobName = @NewJobName WHERE JobName = @OldJobName;
        UPDATE log.JobRunEvent SET JobName = @NewJobName WHERE JobName = @OldJobName;
        UPDATE log.TaskRunEvent SET JobName = @NewJobName WHERE JobName = @OldJobName;

        DELETE FROM orch.Jobs WHERE JobName = @OldJobName;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO
