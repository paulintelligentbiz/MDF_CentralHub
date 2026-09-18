CREATE   PROCEDURE orch.spRenameTask
    @OldTaskName varchar(200),
    @NewTaskName varchar(200)
AS
BEGIN
    SET NOCOUNT ON;

    IF @OldTaskName = @NewTaskName
        RETURN;

    IF NOT EXISTS (SELECT 1 FROM orch.Tasks WHERE TaskName = @OldTaskName)
        THROW 50000, 'spRenameTask: @OldTaskName does not exist in orch.Tasks.', 1;

    IF EXISTS (SELECT 1 FROM orch.Tasks WHERE TaskName = @NewTaskName)
        THROW 50000, 'spRenameTask: @NewTaskName already exists in orch.Tasks.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Tasks.TaskName is a clustered PK with NO ACTION foreign keys pointing at it
        -- (TaskWatermark, TaskParameters, TaskDependencies), so it can't be UPDATEd in place
        -- while those children still reference the old value. Insert the renamed row first,
        -- repoint every reference to it, then delete the old row once nothing points at it
        -- anymore.
        INSERT INTO orch.Tasks (TaskName, Include, JobName, ObjectName, WorkspaceName,
            TimeoutInSeconds, Retries, RetryIntervalInSeconds, UpdateOption, ParametersJson,
            Dependencies, TaskType, System, Layer, LoggingLevel)
        SELECT @NewTaskName, Include, JobName, ObjectName, WorkspaceName,
            TimeoutInSeconds, Retries, RetryIntervalInSeconds, UpdateOption, ParametersJson,
            Dependencies, TaskType, System, Layer, LoggingLevel
        FROM orch.Tasks
        WHERE TaskName = @OldTaskName;

        -- TaskWatermark.TaskName is an FK to Tasks (no longer also its PK -- the table is
        -- append-only, keyed by its own TaskWatermarkId, so a Task can have many historical
        -- rows here). A plain UPDATE works for all of them at once, same as Tasks itself
        -- would if it weren't for the FK: the new Tasks row above already exists by the time
        -- this runs, so the FK is satisfied throughout.
        UPDATE orch.TaskWatermark SET TaskName = @NewTaskName WHERE TaskName = @OldTaskName;

        -- TaskParameters.TaskName is also an FK-only column (PK is (TaskName, ParameterName)),
        -- same reasoning as TaskWatermark above -- a plain UPDATE repoints every parameter row
        -- for this Task at once.
        UPDATE orch.TaskParameters SET TaskName = @NewTaskName WHERE TaskName = @OldTaskName;

        UPDATE orch.TaskDependencies SET TaskName = @NewTaskName WHERE TaskName = @OldTaskName;
        UPDATE orch.TaskDependencies SET DependentTaskName = @NewTaskName WHERE DependentTaskName = @OldTaskName;

        -- orch.Tasks.Dependencies is the older JSON-array form of task-to-task precedence
        -- (coexists with the relational orch.TaskDependencies table) -- rewrite any other
        -- task's array that names the old TaskName. Matching on the quoted value avoids a
        -- partial match against a longer name that merely starts with @OldTaskName.
        UPDATE orch.Tasks
        SET Dependencies = REPLACE(Dependencies, '"' + @OldTaskName + '"', '"' + @NewTaskName + '"')
        WHERE Dependencies LIKE '%"' + @OldTaskName + '"%';

        -- Denormalized, unenforced TaskName references in run history -- kept in sync so
        -- past runs still join/filter correctly under the task's new name.
        UPDATE log.RunLog SET TaskName = @NewTaskName WHERE TaskName = @OldTaskName;
        UPDATE log.TaskRunEvent SET TaskName = @NewTaskName WHERE TaskName = @OldTaskName;
        UPDATE log.ActivityRunEvent SET TaskName = @NewTaskName WHERE TaskName = @OldTaskName;

        DELETE FROM orch.Tasks WHERE TaskName = @OldTaskName;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO
