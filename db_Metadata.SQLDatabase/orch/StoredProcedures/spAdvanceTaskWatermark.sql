CREATE   PROCEDURE orch.spAdvanceTaskWatermark
    @TaskName         varchar(200),
    @NotebookExitJson nvarchar(max) = NULL  -- nb_CopyTableToLakehouse's notebookutils.notebook.exit(...) payload;
                                             -- empty/NULL whenever this Task isn't watermark-tracked (no
                                             -- orch.TaskWatermark row), since the notebook only calls
                                             -- notebook.exit(...) when it knows a watermark column
AS
BEGIN
    SET NOCOUNT ON;

    IF @NotebookExitJson IS NULL OR LTRIM(RTRIM(@NotebookExitJson)) = ''
        RETURN;  -- not watermark-tracked -- no watermark to advance

    -- pl_Task_Executor's "Advance Task Watermark" step forwards every Notebook Task's exit
    -- value here unconditionally, regardless of whether that Task is actually part of the
    -- orch.TaskWatermark system at all (e.g. nb_Bronze_Generic isn't -- it tracks its own
    -- per-topic offsets elsewhere -- but still calls notebook.exit(...) with something). A
    -- non-JSON exit value used to reach JSON_VALUE below and crash the whole pipeline step with
    -- "JSON text is not properly formatted" instead of just being irrelevant to this proc.
    IF ISJSON(@NotebookExitJson) = 0
        RETURN;  -- not a JSON payload -- not something this proc can be advancing a watermark from

    -- Stored and read as plain text (orch.TaskWatermark.WatermarkValue is NVARCHAR, not a
    -- typed column) -- WatermarkDataType is metadata about how to interpret/compare the value,
    -- not something this proc converts through, since a 'Char' watermark isn't a date or a
    -- number at all.
    DECLARE @NewWatermarkValue nvarchar(200) = JSON_VALUE(@NotebookExitJson, '$.newWatermarkValue');

    IF @NewWatermarkValue IS NULL
        RETURN;  -- this run read 0 rows -- watermark stays where it was

    -- Tasks.UpdateOption = 'Overwrite' forces nb_CopyTableToLakehouse to read the source in
    -- full (ignoring any stored watermark value) and truncate+reload the destination; it flags
    -- that in its exit payload as resetWatermark: true. A normal incremental run leaves this
    -- false/absent. An unexpected NULL defaults to "not a reset" rather than silently discarding
    -- Previous* recoverability data.
    DECLARE @ResetWatermark bit = ISNULL(TRY_CONVERT(bit, JSON_VALUE(@NotebookExitJson, '$.resetWatermark')), 0);

    -- orch.TaskWatermark is append-only: each advance inserts a new row instead of mutating the
    -- existing one, so the full history of every value this Task's watermark has ever held is
    -- preserved. IsCurrentWatermark = 1 marks exactly one row per Task -- the one
    -- orch.spGetTaskParametersJson actually reads -- enforced by a filtered unique index
    -- (UQ_TaskWatermark_CurrentPerTask), not just this proc's discipline.
    --
    -- The most recent existing row (by RunDateTimeUtc) supplies the config
    -- (WatermarkColumn/WatermarkDataType, which don't change row-to-row) and the value that
    -- becomes this new row's Previous*.
    DECLARE @PriorWatermarkColumn   varchar(200),
            @PriorWatermarkDataType varchar(20),
            @PriorWatermarkValue    nvarchar(200);

    SELECT TOP (1)
        @PriorWatermarkColumn = WatermarkColumn,
        @PriorWatermarkDataType = WatermarkDataType,
        @PriorWatermarkValue = WatermarkValue
    FROM orch.TaskWatermark
    WHERE TaskName = @TaskName
    ORDER BY RunDateTimeUtc DESC;

    -- No existing row at all -- either this Task has never been seeded (nothing was ever
    -- inserted for it manually) or a prior orch.TaskWatermark wipe (e.g. a full Excel resync
    -- via SyncExcelToSQL) cleared its history. Either way this run's own exit payload is
    -- already a full read (there was no watermark to filter by), so its newWatermarkValue is
    -- exactly the value a manual seed would have used -- seed the initial row directly from
    -- it instead of no-op'ing, so a Task only ever needs seeding once, automatically, on
    -- whatever run happens to be its first.
    IF @PriorWatermarkColumn IS NULL
    BEGIN
        DECLARE @SeedWatermarkColumn varchar(200) = JSON_VALUE(@NotebookExitJson, '$.watermarkColumn');

        IF @SeedWatermarkColumn IS NULL
            RETURN;  -- exit payload doesn't name a watermark column -- nothing to seed

        -- The exit payload doesn't say which of orch.WatermarkDataType's values applies
        -- (that's normally read back from the prior row, which doesn't exist yet here), so
        -- infer it from whichever supported type the value actually parses as, DateTime
        -- first, then Numeric -- anything that parses as neither defaults to 'Char' (always
        -- succeeds, since a Char watermark accepts any text). This can misclassify a value
        -- that's genuinely meant to be Char but happens to also look like a date or a number
        -- (e.g. a purely numeric string cursor) -- if that matters for a given Task, seed its
        -- first row manually instead of relying on this inference.
        DECLARE @SeedWatermarkDataType varchar(20) =
            CASE
                WHEN TRY_CONVERT(datetime2(7), @NewWatermarkValue) IS NOT NULL THEN 'DateTime'
                WHEN TRY_CONVERT(decimal(38, 0), @NewWatermarkValue) IS NOT NULL THEN 'Numeric'
                ELSE 'Char'
            END;

        INSERT INTO orch.TaskWatermark (
            TaskName, WatermarkColumn, WatermarkDataType, WatermarkValue,
            IsCurrentWatermark, RunDateTimeUtc
        )
        VALUES (
            @TaskName, @SeedWatermarkColumn, @SeedWatermarkDataType, @NewWatermarkValue,
            1, GETDATE()
        );

        RETURN;
    END

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Un-flag the old current row before inserting the new one, not after -- both would
        -- otherwise transiently violate UQ_TaskWatermark_CurrentPerTask (at most one current
        -- row per Task) between the two statements.
        UPDATE orch.TaskWatermark
        SET IsCurrentWatermark = 0
        WHERE TaskName = @TaskName AND IsCurrentWatermark = 1;

        INSERT INTO orch.TaskWatermark (
            TaskName, WatermarkColumn, WatermarkDataType, WatermarkValue,
            PreviousWatermarkValue,
            IsCurrentWatermark, RunDateTimeUtc
        )
        VALUES (
            @TaskName, @PriorWatermarkColumn, @PriorWatermarkDataType, @NewWatermarkValue,
            -- Reset run: cleared to NULL, same as before -- the destination was just wiped and
            -- reloaded, so the old value no longer corresponds to anything recoverable.
            CASE WHEN @ResetWatermark = 0 THEN @PriorWatermarkValue END,
            1, GETDATE()
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;

GO
