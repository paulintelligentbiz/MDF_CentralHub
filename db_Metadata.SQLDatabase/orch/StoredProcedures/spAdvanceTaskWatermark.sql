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

    DECLARE @NewWatermarkValue nvarchar(100) = JSON_VALUE(@NotebookExitJson, '$.newWatermarkValue');

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
    -- becomes this new row's Previous*. No existing row at all means this Task was never
    -- seeded as watermark-tracked in the first place -- a harmless no-op, same as the
    -- "not watermark-tracked" case above.
    DECLARE @PriorWatermarkColumn   varchar(200),
            @PriorWatermarkDataType varchar(20),
            @PriorDateTimeValue     datetime2(7),
            @PriorNumericValue     decimal(38, 0);

    SELECT TOP (1)
        @PriorWatermarkColumn = WatermarkColumn,
        @PriorWatermarkDataType = WatermarkDataType,
        @PriorDateTimeValue = WatermarkDateTimeValue,
        @PriorNumericValue = WatermarkNumericValue
    FROM orch.TaskWatermark
    WHERE TaskName = @TaskName
    ORDER BY RunDateTimeUtc DESC;

    IF @PriorWatermarkColumn IS NULL
        RETURN;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Un-flag the old current row before inserting the new one, not after -- both would
        -- otherwise transiently violate UQ_TaskWatermark_CurrentPerTask (at most one current
        -- row per Task) between the two statements.
        UPDATE orch.TaskWatermark
        SET IsCurrentWatermark = 0
        WHERE TaskName = @TaskName AND IsCurrentWatermark = 1;

        INSERT INTO orch.TaskWatermark (
            TaskName, WatermarkColumn, WatermarkDataType,
            WatermarkDateTimeValue, WatermarkNumericValue,
            PreviousWatermarkDateTimeValue, PreviousWatermarkNumericValue,
            IsCurrentWatermark, RunDateTimeUtc
        )
        VALUES (
            @TaskName, @PriorWatermarkColumn, @PriorWatermarkDataType,
            CASE WHEN @PriorWatermarkDataType = 'DateTime' THEN TRY_CONVERT(datetime2(7), @NewWatermarkValue) END,
            CASE WHEN @PriorWatermarkDataType = 'Numeric'  THEN TRY_CONVERT(decimal(38, 0), @NewWatermarkValue) END,
            -- Reset run: cleared to NULL, same as before -- the destination was just wiped and
            -- reloaded, so the old value no longer corresponds to anything recoverable.
            CASE WHEN @ResetWatermark = 0 AND @PriorWatermarkDataType = 'DateTime' THEN @PriorDateTimeValue END,
            CASE WHEN @ResetWatermark = 0 AND @PriorWatermarkDataType = 'Numeric'  THEN @PriorNumericValue END,
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
