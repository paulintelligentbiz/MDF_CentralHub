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
    -- false/absent.
    DECLARE @ResetWatermark bit = TRY_CONVERT(bit, JSON_VALUE(@NotebookExitJson, '$.resetWatermark'));

    -- Only matches a row if this Task is watermark-driven (TaskWatermark.TaskName exists);
    -- otherwise a harmless no-op.
    --
    -- Normal run: shifts current -> Previous* before overwriting current, so the prior
    -- high-water mark is always recoverable if this run needs to be rolled back.
    --
    -- Reset run (@ResetWatermark = 1): the destination table was just truncated and fully
    -- reloaded, so the old Previous* value doesn't correspond to anything recoverable anymore
    -- -- it's cleared to NULL instead of being shifted forward, and current is re-baselined to
    -- the high-water value computed from the full reload, so a later run switched back to
    -- Append/incremental resumes from the correct point.
    UPDATE tw
    SET
        PreviousWatermarkDateTimeValue = CASE
                                             WHEN @ResetWatermark = 1 THEN NULL
                                             WHEN tw.WatermarkDataType = 'DateTime' THEN tw.WatermarkDateTimeValue
                                             ELSE tw.PreviousWatermarkDateTimeValue
                                         END,
        WatermarkDateTimeValue         = CASE WHEN tw.WatermarkDataType = 'DateTime' THEN TRY_CONVERT(datetime2(7), @NewWatermarkValue) ELSE tw.WatermarkDateTimeValue END,
        PreviousWatermarkNumericValue  = CASE
                                             WHEN @ResetWatermark = 1 THEN NULL
                                             WHEN tw.WatermarkDataType = 'Numeric' THEN tw.WatermarkNumericValue
                                             ELSE tw.PreviousWatermarkNumericValue
                                         END,
        WatermarkNumericValue          = CASE WHEN tw.WatermarkDataType = 'Numeric'  THEN TRY_CONVERT(decimal(38, 0), @NewWatermarkValue) ELSE tw.WatermarkNumericValue END,
        ModifiedUtc                    = SYSUTCDATETIME()
    FROM orch.TaskWatermark tw
    WHERE tw.TaskName = @TaskName;
END;

GO
