CREATE   PROCEDURE orch.spAdvanceTaskWatermark
    @TaskName         varchar(200),
    @NotebookExitJson nvarchar(max) = NULL  -- nb_CopyTableToLakehouse's notebookutils.notebook.exit(...) payload;
                                             -- empty/NULL for a full load, since that notebook only calls
                                             -- notebook.exit(...) when it ran incrementally
AS
BEGIN
    SET NOCOUNT ON;

    IF @NotebookExitJson IS NULL OR LTRIM(RTRIM(@NotebookExitJson)) = ''
        RETURN;  -- full load -- no watermark to advance

    DECLARE @NewWatermarkValue nvarchar(100) = JSON_VALUE(@NotebookExitJson, '$.newWatermarkValue');

    IF @NewWatermarkValue IS NULL
        RETURN;  -- incremental run read 0 new rows -- watermark stays where it was

    -- Only matches a row if this Task is watermark-driven (TaskWatermark.TaskName exists);
    -- otherwise a harmless no-op. Shifts current -> Previous* before overwriting current, so
    -- the prior high-water mark is always recoverable if this run needs to be rolled back.
    UPDATE tw
    SET
        PreviousWatermarkDateTimeValue = CASE WHEN tw.WatermarkDataType = 'DateTime' THEN tw.WatermarkDateTimeValue ELSE tw.PreviousWatermarkDateTimeValue END,
        WatermarkDateTimeValue         = CASE WHEN tw.WatermarkDataType = 'DateTime' THEN TRY_CONVERT(datetime2(7), @NewWatermarkValue) ELSE tw.WatermarkDateTimeValue END,
        PreviousWatermarkNumericValue  = CASE WHEN tw.WatermarkDataType = 'Numeric'  THEN tw.WatermarkNumericValue ELSE tw.PreviousWatermarkNumericValue END,
        WatermarkNumericValue          = CASE WHEN tw.WatermarkDataType = 'Numeric'  THEN TRY_CONVERT(decimal(38, 0), @NewWatermarkValue) ELSE tw.WatermarkNumericValue END,
        ModifiedUtc                    = SYSUTCDATETIME()
    FROM orch.TaskWatermark tw
    WHERE tw.TaskName = @TaskName;
END;

GO
