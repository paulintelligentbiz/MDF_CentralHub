CREATE   PROCEDURE orch.spGetTaskParametersJson
    @TaskName       varchar(200),
    @ParametersJson nvarchar(max)  -- the Task's own orch.Tasks.ParametersJson, passed through as-is
AS
BEGIN
    SET NOCOUNT ON;

    -- Merges in updateOption (orch.Tasks.UpdateOption -- always present, so this always runs)
    -- and, when this Task has a row there, watermarkColumn/watermarkValue from
    -- orch.TaskWatermark; a Task with no TaskWatermark row is a full load, so those two keys
    -- are left out and @ParametersJson otherwise comes back unchanged -- the caller
    -- (pl_Task_Executor) doesn't need to know which case it is.
    --
    -- orch.TaskWatermark is append-only (one row per past advance, not one row per Task), so
    -- this must filter to IsCurrentWatermark = 1 -- without it, a Task with watermark history
    -- would match multiple rows here, and this SELECT @var = ... FROM ... pattern silently
    -- applies once per matching row in an unspecified order instead of erroring, which would
    -- pick an arbitrary historical value rather than the current one.
    --
    -- A Task with no parameters at all can reach here as '' rather than NULL -- the wave-runner
    -- pipelines coalesce a missing/null ParametersJson to '' before passing it through, since a
    -- JSON null value doesn't survive a string-typed pipeline parameter reliably. JSON_MODIFY
    -- throws on '' (not valid JSON) but safely passes NULL straight through unchanged, so
    -- normalize '' to NULL here rather than have every caller guard against both.
    IF LTRIM(RTRIM(ISNULL(@ParametersJson, ''))) = ''
        SET @ParametersJson = NULL;

    DECLARE @Result nvarchar(max) = @ParametersJson;

    SELECT @Result = JSON_MODIFY(@Result, '$.updateOption', t.UpdateOption)
    FROM orch.Tasks t
    WHERE t.TaskName = @TaskName;

    -- WatermarkValue is stored as plain text already (not a typed column -- see
    -- orch.TaskWatermark), so this is a direct read with no per-WatermarkDataType conversion.
    SELECT
        @Result = JSON_MODIFY(
                      JSON_MODIFY(@Result, '$.watermarkColumn', tw.WatermarkColumn),
                      '$.watermarkValue', tw.WatermarkValue
                  )
    FROM orch.TaskWatermark tw
    WHERE tw.TaskName = @TaskName AND tw.IsCurrentWatermark = 1;

    SELECT @Result AS ParametersJson;
END;

GO
