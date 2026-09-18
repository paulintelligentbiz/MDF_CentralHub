CREATE   PROCEDURE orch.spGetTaskParametersJson
    @TaskName       varchar(200),
    @ParametersJson nvarchar(max)  -- the Task's own orch.Tasks.ParametersJson, passed through as-is
AS
BEGIN
    SET NOCOUNT ON;

    -- Merges in updateOption (orch.Tasks.UpdateOption -- always present, so this always runs)
    -- and, when this Task has a row there, watermarkColumn/watermarkValue from
    -- orch.TaskWatermark; a Task with no TaskWatermark row is a full load, so those two keys
    -- are left out. This always returns real JSON -- '{"updateOption":...}' at minimum, even
    -- for a Task with no ParametersJson of its own -- never NULL, so the caller
    -- (pl_Task_Executor) doesn't need to know which case it is, and neither does whatever
    -- notebook eventually parses this.
    --
    -- orch.TaskWatermark is append-only (one row per past advance, not one row per Task), so
    -- this must filter to IsCurrentWatermark = 1 -- without it, a Task with watermark history
    -- would match multiple rows here, and this SELECT @var = ... FROM ... pattern silently
    -- applies once per matching row in an unspecified order instead of erroring, which would
    -- pick an arbitrary historical value rather than the current one.
    --
    -- A Task with no parameters at all can reach here as '' rather than NULL -- the wave-runner
    -- pipelines coalesce a missing/null ParametersJson to '' before passing it through, since a
    -- JSON null value doesn't survive a string-typed pipeline parameter reliably. Normalize
    -- both to a bare '{}' rather than NULL: JSON_MODIFY(NULL, ...) always returns NULL, no
    -- matter how many merges follow, so a NULL/blank Task would make this proc return NULL
    -- outright -- and any notebook whose ParametersJson override arrives as NULL/"null" then
    -- crashes on its own json.loads(...)/.get(...) calls. '{}' merges normally and this proc
    -- always hands back real, parseable JSON, whether or not the Task has anything configured.
    IF LTRIM(RTRIM(ISNULL(@ParametersJson, ''))) = ''
        SET @ParametersJson = '{}';

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
