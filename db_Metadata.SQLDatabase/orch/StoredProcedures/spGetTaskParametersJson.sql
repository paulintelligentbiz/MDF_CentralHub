CREATE   PROCEDURE orch.spGetTaskParametersJson
    @TaskName       varchar(200),
    @ParametersJson nvarchar(max)  -- the Task's own orch.Tasks.ParametersJson, passed through as-is
AS
BEGIN
    SET NOCOUNT ON;

    -- Merges in watermarkColumn/watermarkValue from orch.TaskWatermark when this Task has a
    -- row there; a Task with no TaskWatermark row is a full load, so @ParametersJson comes
    -- back unchanged -- the caller (pl_Task_Executor) doesn't need to know which case it is.
    DECLARE @Result nvarchar(max) = @ParametersJson;

    SELECT
        @Result = JSON_MODIFY(
                      JSON_MODIFY(@Result, '$.watermarkColumn', tw.WatermarkColumn),
                      '$.watermarkValue',
                      CASE tw.WatermarkDataType
                          WHEN 'DateTime' THEN CONVERT(varchar(33), tw.WatermarkDateTimeValue, 127)  -- ISO 8601, matches the notebook's isoformat() on write-back
                          WHEN 'Numeric'  THEN CONVERT(varchar(50), tw.WatermarkNumericValue)
                      END
                  )
    FROM orch.TaskWatermark tw
    WHERE tw.TaskName = @TaskName;

    SELECT @Result AS ParametersJson;
END;

GO
