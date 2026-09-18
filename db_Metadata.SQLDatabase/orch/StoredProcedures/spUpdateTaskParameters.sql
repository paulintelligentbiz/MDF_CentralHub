CREATE   PROCEDURE orch.spUpdateTaskParameters
    @TaskName varchar(200) = NULL  -- NULL updates every Task with rows in orch.TaskParameters;
                                    -- otherwise just this one
AS
BEGIN
    SET NOCOUNT ON;

    -- Every Value is emitted as a JSON string, matching how ParametersJson is actually consumed
    -- everywhere else in this project (nb_CopySqlDemoTableToLakehouse etc. always read it back
    -- via params.get(...) as Python strings, converting further only where the caller already
    -- knows to) -- this doesn't try to infer numeric/boolean JSON types from the text.
    -- STRING_ESCAPE(..., 'json') handles quotes/backslashes/control characters in either
    -- ParameterName or Value correctly, so this stays a single set-based statement instead of
    -- looping row by row with JSON_MODIFY.
    ;WITH Built AS (
        SELECT
            tp.TaskName,
            '{' + STRING_AGG(
                      '"' + STRING_ESCAPE(tp.ParameterName, 'json') + '":"' + STRING_ESCAPE(tp.Value, 'json') + '"',
                      ','
                  ) WITHIN GROUP (ORDER BY tp.ParameterName) + '}' AS ParametersJson
        FROM orch.TaskParameters tp
        WHERE @TaskName IS NULL OR tp.TaskName = @TaskName
        GROUP BY tp.TaskName
    )
    UPDATE t
    SET t.ParametersJson = b.ParametersJson
    FROM orch.Tasks t
    JOIN Built b ON b.TaskName = t.TaskName;
END;

GO
