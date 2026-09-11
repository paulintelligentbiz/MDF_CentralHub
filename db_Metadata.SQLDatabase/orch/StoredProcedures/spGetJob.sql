CREATE   PROCEDURE orch.spGetJob
    @JobName varchar(200) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- No JobName given (NULL, or '' -- what an unset Fabric pipeline string
    -- parameter arrives as) -- fall back to the first Job on file, ordered by
    -- JobName (Jobs' own clustered index order) so the choice is deterministic.
    IF @JobName IS NULL OR @JobName = ''
        SELECT TOP (1) * FROM orch.Jobs ORDER BY JobName ASC;
    ELSE
        SELECT * FROM orch.Jobs WHERE JobName = @JobName;
END;

GO
