CREATE   PROCEDURE orch.spGetJob
    @JobName varchar(200)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT * FROM orch.Jobs WHERE JobName = @JobName;
END;

GO
