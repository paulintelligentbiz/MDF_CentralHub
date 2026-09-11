CREATE TABLE [log].[RunLog] (
    [RunLogId]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [RunID]        NVARCHAR (200) NOT NULL,
    [JobName]      NVARCHAR (200) NULL,
    [ObjectPath]   NVARCHAR (400) NULL,
    [TaskName]     NVARCHAR (200) NULL,
    [StartTime]    DATETIME2 (0)  NULL,
    [StopTime]     DATETIME2 (0)  NULL,
    [ExitValue]    NVARCHAR (MAX) NULL,
    [ErrorMessage] NVARCHAR (MAX) NULL,
    [Status]       NVARCHAR (50)  NULL,
    [TaskType]     NVARCHAR (50)  NULL,
    CONSTRAINT [PK_run_logs] PRIMARY KEY CLUSTERED ([RunLogId] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_run_logs_RunID_Alias]
    ON [log].[RunLog]([RunID] ASC, [TaskName] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_run_logs_StartTime]
    ON [log].[RunLog]([StartTime] ASC);


GO

