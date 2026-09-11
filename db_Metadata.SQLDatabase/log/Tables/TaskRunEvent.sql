CREATE TABLE [log].[TaskRunEvent] (
    [RunLogId]     BIGINT        NOT NULL,
    [JobName]      VARCHAR (200) NOT NULL,
    [TaskName]     VARCHAR (200) NOT NULL,
    [LoggingLevel] TINYINT       NOT NULL,
    [Status]       NVARCHAR (50) NULL,
    [StartTimeUtc] DATETIME2 (7) NULL,
    [EndTimeUtc]   DATETIME2 (7) NULL,
    CONSTRAINT [PK_TaskRunEvent] PRIMARY KEY CLUSTERED ([RunLogId] ASC),
    CONSTRAINT [FK_TaskRunEvent_JobName] FOREIGN KEY ([JobName]) REFERENCES [orch].[Jobs] ([JobName]),
    CONSTRAINT [FK_TaskRunEvent_TaskName] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName]),
    CONSTRAINT [FK_TaskRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_TaskRunEvent_RunLog] FOREIGN KEY ([RunLogId]) REFERENCES [log].[RunLog] ([RunLogId])
);


GO
