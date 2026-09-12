CREATE TABLE [log].[TaskRunEvent] (
    [TaskRunEventId] BIGINT        NOT NULL,
    -- The Job run this task executed under, if any -- lets a Job's descendants be found by a
    -- real FK join instead of by RunLogId value equality.
    [JobRunEventId]  BIGINT        NULL,
    [JobName]        VARCHAR (200) NOT NULL,
    [TaskName]       VARCHAR (200) NOT NULL,
    [LoggingLevel]   TINYINT       NOT NULL,
    [Status]         NVARCHAR (50) NULL,
    [StartTimeUtc]   DATETIME2 (7) NULL,
    [EndTimeUtc]     DATETIME2 (7) NULL,
    CONSTRAINT [PK_TaskRunEvent] PRIMARY KEY CLUSTERED ([TaskRunEventId] ASC),
    CONSTRAINT [FK_TaskRunEvent_JobName] FOREIGN KEY ([JobName]) REFERENCES [orch].[Jobs] ([JobName]),
    CONSTRAINT [FK_TaskRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_TaskRunEvent_RunLog] FOREIGN KEY ([TaskRunEventId]) REFERENCES [log].[RunLog] ([RunLogId]),
    CONSTRAINT [FK_TaskRunEvent_TaskName] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName]),
    CONSTRAINT [FK_TaskRunEvent_JobRunEvent] FOREIGN KEY ([JobRunEventId]) REFERENCES [log].[JobRunEvent] ([JobRunEventId])
);


GO

