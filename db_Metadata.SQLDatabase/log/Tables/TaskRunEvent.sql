CREATE TABLE [log].[TaskRunEvent] (
    [TaskRunEventId] BIGINT        NOT NULL,
    -- The Job run this task executed under, if any -- lets a Job's descendants be found by a
    -- real FK join instead of by RunLogId value equality.
    [JobRunEventId]  BIGINT        NULL,
    -- Denormalized, not FK-enforced (FK_TaskRunEvent_JobName dropped) -- same convention as
    -- ActivityRunEvent.TaskName: this row should still outlive a renamed/deleted orch.Jobs row.
    [JobName]        VARCHAR (200) NOT NULL,
    -- Denormalized, not FK-enforced (FK_TaskRunEvent_TaskName dropped) -- no log-schema table
    -- should hold a hard reference into orch that would block deleting/renaming an orch row.
    [TaskName]       VARCHAR (200) NOT NULL,
    [LoggingLevel]   TINYINT       NOT NULL,
    [Status]         NVARCHAR (50) NULL,
    [StartTimeUtc]   DATETIME2 (7) NULL,
    [EndTimeUtc]     DATETIME2 (7) NULL,
    CONSTRAINT [PK_TaskRunEvent] PRIMARY KEY CLUSTERED ([TaskRunEventId] ASC),
    CONSTRAINT [FK_TaskRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_TaskRunEvent_RunLog] FOREIGN KEY ([TaskRunEventId]) REFERENCES [log].[RunLog] ([RunLogId]),
    CONSTRAINT [FK_TaskRunEvent_JobRunEvent] FOREIGN KEY ([JobRunEventId]) REFERENCES [log].[JobRunEvent] ([JobRunEventId])
);


GO

