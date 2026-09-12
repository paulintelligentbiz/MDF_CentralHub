-- JobRunEvent is the root of the run hierarchy: its own PK (JobRunEventId, renamed from the
-- generic RunLogId every event table used to share) is what PipelineRunEvent, TaskRunEvent
-- and ActivityRunEvent now carry as an explicit JobRunEventId FK, instead of correlating by
-- happening to hold the same RunLogId value.
CREATE TABLE [log].[JobRunEvent] (
    [JobRunEventId]  BIGINT         NOT NULL,
    [JobName]        VARCHAR (200)  NOT NULL,
    [LoggingLevel]   TINYINT        NOT NULL,
    [JobInstanceId]  NVARCHAR (100) NULL,
    [ItemId]         NVARCHAR (100) NULL,
    [JobType]        NVARCHAR (50)  NULL,
    [InvokeType]     NVARCHAR (50)  NULL,
    [Status]         NVARCHAR (50)  NULL,
    [RootActivityId] NVARCHAR (100) NULL,
    [StartTimeUtc]   DATETIME2 (7)  NULL,
    [EndTimeUtc]     DATETIME2 (7)  NULL,
    [FailureReason]  NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_JobRunEvent] PRIMARY KEY CLUSTERED ([JobRunEventId] ASC),
    CONSTRAINT [FK_JobRunEvent_JobName] FOREIGN KEY ([JobName]) REFERENCES [orch].[Jobs] ([JobName]),
    CONSTRAINT [FK_JobRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_JobRunEvent_RunLog] FOREIGN KEY ([JobRunEventId]) REFERENCES [log].[RunLog] ([RunLogId])
);


GO

