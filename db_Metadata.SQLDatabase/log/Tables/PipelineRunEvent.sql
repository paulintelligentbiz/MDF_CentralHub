CREATE TABLE [log].[PipelineRunEvent] (
    [PipelineRunEventId] BIGINT         NOT NULL,
    -- The Job run this pipeline executed under, if any -- lets a Job's descendants be found
    -- by a real FK join instead of by RunLogId value equality.
    [JobRunEventId]      BIGINT         NULL,
    [LoggingLevel]       TINYINT        NOT NULL,
    [JobInstanceId]      NVARCHAR (100) NULL,
    [ItemId]             NVARCHAR (100) NULL,
    [JobType]            NVARCHAR (50)  NULL,
    [InvokeType]         NVARCHAR (50)  NULL,
    [Status]             NVARCHAR (50)  NULL,
    [RootActivityId]     NVARCHAR (100) NULL,
    [StartTimeUtc]       DATETIME2 (7)  NULL,
    [EndTimeUtc]         DATETIME2 (7)  NULL,
    [FailureReason]      NVARCHAR (MAX) NULL,
    CONSTRAINT [PK_PipelineRunEvent] PRIMARY KEY CLUSTERED ([PipelineRunEventId] ASC),
    CONSTRAINT [FK_PipelineRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_PipelineRunEvent_RunLog] FOREIGN KEY ([PipelineRunEventId]) REFERENCES [log].[RunLog] ([RunLogId]),
    CONSTRAINT [FK_PipelineRunEvent_JobRunEvent] FOREIGN KEY ([JobRunEventId]) REFERENCES [log].[JobRunEvent] ([JobRunEventId])
);


GO

