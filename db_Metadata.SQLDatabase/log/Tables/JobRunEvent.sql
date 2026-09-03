CREATE TABLE [log].[JobRunEvent] (
    [RunLogId]       BIGINT         NOT NULL,
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
    CONSTRAINT [PK_JobRunEvent] PRIMARY KEY CLUSTERED ([RunLogId] ASC),
    CONSTRAINT [FK_JobRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_JobRunEvent_RunLog] FOREIGN KEY ([RunLogId]) REFERENCES [log].[RunLog] ([RunLogId])
);


GO
