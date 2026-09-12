CREATE TABLE [log].[ActivityRunEvent] (
    -- Already uniquely named (this table's actual PK) -- no rename needed here. Still the FK
    -- to the RunLog ledger entry shared with the parent Task run (see spLogActivityRunEvent).
    [RunLogId]                BIGINT          NOT NULL,
    -- The Job run this activity executed under, if any -- lets a Job's descendants be found
    -- by a real FK join instead of by RunLogId value equality.
    [JobRunEventId]           BIGINT          NULL,
    [LoggingLevel]            TINYINT         NOT NULL,
    [ActivityRunId]           NVARCHAR (100)  NOT NULL,
    [TaskName]                NVARCHAR (200)  NULL,
    [TaskInstanceId]          NVARCHAR (100)  NULL,
    [ActivityName]            NVARCHAR (200)  NULL,
    [ActivityType]            NVARCHAR (100)  NULL,
    [LinkedServiceName]       NVARCHAR (200)  NULL,
    [Status]                  NVARCHAR (50)   NULL,
    [ActivityRunStart]        DATETIME2 (7)   NULL,
    [ActivityRunEnd]          DATETIME2 (7)   NULL,
    [DurationInMs]            INT             NULL,
    [InputJson]               NVARCHAR (MAX)  NULL,
    [OutputJson]              NVARCHAR (MAX)  NULL,
    [ErrorCode]               NVARCHAR (100)  NULL,
    [ErrorMessage]            NVARCHAR (MAX)  NULL,
    [ErrorFailureType]        NVARCHAR (100)  NULL,
    [ErrorTarget]             NVARCHAR (200)  NULL,
    [ErrorDetails]            NVARCHAR (MAX)  NULL,
    [RetryAttempt]            INT             NULL,
    [IterationHash]           NVARCHAR (200)  NULL,
    [UserPropertiesJson]      NVARCHAR (MAX)  NULL,
    [RecoveryStatus]          NVARCHAR (50)   NULL,
    [IntegrationRuntimeNames] NVARCHAR (MAX)  NULL,
    [ExecutionDetailsJson]    NVARCHAR (MAX)  NULL,
    [ResourceId]              NVARCHAR (1000) NULL,
    CONSTRAINT [PK_ActivityRunEvent] PRIMARY KEY CLUSTERED ([ActivityRunId] ASC),
    CONSTRAINT [FK_ActivityRunEvent_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_ActivityRunEvent_RunLog] FOREIGN KEY ([RunLogId]) REFERENCES [log].[RunLog] ([RunLogId]),
    CONSTRAINT [FK_ActivityRunEvent_JobRunEvent] FOREIGN KEY ([JobRunEventId]) REFERENCES [log].[JobRunEvent] ([JobRunEventId])
);


GO

