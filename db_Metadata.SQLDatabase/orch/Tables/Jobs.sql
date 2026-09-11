CREATE TABLE [orch].[Jobs] (
    [JobName]                VARCHAR (200)  NOT NULL,
    [Include]                BIT            CONSTRAINT [DF_WrapDAG_Include] DEFAULT ((1)) NOT NULL,
    [TimeoutInSeconds]       INT            NOT NULL,
    [Retries]                INT            NOT NULL,
    [RetryIntervalInSeconds] INT            NOT NULL,
    [ScheduledStartUTC]      TIME (0)       NULL,
    [ParametersJson]         NVARCHAR (MAX) NULL,
    [Dependencies]           NVARCHAR (MAX) NULL,
    [WorkspaceName]          VARCHAR (200)  NULL,
    [Environment]            NVARCHAR (100) NULL,
    [LoggingLevel]           TINYINT        CONSTRAINT [DF_Jobs_LoggingLevel] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_Jobs] PRIMARY KEY CLUSTERED ([JobName] ASC),
    CONSTRAINT [FK_Jobs_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel])
);


GO

