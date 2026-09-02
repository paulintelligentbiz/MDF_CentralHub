CREATE TABLE [orch].[Tasks] (
    [TaskName]               NVARCHAR (200) NOT NULL,
    [Include]                BIT            CONSTRAINT [DF_TranDAG_Include] DEFAULT ((1)) NOT NULL,
    [JobName]                VARCHAR (200)  NOT NULL,
    [ObjectName]             VARCHAR (200)  NOT NULL,
    [WorkspaceName]          VARCHAR (200)  NULL,
    [TimeoutInSeconds]       INT            NOT NULL,
    [Retries]                INT            NOT NULL,
    [RetryIntervalInSeconds] INT            NOT NULL,
    [ParametersJson]         NVARCHAR (MAX) NULL,
    [Dependencies]           NVARCHAR (MAX) NULL,
    [TaskType]               NVARCHAR (50)  NOT NULL,
    [System]                 NVARCHAR (100) NULL,
    [Layer]                  NVARCHAR (100) NULL,
    [LoggingLevel]           TINYINT        CONSTRAINT [DF_Tasks_LoggingLevel] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_Tran_DAG] PRIMARY KEY CLUSTERED ([TaskName] ASC),
    CONSTRAINT [FK_Tasks_Job] FOREIGN KEY ([JobName]) REFERENCES [orch].[Jobs] ([JobName]),
    CONSTRAINT [FK_Tasks_LoggingLevel] FOREIGN KEY ([LoggingLevel]) REFERENCES [log].[LoggingLevel] ([LoggingLevel]),
    CONSTRAINT [FK_Tasks_ObjectIDs_Job] FOREIGN KEY ([WorkspaceName], [JobName]) REFERENCES [orch].[ObjectIDs] ([WorkspaceName], [ObjectName]),
    CONSTRAINT [FK_Tasks_ObjectIDs_Object] FOREIGN KEY ([WorkspaceName], [ObjectName]) REFERENCES [orch].[ObjectIDs] ([WorkspaceName], [ObjectName]),
    CONSTRAINT [FK_Tasks_TaskType] FOREIGN KEY ([TaskType]) REFERENCES [orch].[TaskType] ([TaskType])
);


GO

