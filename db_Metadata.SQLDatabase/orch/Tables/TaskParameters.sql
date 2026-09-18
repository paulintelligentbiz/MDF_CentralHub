CREATE TABLE [orch].[TaskParameters] (
    [TaskName]      VARCHAR (200) NOT NULL,
    [ParameterName] VARCHAR (200) NOT NULL,
    [Value]         VARCHAR (200) NOT NULL,
    CONSTRAINT [PK_TaskParameters] PRIMARY KEY CLUSTERED ([TaskName] ASC, [ParameterName] ASC),
    CONSTRAINT [FK_TaskParameters_Task] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName])
);

GO
