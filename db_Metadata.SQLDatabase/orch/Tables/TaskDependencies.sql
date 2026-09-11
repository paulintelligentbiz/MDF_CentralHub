CREATE TABLE [orch].[TaskDependencies] (
    [TaskName]          VARCHAR (200) NOT NULL,
    [DependentTaskName] VARCHAR (200) NOT NULL,
    [DependsOn]         VARCHAR (20)  NOT NULL,
    CONSTRAINT [PK_TaskDependencies] PRIMARY KEY CLUSTERED ([TaskName] ASC, [DependentTaskName] ASC),
    CONSTRAINT [FK_TaskDependencies_DependsOn] FOREIGN KEY ([DependsOn]) REFERENCES [orch].[DependencyCondition] ([DependencyCondition]),
    CONSTRAINT [FK_TaskDependencies_Task] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName])
);


GO

