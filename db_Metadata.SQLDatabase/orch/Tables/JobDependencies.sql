CREATE TABLE [orch].[JobDependencies] (
    [JobName]          VARCHAR (200) NOT NULL,
    [DependentJobName] VARCHAR (200) NOT NULL,
    [DependsOn]        VARCHAR (20)  NOT NULL,
    CONSTRAINT [PK_JobDependencies] PRIMARY KEY CLUSTERED ([JobName] ASC, [DependentJobName] ASC),
    CONSTRAINT [FK_JobDependencies_DependsOn] FOREIGN KEY ([DependsOn]) REFERENCES [orch].[DependencyCondition] ([DependencyCondition]),
    CONSTRAINT [FK_JobDependencies_Job] FOREIGN KEY ([JobName]) REFERENCES [orch].[Jobs] ([JobName])
);


GO

