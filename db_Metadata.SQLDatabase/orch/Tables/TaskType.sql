CREATE TABLE [orch].[TaskType] (
    [TaskType]      VARCHAR (50) NOT NULL,
    [AllowParallel] BIT          CONSTRAINT [DF_TaskType_AllowParallel] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_TaskType] PRIMARY KEY CLUSTERED ([TaskType] ASC)
);


GO

