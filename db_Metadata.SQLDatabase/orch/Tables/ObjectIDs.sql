CREATE TABLE [orch].[ObjectIDs] (
    [WorkspaceName] VARCHAR (200) NOT NULL,
    [ObjectName]    VARCHAR (200) NOT NULL,
    [ObjectID]      VARCHAR (200) NOT NULL,
    [WorkspaceID]   VARCHAR (200) NULL,
    CONSTRAINT [PK_ObjectIDs] PRIMARY KEY CLUSTERED ([WorkspaceName] ASC, [ObjectName] ASC)
);


GO

