CREATE TABLE [log].[LoggingLevel] (
    [LoggingLevel]     TINYINT       NOT NULL,
    [LoggingLevelName] NVARCHAR (20) NOT NULL,
    CONSTRAINT [PK_LoggingLevel] PRIMARY KEY CLUSTERED ([LoggingLevel] ASC)
);


GO

