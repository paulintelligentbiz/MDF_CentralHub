CREATE TABLE [orch].[TaskWatermark] (
    [TaskName]                       VARCHAR (200)   NOT NULL,
    [WatermarkColumn]                VARCHAR (200)   NOT NULL,
    [WatermarkDataType]              VARCHAR (20)    NOT NULL,
    [WatermarkDateTimeValue]         DATETIME2 (7)   NULL,
    [WatermarkNumericValue]          DECIMAL (38, 0) NULL,
    [PreviousWatermarkDateTimeValue] DATETIME2 (7)   NULL,
    [PreviousWatermarkNumericValue]  DECIMAL (38, 0) NULL,
    [ModifiedUtc]                    DATETIME2 (7)   CONSTRAINT [DF_TaskWatermark_ModifiedUtc] DEFAULT (SYSUTCDATETIME()) NOT NULL,
    CONSTRAINT [PK_TaskWatermark] PRIMARY KEY CLUSTERED ([TaskName] ASC),
    CONSTRAINT [FK_TaskWatermark_Task] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName]),
    CONSTRAINT [FK_TaskWatermark_WatermarkDataType] FOREIGN KEY ([WatermarkDataType]) REFERENCES [orch].[WatermarkDataType] ([WatermarkDataType])
);


GO

