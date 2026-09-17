CREATE TABLE [orch].[TaskWatermark] (
    -- Surrogate key -- TaskName can no longer be the PK now that this table is append-only
    -- (one new row per advance, not one row updated in place; see orch.spAdvanceTaskWatermark).
    [TaskWatermarkId]                BIGINT          IDENTITY (1, 1) NOT NULL,
    [TaskName]                       VARCHAR (200)   NOT NULL,
    [WatermarkColumn]                VARCHAR (200)   NOT NULL,
    [WatermarkDataType]              VARCHAR (20)    NOT NULL,
    [WatermarkDateTimeValue]         DATETIME2 (7)   NULL,
    [WatermarkNumericValue]          DECIMAL (38, 0) NULL,
    [PreviousWatermarkDateTimeValue] DATETIME2 (7)   NULL,
    [PreviousWatermarkNumericValue]  DECIMAL (38, 0) NULL,
    -- Exactly one row per TaskName has this set to 1 -- the row orch.spGetTaskParametersJson
    -- actually reads. Enforced below by UQ_TaskWatermark_CurrentPerTask, not just proc discipline.
    [IsCurrentWatermark]             BIT             CONSTRAINT [DF_TaskWatermark_IsCurrentWatermark] DEFAULT ((1)) NOT NULL,
    [RunDateTimeUtc]                 DATETIME2 (7)   CONSTRAINT [DF_TaskWatermark_RunDateTimeUtc] DEFAULT (GETDATE()) NOT NULL,
    CONSTRAINT [PK_TaskWatermark] PRIMARY KEY CLUSTERED ([TaskWatermarkId] ASC),
    CONSTRAINT [FK_TaskWatermark_Task] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName]),
    CONSTRAINT [CK_TaskWatermark_WatermarkDataType] CHECK ([WatermarkDataType] IN ('DateTime', 'Numeric'))
);

GO

CREATE UNIQUE INDEX [UQ_TaskWatermark_CurrentPerTask]
    ON [orch].[TaskWatermark]([TaskName] ASC)
    WHERE ([IsCurrentWatermark]=(1));

GO
