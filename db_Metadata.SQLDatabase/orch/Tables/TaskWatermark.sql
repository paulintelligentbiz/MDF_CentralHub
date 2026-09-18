CREATE TABLE [orch].[TaskWatermark] (
    -- Surrogate key -- TaskName can no longer be the PK now that this table is append-only
    -- (one new row per advance, not one row updated in place; see orch.spAdvanceTaskWatermark).
    [TaskWatermarkId]         BIGINT        IDENTITY (1, 1) NOT NULL,
    [TaskName]                VARCHAR (200) NOT NULL,
    [WatermarkColumn]         VARCHAR (200) NOT NULL,
    [WatermarkDataType]       VARCHAR (20)  NOT NULL,
    -- Single generic text column instead of one typed column per WatermarkDataType --
    -- WatermarkDataType now has a Char option (arbitrary tracking value, not a date or a
    -- number), which doesn't fit a strongly-typed column at all, and a value already arrives
    -- as text from the notebook's exit payload / gets read back as text by
    -- orch.spGetTaskParametersJson, so round-tripping it through a typed column added a
    -- conversion step without adding any real validation.
    [WatermarkValue]          NVARCHAR (200) NULL,
    [PreviousWatermarkValue]  NVARCHAR (200) NULL,
    -- Exactly one row per TaskName has this set to 1 -- the row orch.spGetTaskParametersJson
    -- actually reads. Enforced below by UQ_TaskWatermark_CurrentPerTask, not just proc discipline.
    [IsCurrentWatermark]      BIT           CONSTRAINT [DF_TaskWatermark_IsCurrentWatermark] DEFAULT ((1)) NOT NULL,
    [RunDateTimeUtc]          DATETIME2 (7) CONSTRAINT [DF_TaskWatermark_RunDateTimeUtc] DEFAULT (GETDATE()) NOT NULL,
    CONSTRAINT [PK_TaskWatermark] PRIMARY KEY CLUSTERED ([TaskWatermarkId] ASC),
    CONSTRAINT [FK_TaskWatermark_Task] FOREIGN KEY ([TaskName]) REFERENCES [orch].[Tasks] ([TaskName]),
    CONSTRAINT [FK_TaskWatermark_WatermarkDataType] FOREIGN KEY ([WatermarkDataType]) REFERENCES [orch].[WatermarkDataType] ([WatermarkDataType])
);

GO

CREATE UNIQUE INDEX [UQ_TaskWatermark_CurrentPerTask]
    ON [orch].[TaskWatermark]([TaskName] ASC)
    WHERE ([IsCurrentWatermark]=(1));

GO
