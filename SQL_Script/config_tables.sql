-- =========
-- 1) pipeline_run (1 row per weekly run)
-- =========
IF OBJECT_ID('dbo.pipeline_run', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.pipeline_run (
    run_id               UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    pipeline_name        NVARCHAR(100)    NOT NULL,
    batch_week_start     DATE             NOT NULL,
    batch_week_end       DATE             NOT NULL,

    pipeline_start_time  DATETIME2(3)     NOT NULL,
    pipeline_end_time    DATETIME2(3)     NULL,

    status               NVARCHAR(30)     NOT NULL,   -- SUCCESS / SUCCESS_WITH_REJECTS / FAILED
    correlation_id       NVARCHAR(50)     NULL,

    rows_fetched         INT              NULL,
    rows_output          INT              NULL,
    rows_rejected        INT              NULL,
    reject_rate_pct      DECIMAL(9,6)     NULL,
    reject_threshold_pct DECIMAL(9,6)     NULL,

    output_path          NVARCHAR(400)    NULL,
    rejects_path         NVARCHAR(400)    NULL,

    error_type           NVARCHAR(100)    NULL,
    error_message        NVARCHAR(MAX)    NULL,

    created_at_utc       DATETIME2(3)     NOT NULL DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_pipeline_run_lookup
    ON dbo.pipeline_run (pipeline_name, batch_week_start, batch_week_end);
END
GO

-- =========
-- 2) pipeline_stage (1 row per stage per run)
-- =========
IF OBJECT_ID('dbo.pipeline_stage', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.pipeline_stage (
    stage_id         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    run_id           UNIQUEIDENTIFIER     NOT NULL,
    stage_name       NVARCHAR(30)         NOT NULL,  -- EXTRACT / TRANSFORM / LOAD / SYNC
    stage_start_time DATETIME2(3)         NOT NULL,
    stage_end_time   DATETIME2(3)         NULL,
    status           NVARCHAR(30)         NOT NULL,  -- STARTED / SUCCESS / FAILED
    row_count        INT                  NULL,
    error_message    NVARCHAR(MAX)        NULL,
    created_at_utc   DATETIME2(3)         NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_pipeline_stage_run
      FOREIGN KEY (run_id) REFERENCES dbo.pipeline_run(run_id)
  );

  CREATE INDEX IX_pipeline_stage_run
    ON dbo.pipeline_stage (run_id, stage_name);
END
GO

-- =========
-- 3) pipeline_checkpoint (1 row per pipeline)
-- =========
IF OBJECT_ID('dbo.pipeline_checkpoint', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.pipeline_checkpoint (
    pipeline_name              NVARCHAR(100) NOT NULL PRIMARY KEY,

    last_successful_week_start DATE          NULL,
    last_successful_week_end   DATE          NULL,

    next_week_start            DATE          NOT NULL,
    next_week_end              DATE          NOT NULL,

    updated_at_utc             DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
  );
END
GO
