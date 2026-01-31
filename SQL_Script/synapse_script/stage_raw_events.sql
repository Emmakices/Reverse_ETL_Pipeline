USE reverse_etl_poc_db;
GO

CREATE OR ALTER VIEW dbo.vw_stg_raw_events AS
SELECT
    event_time,
    event_type,
    product_id,
    category_id,
    user_id,
    category_code,
    brand,
    price,
    user_session,
    batch_week_start,
    batch_week_end,
    ingested_at
FROM OPENROWSET(
    BULK 'reverse_etl/stg_raw_events/**',
    DATA_SOURCE = 'ds_revetl',
    FORMAT = 'PARQUET'
) AS s;
GO


SELECT TOP 5 batch_week_start, batch_week_end, event_type
FROM dbo.vw_stg_raw_events;
