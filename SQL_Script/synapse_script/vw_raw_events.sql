USE reverse_etl_poc_db;
GO

CREATE OR ALTER VIEW dbo.vw_raw_events AS
SELECT
    CAST(event_time AS datetime2(7))      AS event_time,
    CAST(event_type AS varchar(50))       AS event_type,

    -- IDs as strings (important)
    CAST(product_id AS varchar(64))       AS product_id,
    CAST(category_id AS varchar(64))      AS category_id,
    CAST(user_id AS varchar(64))          AS user_id,

    CAST(category_code AS varchar(255))   AS category_code,
    CAST(brand AS varchar(100))           AS brand,
    CAST(price AS decimal(18,2))          AS price,
    CAST(user_session AS varchar(255))    AS user_session,

    CAST(batch_week_start AS date)        AS batch_week_start,
    CAST(batch_week_end AS date)          AS batch_week_end,
    CAST(ingested_at AS datetime2(7))     AS ingested_at
FROM OPENROWSET(
    BULK 'reverse_etl/raw_events/**',
    DATA_SOURCE = 'ds_revetl',
    FORMAT = 'PARQUET'
) AS src;
GO
