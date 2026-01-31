USE reverse_etl_poc_db;
GO

CREATE OR ALTER VIEW dbo.vw_reverse_etl_customer_weekly AS
SELECT
    user_id AS external_customer_id,
    batch_week_start,
    batch_week_end,
    SUM(COALESCE(price, 0)) AS total_spend_week,
    COUNT(1) AS activity_count_week,
    MAX(event_time) AS last_activity_time,
    CASE
        WHEN SUM(COALESCE(price, 0)) >= 5000 THEN 'HIGH'
        WHEN SUM(COALESCE(price, 0)) >= 1000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS customer_tier
FROM OPENROWSET(
    BULK 'reverse_etl/stg_raw_events/**',
    DATA_SOURCE = 'ds_revetl',
    FORMAT = 'PARQUET'
) AS s
GROUP BY
    user_id,
    batch_week_start,
    batch_week_end;
GO
