USE reverse_etl_poc_db;
go CREATE
OR ALTER VIEW dbo.vw_customer_weekly_signals AS
SELECT
    user_id,
    batch_week_start,
    batch_week_end,
    COUNT(1) AS weekly_event_count,
    SUM(COALESCE(price, 0)) AS weekly_total_spend,
    MAX(event_time) AS last_event_time
FROM
    openrowset(
        BULK 'reverse_etl/stg_raw_events/**',
        data_source = 'ds_revetl',
        format = 'PARQUET'
    ) AS s
GROUP BY
    user_id,
    batch_week_start,
    batch_week_end;
go
