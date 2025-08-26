{{ config(
    materialized = 'view',
    unique_key = 'date_id',
    tags = ['dimension', 'date'],
    description = 'Date dimension table extracted from time_stamp'
) }}

SELECT DISTINCT
  FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64)))) AS date_id,
  DATE(TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS date,
  EXTRACT(DAY FROM TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS day,
  EXTRACT(MONTH FROM TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS month,
  FORMAT_TIMESTAMP('%B', TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS month_name,
  EXTRACT(YEAR FROM TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS year,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS weekday,
  FORMAT_TIMESTAMP('%A', TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) AS weekday_name,
  IF(EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64))) IN (1, 7), TRUE, FALSE) AS is_weekend
FROM `unigap-project-461012.Glamira.glamira_summary`
WHERE time_stamp.long_value IS NOT NULL
