{{ config(
    materialized = 'view',
    unique_key = 'store_id',
    tags = ['dimension', 'store'],
    description = 'Store dimension including mapping from store IP to location ID using IP2Location.'
) }}

WITH store_data AS (
  SELECT DISTINCT
    SAFE_CAST(store_id AS INT64) AS store_id,
    ip
  FROM `unigap-project-461012.Glamira.glamira_summary`
  WHERE store_id IS NOT NULL AND ip IS NOT NULL
),

location_data AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY ip) AS location_id,
    ip
  FROM `unigap-project-461012.Ip2location.Ip2location`
  WHERE ip IS NOT NULL
)

SELECT
  s.store_id,
  l.location_id
FROM store_data s
LEFT JOIN location_data l
  ON s.ip = l.ip
