{{ config(
    materialized = 'view',
    unique_key = 'location_id',
    tags = ['dimension', 'location'],
    description = 'Dimension table for IP-based geolocation data including city and country.'
) }}

SELECT
  ROW_NUMBER() OVER (ORDER BY ip) AS location_id,
  ip,
  city,
  country
FROM `unigap-project-461012.Ip2location.Ip2location`
WHERE ip IS NOT NULL
  AND city IS NOT NULL
  AND country IS NOT NULL
