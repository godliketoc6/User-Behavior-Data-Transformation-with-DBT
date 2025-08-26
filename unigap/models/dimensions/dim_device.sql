{{ config(
    materialized = 'view',
    tags = ['dimension', 'device'],
    unique_key = 'device_key',
    description = 'Device dimension extracted from user_agent and device_id'
) }}

WITH device_analysis AS (
  SELECT DISTINCT
    COALESCE(NULLIF(device_id, ''), 'XNA') AS device_id,
    COALESCE(NULLIF(user_agent, ''), 'XNA') AS user_agent,
    
    -- Extract device type from user agent
    CASE 
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'mobile|android|iphone') THEN 'Mobile'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'tablet|ipad') THEN 'Tablet'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'windows|mac|linux') THEN 'Desktop'
      ELSE 'Unknown'
    END AS device_type,
    
    -- Extract browser
    CASE 
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'chrome') THEN 'Chrome'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'firefox') THEN 'Firefox'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'safari') THEN 'Safari'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'edge') THEN 'Edge'
      ELSE 'Other'
    END AS browser,
    
    -- Extract OS
    CASE 
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'windows') THEN 'Windows'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'mac|ios') THEN 'macOS/iOS'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'android') THEN 'Android'
      WHEN REGEXP_CONTAINS(LOWER(user_agent), r'linux') THEN 'Linux'
      ELSE 'Other'
    END AS operating_system
    
  FROM `unigap-project-461012.Glamira.glamira_summary`
  WHERE device_id IS NOT NULL OR user_agent IS NOT NULL
)

SELECT
  -- ✅ PRIMARY KEY
  ROW_NUMBER() OVER (ORDER BY device_id, user_agent) AS device_key,
  
  -- 🧩 ATTRIBUTES
  device_id,
  user_agent,
  device_type,
  browser,
  operating_system,
  
  -- 📱 CATEGORIZATION
  CASE 
    WHEN device_type = 'Mobile' THEN TRUE 
    ELSE FALSE 
  END AS is_mobile
  
FROM device_analysis