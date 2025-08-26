{{ config(
    materialized = 'table',
    tags = ['fact', 'orders'],
    description = 'Optimized central fact table for orders with foreign keys to all dimensions and key business metrics.'
) }}

-- ✅ Pre-filter and optimize base data
WITH base_orders AS (
  SELECT
    -- ✅ PRIMARY KEY
    FARM_FINGERPRINT( 
  COALESCE(
    CAST(order_id.int_value AS STRING),
    order_id.string_value,
    CAST(order_id.double_value AS STRING)
  )
) AS order_id,

    
    -- 🔑 Direct foreign keys (no complex joins)
    COALESCE(NULLIF(SAFE_CAST(user_id_db AS STRING), ''), 'XNA') AS user_id_db,
    SAFE_CAST(store_id AS INT64) AS store_id,
    FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64)))) AS date_id,
    
    -- 📊 Core metrics
    cp.product_id.int_value AS product_id,
    COALESCE(
      cp.price.double_value,
      SAFE_CAST(REPLACE(REGEXP_REPLACE(cp.price.string_value, r'[^\d,.]', ''), ',', '.') AS NUMERIC)
    ) AS order_amount,
    
    -- Payment info for FK lookup
    CASE
      WHEN is_paypal.boolean_value IS NOT NULL THEN is_paypal.boolean_value
      WHEN LOWER(NULLIF(is_paypal.string_value, '')) = 'true' THEN TRUE
      ELSE FALSE
    END AS is_paypal,
    COALESCE(NULLIF(cp.currency, ''), 'XNA') AS currency,
    
    -- 🧩 Option details (simplified)
    opt.option_id.int_value AS option_id,
    opt.value_id.int_value AS value_id,
    COALESCE(NULLIF(opt.option_label, ''), 'XNA') AS option_label,
    COALESCE(NULLIF(opt.value_label, ''), 'XNA') AS value_label,
    
    -- Timestamp
    TIMESTAMP_SECONDS(SAFE_CAST(time_stamp.long_value AS INT64)) AS order_timestamp,
    
    -- Store IP for location lookup (avoid nested subqueries)
    ip
    
  FROM `unigap-project-461012.Glamira.glamira_summary` AS g
  CROSS JOIN UNNEST(g.cart_products) AS cp
  CROSS JOIN UNNEST(cp.option.array_CartOption_value) AS opt
  WHERE 
    -- ✅ Early filtering to reduce data volume
    (order_id.int_value IS NOT NULL OR order_id.string_value IS NOT NULL OR order_id.double_value IS NOT NULL)
    AND cp.product_id.int_value IS NOT NULL
    AND cp.price IS NOT NULL
    AND (cp.price.double_value > 0 OR SAFE_CAST(REPLACE(REGEXP_REPLACE(cp.price.string_value, r'[^\d,.]', ''), ',', '.') AS NUMERIC) > 0)
),

-- ✅ Simple payment method lookup (avoid window functions in main query)
payment_lookup AS (
  SELECT DISTINCT
    is_paypal,
    currency,
    DENSE_RANK() OVER (ORDER BY is_paypal, currency) AS payment_method_id
  FROM (
    SELECT DISTINCT is_paypal, currency FROM base_orders
    WHERE currency != 'XNA'
  )
),

-- ✅ Simple location lookup (pre-computed)
location_lookup AS (
  SELECT DISTINCT
    ip,
    DENSE_RANK() OVER (ORDER BY ip) AS location_id
  FROM `unigap-project-461012.Ip2location.Ip2location`
  WHERE ip IS NOT NULL
)

-- ✅ Final optimized output
SELECT
  -- ✅ PRIMARY KEY
  bo.order_id,
  
  -- 🆕 UNIQUE ID combining order_id and product_id
  FARM_FINGERPRINT(
    CONCAT(
      COALESCE(CAST(bo.order_id AS STRING), ''),
      '|',
      COALESCE(CAST(bo.product_id AS STRING), '')
    )
  ) AS id,
  
  -- 🔑 FOREIGN KEYS (direct assignments, minimal joins)
  bo.user_id_db,
  bo.store_id,
  ll.location_id,
  bo.date_id,
  bo.product_id,
  COALESCE(pl.payment_method_id, 999) AS payment_method_id, -- Default for unmatched
  
  -- 🎯 Simplified option foreign keys
  CASE 
    WHEN bo.option_label = 'diamond' THEN COALESCE(SAFE_CAST(bo.option_id AS STRING), 'XNA')
    ELSE 'XNA'
  END AS diamond_option_id, 
  
  CASE 
    WHEN bo.option_label = 'alloy' THEN COALESCE(SAFE_CAST(bo.option_id AS STRING), 'XNA')
    ELSE 'XNA'
  END AS alloy_option_id,
  
  -- 📊 MEASURES (simplified)
  bo.order_amount,
  bo.order_amount AS total_order_value,
  
  -- 🧩 DESCRIPTIVE ATTRIBUTES
  bo.option_label,
  bo.value_label,
  bo.order_timestamp,
  
  -- 📈 COUNT METRIC
  1 AS order_count

FROM base_orders bo
LEFT JOIN payment_lookup pl
  ON bo.is_paypal = pl.is_paypal AND bo.currency = pl.currency
LEFT JOIN location_lookup ll
  ON bo.ip = ll.ip

WHERE bo.order_id IS NOT NULL