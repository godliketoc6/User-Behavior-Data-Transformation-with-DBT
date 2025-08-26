{{ config(
    materialized = 'view',
    tags = ['fact', 'receipt'],
    description = 'Fact table storing order receipt info including URLs, user agent, price, and linked payment method.'
) }}

WITH raw_data AS (
  SELECT *
  FROM `unigap-project-461012.Glamira.glamira_summary`
  WHERE order_id IS NOT NULL
),

receipt_base AS (
  SELECT DISTINCT
    -- ✅ PRIMARY KEY
    FARM_FINGERPRINT(
  COALESCE(
    CAST(order_id.int_value AS STRING),
    order_id.string_value,
    CAST(order_id.double_value AS STRING)
  )
) AS order_id,


    -- 🧾 ATTRIBUTES
    current_url,
    referrer_url,
    SAFE_CAST(store_id AS INT64) AS store_id,
    SAFE_CAST(local_time AS TIMESTAMP) AS local_time,
    user_agent,

    -- ✅ Normalized price
    COALESCE(
      cp.price.double_value,
      SAFE_CAST(
        REPLACE(REGEXP_REPLACE(cp.price.string_value, r'[^\d,]', ''), ',', '.') AS NUMERIC
      )
    ) AS price,

    -- ✅ Join keys (not part of final output)
    CASE
      WHEN is_paypal.boolean_value IS NOT NULL THEN is_paypal.boolean_value
      WHEN LOWER(is_paypal.string_value) = 'true' THEN TRUE
      ELSE FALSE
    END AS is_paypal,
    cp.currency AS currency

  FROM raw_data AS g
  LEFT JOIN UNNEST(g.cart_products) AS cp
  WHERE cp.price IS NOT NULL
),

payment_methods AS (
  SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY is_paypal, currency) AS payment_method_id,
    is_paypal,
    currency
  FROM (
    SELECT DISTINCT
      CASE
        WHEN is_paypal.boolean_value IS NOT NULL THEN is_paypal.boolean_value
        WHEN LOWER(is_paypal.string_value) = 'true' THEN TRUE
        ELSE FALSE
      END AS is_paypal,
      cp.currency AS currency
    FROM raw_data AS g
    LEFT JOIN UNNEST(g.cart_products) AS cp
    WHERE cp.currency IS NOT NULL
  )
)

-- ✅ Final Receipt Table with FK
SELECT
  r.order_id,
  r.current_url,
  r.referrer_url,
  r.store_id,
  r.local_time,
  r.user_agent,
  r.price,
  pm.payment_method_id
FROM receipt_base r
LEFT JOIN payment_methods pm
  ON r.is_paypal = pm.is_paypal AND r.currency = pm.currency
WHERE r.order_id IS NOT NULL
