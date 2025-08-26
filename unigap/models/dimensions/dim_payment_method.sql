{{ config(
    materialized = 'view',
    unique_key = 'payment_method_id',
    tags = ['dimension', 'payment'],
    description = 'Dimension table for unique combinations of payment methods and currencies.'
) }}

-- Dimension table for payment methods
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY is_paypal, currency) AS payment_method_id,

  CASE 
    WHEN is_paypal THEN 'PayPal'
    ELSE 'Credit Card'
  END AS payment_method_name,

  is_paypal,

  -- Replace NULL or empty strings with 'XNA'
  COALESCE(NULLIF(currency, ''), 'XNA') AS currency

FROM (
  SELECT DISTINCT
    -- Normalize is_paypal to a boolean with fallback
    CASE
      WHEN is_paypal.boolean_value IS NOT NULL THEN is_paypal.boolean_value
      WHEN LOWER(NULLIF(is_paypal.string_value, '')) = 'true' THEN TRUE
      ELSE FALSE
    END AS is_paypal,

    cp.currency AS currency

  FROM `unigap-project-461012.Glamira.glamira_summary` AS g
  LEFT JOIN UNNEST(g.cart_products) AS cp
  WHERE cp.currency IS NOT NULL
) AS payment_methods
