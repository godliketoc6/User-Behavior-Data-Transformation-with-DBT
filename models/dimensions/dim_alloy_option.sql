{{ config(
    materialized = 'view',
    tags = ['dimension', 'diamond'],
    unique_key = 'option_id'
) }}

-- ✅ dim_diamond: Dedicated dimension for diamond options
SELECT DISTINCT
  CAST(opt.option_id.int_value AS INT64) AS option_id,
  CAST(opt.value_id.int_value AS INT64) AS value_id,
  opt.value_label
FROM `unigap-project-461012.Glamira.glamira_summary` AS g
CROSS JOIN UNNEST(g.cart_products) AS cp
CROSS JOIN UNNEST(cp.option.array_CartOption_value) AS opt
WHERE opt.option_label = 'alloy'
