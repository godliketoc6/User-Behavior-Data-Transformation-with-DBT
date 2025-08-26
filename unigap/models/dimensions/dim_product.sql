{{ config(
    materialized = 'view',
    unique_key = 'product_id',
    tags = ['dimension', 'product'],
    description = 'Product dimension including cleaned price, product name, and quality metadata.'
) }}

-- Step 1: Extract quality and quality_label from global option array
WITH quality_data AS (
  SELECT
    quality,
    quality_label
  FROM
    `unigap-project-461012.Glamira.glamira_summary`,
    UNNEST(option.array_Option_value) AS opt
  WHERE
    opt.quality IS NOT NULL OR opt.quality_label IS NOT NULL
  LIMIT 1
),

-- Step 2: Main dim_product logic with price cleaned
dim_product AS (
  SELECT
    -- ✅ PRIMARY KEY
    FARM_FINGERPRINT(
  CAST(cp.product_id.int_value AS STRING)
) AS product_id,


    -- 🔑 FOREIGN KEYSa
    global_opt.category_id,

    -- 🧩 ATTRIBUTES
    -- Clean price formats
    CAST(REGEXP_REPLACE(cp.price.string_value, r'[^0-9,]', '') AS STRING) AS raw_price,
    CAST(
      REPLACE(REGEXP_REPLACE(cp.price.string_value, r'[^\d,]', ''), ',', '.') AS NUMERIC
    ) AS price
  FROM
    `unigap-project-461012.Glamira.glamira_summary` AS g
  CROSS JOIN UNNEST(g.cart_products) AS cp
  CROSS JOIN UNNEST(cp.option.array_CartOption_value) AS cp_option
  LEFT JOIN UNNEST(g.option.array_Option_value) AS global_opt
    ON global_opt.option_id = CAST(cp_option.option_id.int_value AS STRING)
    AND global_opt.value_id = CAST(cp_option.value_id.int_value AS STRING)
  WHERE
    cp.product_id.int_value IS NOT NULL
    AND cp.price.string_value IS NOT NULL
)

-- Step 3: Final output with product name and quality info
SELECT
  -- ✅ PRIMARY KEY
  dp.product_id,

  -- 🧩 ATTRIBUTES
  dp.price,

  -- 🧩 Join product name
  pu.name AS product_name,

  -- 🧩 Append global quality info
  qd.quality,
  qd.quality_label

FROM dim_product dp
LEFT JOIN `unigap-project-461012.ProductName.prod_urls` pu
  ON CAST(dp.product_id AS STRING) = pu.product_id
LEFT JOIN quality_data qd
  ON TRUE
