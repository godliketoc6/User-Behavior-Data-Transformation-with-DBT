SELECT DISTINCT
  -- ✅ FARM_FINGERPRINT PK based on business key
  FARM_FINGERPRINT(
    CONCAT(
      COALESCE(CAST(user_id_db AS STRING), ''),
      '|',
      COALESCE(email_address, ''),
      '|',
      COALESCE(device_id, '')
    )
  ) AS customer_pk,
  -- Original business keys
  SAFE_CAST(user_id_db AS INT64) AS user_id_db,
  email_address,
  device_id,
  -- ✅ Normalize show_recommendation as BOOLEAN
  COALESCE(
    show_recommendation.boolean_value,
    SAFE_CAST(
      LOWER(show_recommendation.string_value) = 'true' AS BOOL
    )
  ) AS show_recommendation,
  recommendation.string_value AS recommendation_status,
  api_version,
  -- SCD Type 2 support
  CURRENT_TIMESTAMP() AS record_created_date,
  TIMESTAMP('2099-12-31') AS record_expired_date,
  TRUE AS is_current_record
FROM
  `unigap-project-461012.Glamira.glamira_summary`
WHERE
  user_id_db IS NOT NULL
  AND email_address IS NOT NULL
