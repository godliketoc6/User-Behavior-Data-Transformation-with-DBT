{{ config(
    materialized = 'view',
    tags = ['dim', 'currency'],
    description = 'Dimension table for standardized currency symbols, codes, names, exchange rates, and countries'
) }}

WITH currency_map AS (
  SELECT 'CAD $' AS raw_value, 'CAD' AS code, 'Canadian Dollar' AS currency_name, '$' AS symbol, 0.74 AS exchange_rate, 'CA' AS country_code UNION ALL
  SELECT 'лв', 'BGN', 'Bulgarian Lev', 'лв', 0.56, 'BG' UNION ALL
  SELECT '加币$', 'CAD', 'Canadian Dollar', '$', 0.74, 'CA' UNION ALL
  SELECT '$US', 'USD', 'US Dollar', '$', 1.0, 'US' UNION ALL
  SELECT '£', 'GBP', 'British Pound', '£', 0.78, 'GB' UNION ALL
  SELECT 'R$', 'BRL', 'Brazilian Real', 'R$', 0.19, 'BR' UNION ALL
  SELECT 'CHF', 'CHF', 'Swiss Franc', 'CHF', 1.11, 'CH' UNION ALL
  SELECT 'AZN', 'AZN', 'Azerbaijani Manat', '₼', 0.59, 'AZ' UNION ALL
  SELECT 'HNL L', 'HNL', 'Honduran Lempira', 'L', 0.041, 'HN' UNION ALL
  SELECT 'DOP $', 'DOP', 'Dominican Peso', 'RD$', 0.017, 'DO' UNION ALL
  SELECT 'EUR', 'EUR', 'Euro', '€', 0.91, 'EU' UNION ALL
  SELECT 'KWD', 'KWD', 'Kuwaiti Dinar', 'KD', 3.26, 'KW' UNION ALL
  SELECT 'швейцарских франка', 'CHF', 'Swiss Franc', 'CHF', 1.11, 'CH' UNION ALL
  SELECT 'Ft', 'HUF', 'Hungarian Forint', 'Ft', 0.0028, 'HU' UNION ALL
  SELECT 'MXN $', 'MXN', 'Mexican Peso', '$', 0.058, 'MX' UNION ALL
  SELECT '₹', 'INR', 'Indian Rupee', '₹', 0.012, 'IN' UNION ALL
  SELECT 'RM', 'MYR', 'Malaysian Ringgit', 'RM', 0.21, 'MY' UNION ALL
  SELECT 'يورو', 'EUR', 'Euro', '€', 0.91, 'AE' UNION ALL
  SELECT 'евро', 'EUR', 'Euro', '€', 0.91, 'RU' UNION ALL
  SELECT '港币$', 'HKD', 'Hong Kong Dollar', '$', 0.13, 'HK' UNION ALL
  SELECT 'USD', 'USD', 'US Dollar', '$', 1.0, 'US' UNION ALL
  SELECT 'шведских крон', 'SEK', 'Swedish Krona', 'kr', 0.093, 'SE' UNION ALL
  SELECT 'Ucretsiz', 'TRY', 'Turkish Lira (Free)', '₺', 0.031, 'TR' UNION ALL
  SELECT 'RON', 'RON', 'Romanian Leu', 'lei', 0.21, 'RO' UNION ALL
  SELECT 'SEK', 'SEK', 'Swedish Krona', 'kr', 0.093, 'SE' UNION ALL
  SELECT 'зл', 'PLN', 'Polish Zloty', 'zł', 0.25, 'PL' UNION ALL
  SELECT '₫', 'VND', 'Vietnamese Dong', '₫', 0.000042, 'VN' UNION ALL
  SELECT '加元', 'CAD', 'Canadian Dollar', '$', 0.74, 'CA' UNION ALL
  SELECT 'kr', 'SEK', 'Swedish Krona', 'kr', 0.093, 'SE' UNION ALL
  SELECT 'CHF \'', 'CHF', 'Swiss Franc', 'CHF', 1.11, 'CH' UNION ALL
  SELECT 'USD $', 'USD', 'US Dollar', '$', 1.0, 'US' UNION ALL
  SELECT '¥', 'JPY', 'Japanese Yen', '¥', 0.0065, 'JP' UNION ALL
  SELECT 'злотых', 'PLN', 'Polish Zloty', 'zł', 0.25, 'PL' UNION ALL
  SELECT 'din', 'RSD', 'Serbian Dinar', 'дин.', 0.0091, 'RS' UNION ALL
  SELECT 'Lei', 'RON', 'Romanian Leu', 'lei', 0.21, 'RO' UNION ALL
  SELECT '₴', 'UAH', 'Ukrainian Hryvnia', '₴', 0.025, 'UA' UNION ALL
  SELECT 'AFN', 'AFN', 'Afghan Afghani', '؋', 0.012, 'AF' UNION ALL
  SELECT 'HUF', 'HUF', 'Hungarian Forint', 'Ft', 0.0028, 'HU' UNION ALL
  SELECT 'ZAR', 'ZAR', 'South African Rand', 'R', 0.053, 'ZA' UNION ALL
  SELECT 'AED', 'AED', 'UAE Dirham', 'د.إ', 0.27, 'AE' UNION ALL
  SELECT 'US $', 'USD', 'US Dollar', '$', 1.0, 'US' UNION ALL
  SELECT 'د ك', 'KWD', 'Kuwaiti Dinar', 'KD', 3.26, 'KW' UNION ALL
  SELECT 'PEN S', 'PEN', 'Peruvian Sol', 'S/', 0.27, 'PE' UNION ALL
  SELECT 'zł', 'PLN', 'Polish Zloty', 'zł', 0.25, 'PL' UNION ALL
  SELECT 'GTQ Q', 'GTQ', 'Guatemalan Quetzal', 'Q', 0.13, 'GT' UNION ALL
  SELECT 'Lekë', 'ALL', 'Albanian Lek', 'L', 0.011, 'AL' UNION ALL
  SELECT '، درهم', 'AED', 'UAE Dirham', 'د.إ', 0.27, 'AE' UNION ALL
  SELECT '€', 'EUR', 'Euro', '€', 0.91, 'EU' UNION ALL
  SELECT 'NZD $', 'NZD', 'New Zealand Dollar', '$', 0.61, 'NZ'
),

raw_currency AS (
  SELECT DISTINCT
    cp.currency AS raw_value
  FROM `unigap-project-461012.Glamira.glamira_summary`,
  UNNEST(cart_products) AS cp
  WHERE cp.currency IS NOT NULL
)

-- Final output: ONLY matched currencies
SELECT
  cm.symbol,
  cm.code,
  cm.currency_name,
  cm.exchange_rate,
  cm.country_code
FROM raw_currency rc
JOIN currency_map cm
  ON rc.raw_value = cm.raw_value
WHERE cm.code IS NOT NULL
