CREATE MATERIALIZED VIEW `mv_tag_customer_permission` (`tag_name`, `customer_accounts`)
COMMENT "客户标签"
DISTRIBUTED BY RANDOM
REFRESH ASYNC START("2026-03-18 06:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
  "replicated_storage" = "true",
  "replication_num" = "3",
  "session.group_concat_max_len" = "1048576",
  "storage_medium" = "HDD"
)
AS
WITH base AS (
  SELECT DISTINCT customer_account
  FROM hive_catalog.ods.o_scrm_customer_account
  WHERE day = (
    SELECT MAX(day)
    FROM hive_catalog.ods.o_scrm_customer_account
  )
),
tp AS (
  SELECT
    b.customer_account,
    t.*
  FROM base b
  LEFT JOIN etl_db.tag_customer_profit t
    ON b.customer_account = t.client_id
),
trade_days AS (
  SELECT
    DATE_FORMAT(tradedate, 'yyyyMMdd') AS trade_day,
    ROW_NUMBER() OVER (ORDER BY tradedate DESC) AS rn
  FROM hive_catalog.ods.o_sd_thk_fxckhdata_t_pub_tradedate
  WHERE day = (
      SELECT MAX(day)
      FROM hive_catalog.ods.o_sd_thk_fxckhdata_t_pub_tradedate
    )
    AND iftradingday = '1'
    AND DATE(tradedate) < DATE((
      SELECT MAX(day)
      FROM hive_catalog.ods.o_sd_thk_fxckhdata_t_pub_tradedate
    ))
),
target_days AS (
  SELECT
    MAX(CASE WHEN rn = 1 THEN trade_day END) AS yday,
    MAX(CASE WHEN rn = 2 THEN trade_day END) AS pday,
    MAX(CASE WHEN rn = 3 THEN trade_day END) AS t3day
  FROM trade_days
),
tags AS (
  SELECT customer_account, '创业板(已开通)'   AS tag_name FROM tp WHERE is_gem_board = 1
  UNION ALL SELECT customer_account, '创业板(适格)'   FROM tp WHERE is_gem_qualified = 1
  UNION ALL SELECT customer_account, '创业板(潜客)'   FROM tp WHERE is_gem_potential = 1

  UNION ALL SELECT customer_account, '科创板(已开通)' FROM tp WHERE is_star_market = 1
  UNION ALL SELECT customer_account, '科创板(适格)'   FROM tp WHERE is_star_qualified = 1
  UNION ALL SELECT customer_account, '科创板(潜客)'   FROM tp WHERE is_star_potential = 1

  UNION ALL SELECT customer_account, '北交所(已开通)' FROM tp WHERE is_bse = 1
  UNION ALL SELECT customer_account, '北交所(适格)'   FROM tp WHERE is_bse_qualified = 1
  UNION ALL SELECT customer_account, '北交所(潜客)'   FROM tp WHERE is_bse_potential = 1

  UNION ALL SELECT customer_account, '新三板(已开通)' FROM tp WHERE is_neeq = 1
  UNION ALL SELECT customer_account, '新三板(适格)'   FROM tp WHERE is_neeq_qualified = 1
  UNION ALL SELECT customer_account, '新三板(潜客)'   FROM tp WHERE is_neeq_potential = 1

  UNION ALL SELECT customer_account, '沪港通(已开通)' FROM tp WHERE is_shhk = 1
  UNION ALL SELECT customer_account, '深港通(已开通)' FROM tp WHERE is_szhk = 1
  UNION ALL SELECT customer_account, '沪深港通(适格)' FROM tp WHERE is_hshk_qualified = 1
  UNION ALL SELECT customer_account, '沪深港通(潜客)' FROM tp WHERE is_hshk_potential = 1

  UNION ALL SELECT customer_account, '融资融券(已开通)' FROM tp WHERE is_rzrq = 1
  UNION ALL SELECT customer_account, '融资融券(适格)'   FROM tp WHERE is_rzrq_qualified = 1
  UNION ALL SELECT customer_account, '融资融券(潜客)'   FROM tp WHERE is_rzrq_potential = 1

  UNION ALL SELECT customer_account, '股票期权(已开通)' FROM tp WHERE is_option = 1
  UNION ALL SELECT customer_account, '股票期权(适格)'   FROM tp WHERE is_option_qualified = 1
  UNION ALL SELECT customer_account, '股票期权(潜客)'   FROM tp WHERE is_option_potential = 1

  UNION ALL SELECT customer_account, '可转债(已开通)'   FROM tp WHERE is_cb = 1
  UNION ALL SELECT customer_account, '可转债(适格)'     FROM tp WHERE is_cb_qualified = 1
  UNION ALL SELECT customer_account, '可转债(潜客)'     FROM tp WHERE is_cb_potential = 1

  UNION ALL
  SELECT customer_account, 'T0带入金实验组202603'
  FROM tp
  WHERE is_conform_xkcrj_202603 = '1'
    AND MOD(CRC32(CAST(customer_account AS VARCHAR)), 2) = 0

  UNION ALL
  SELECT customer_account, 'T0带入金对照组202603'
  FROM tp
  WHERE is_conform_xkcrj_202603 = '1'
    AND MOD(CRC32(CAST(customer_account AS VARCHAR)), 2) = 1

  UNION ALL SELECT customer_account, '空户_2026' FROM tp WHERE is_empty_2026 = 1
  UNION ALL SELECT customer_account, '已购短线王' FROM tp WHERE is_purchased_dxw = 1
  UNION ALL SELECT customer_account, '已购同赢先锋' FROM tp WHERE is_purchased_tyxf = 1
  UNION ALL SELECT customer_account, '已购同赢先锋1v1' FROM tp WHERE is_purchased_tyxf1v1 = 1
  UNION ALL
  SELECT tp.customer_account, '昨日开户未转账'
  FROM tp
  CROSS JOIN target_days d
  WHERE CAST(COALESCE(tp.cash_sum_banlance, 0) AS DECIMAL(38, 10)) = 0
    AND REGEXP_REPLACE(CAST(tp.open_date AS STRING), '[^0-9]', '') = d.yday
  UNION ALL
  SELECT tp.customer_account, '前日开户未转账'
  FROM tp
  CROSS JOIN target_days d
  WHERE CAST(COALESCE(tp.cash_sum_banlance, 0) AS DECIMAL(38, 10)) = 0
    AND REGEXP_REPLACE(CAST(tp.open_date AS STRING), '[^0-9]', '') = d.pday
  UNION ALL
  SELECT tp.customer_account, '三日前开户未转账'
  FROM tp
  CROSS JOIN target_days d
  WHERE CAST(COALESCE(tp.cash_sum_banlance, 0) AS DECIMAL(38, 10)) = 0
    AND REGEXP_REPLACE(CAST(tp.open_date AS STRING), '[^0-9]', '') = d.t3day
)
SELECT
  tag_name,
  GROUP_CONCAT(DISTINCT customer_account ORDER BY customer_account ASC SEPARATOR ',') AS customer_accounts
FROM tags
GROUP BY tag_name
ORDER BY tag_name ASC;