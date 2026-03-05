CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_nav_compute
COMMENT '产品净值-从交易+价格+沪深300直接计算'
REFRESH ASYNC EVERY(INTERVAL 60 SECOND)
DISTRIBUTED BY HASH(product_name) BUCKETS 2
AS
WITH
-- 1. 交易拆解：每笔的股数、成本、收入（近似：卖出不 cap 于持仓）
trades_detail AS (
  SELECT
    product_name,
    trade_date,
    stock_code,
    side,
    price,
    position_pct,
    FLOOR((10000000 * position_pct / 100 / NULLIF(price, 0)) / 100) * 100 AS want_shares,
    CASE WHEN TRIM(side) = '买入' THEN 1 ELSE -1 END AS side_sign
  FROM config_stock_position
  WHERE product_name IS NOT NULL AND trade_date IS NOT NULL
    AND stock_code IS NOT NULL AND side IN ('买入', '卖出')
    AND price > 0 AND position_pct IS NOT NULL
),
trades_with_amounts AS (
  SELECT
    product_name,
    trade_date,
    stock_code,
    want_shares,
    side_sign,
    want_shares * price AS cost_or_revenue
  FROM trades_detail
),
-- 2. 日期维度：交易日期 + 价格表日期
all_dates AS (
  SELECT DISTINCT trade_date AS biz_date FROM config_stock_position WHERE trade_date IS NOT NULL
  UNION
  SELECT DISTINCT
    CASE
      WHEN CAST(biz_date AS SIGNED) BETWEEN 19000101 AND 99991231
      THEN STR_TO_DATE(CAST(biz_date AS CHAR), '%Y%m%d')
      ELSE biz_date
    END AS biz_date
  FROM portal_db.hsgt_price_deliver
  WHERE biz_date IS NOT NULL
),
-- 3. 每日每股持仓（截至该日）
position_by_date AS (
  SELECT
    t.product_name,
    d.biz_date,
    t.stock_code,
    SUM(t.want_shares * t.side_sign) AS position_shares
  FROM all_dates d
  JOIN trades_with_amounts t ON t.trade_date <= d.biz_date
  GROUP BY t.product_name, d.biz_date, t.stock_code
  HAVING position_shares > 0
),
-- 4. 每日现金
cash_by_date AS (
  SELECT
    p.product_name,
    d.biz_date,
    10000000
      - COALESCE(SUM(CASE WHEN t.side_sign = 1 THEN t.cost_or_revenue ELSE 0 END), 0)
      + COALESCE(SUM(CASE WHEN t.side_sign = -1 THEN t.cost_or_revenue ELSE 0 END), 0) AS cash
  FROM (SELECT DISTINCT product_name FROM config_stock_position WHERE product_name IS NOT NULL AND trade_date IS NOT NULL) p
  CROSS JOIN all_dates d
  LEFT JOIN trades_with_amounts t ON t.product_name = p.product_name AND t.trade_date <= d.biz_date
  GROUP BY p.product_name, d.biz_date
),
-- 5. 价格表：biz_date 支持 yyyymmdd 或 date
price_normalized AS (
  SELECT
    CASE
      WHEN CAST(biz_date AS SIGNED) BETWEEN 19000101 AND 99991231
      THEN STR_TO_DATE(CAST(biz_date AS CHAR), '%Y%m%d')
      ELSE biz_date
    END AS biz_date,
    stock_code,
    last_price
  FROM portal_db.hsgt_price_deliver
  WHERE biz_date IS NOT NULL AND last_price IS NOT NULL
),
-- 6. 市值（持仓 * 收盘价；股票代码需与 hsgt_price_deliver 对齐，必要时做 SZ/SH 映射）
market_value_by_date AS (
  SELECT
    pos.product_name,
    pos.biz_date,
    SUM(pos.position_shares * COALESCE(pr.last_price, 0)) AS market_value
  FROM position_by_date pos
  LEFT JOIN price_normalized pr
    ON pr.biz_date = pos.biz_date
   AND pr.stock_code = pos.stock_code
  GROUP BY pos.product_name, pos.biz_date
),
-- 7. 沪深300 基期与每日净值（stock_code=000300，基期=最早日期）
hs300_price AS (
  SELECT
    CASE
      WHEN CAST(biz_date AS SIGNED) BETWEEN 19000101 AND 99991231
      THEN STR_TO_DATE(CAST(biz_date AS CHAR), '%Y%m%d')
      ELSE biz_date
    END AS biz_date,
    last_price
  FROM portal_db.hsgt_price_deliver
  WHERE biz_date IS NOT NULL
    AND last_price IS NOT NULL
    AND stock_code = '000300'
),
hs300_base AS (
  SELECT last_price AS base_price
  FROM hs300_price
  WHERE biz_date = (SELECT MIN(biz_date) FROM hs300_price)
  LIMIT 1
),
hs300_nav_by_date AS (
  SELECT
    h.biz_date,
    h.last_price / NULLIF((SELECT base_price FROM hs300_base), 0) AS hs300_nav
  FROM hs300_price h
)
-- 8. 最终：组合净值 + 沪深300净值
SELECT
  prod.product_name,
  d.biz_date,
  CAST(
    (COALESCE(c.cash, 10000000) + COALESCE(mv.market_value, 0)) / 10000000
    AS DECIMAL(20,6)
  ) AS nav,
  CAST(h.hs300_nav AS DECIMAL(20,6)) AS hs300_nav,
  NOW() AS updated_at
FROM all_dates d
CROSS JOIN (SELECT DISTINCT product_name FROM config_stock_position WHERE product_name IS NOT NULL AND trade_date IS NOT NULL) prod
LEFT JOIN cash_by_date c ON c.product_name = prod.product_name AND c.biz_date = d.biz_date
LEFT JOIN market_value_by_date mv ON mv.product_name = prod.product_name AND mv.biz_date = d.biz_date
LEFT JOIN hs300_nav_by_date h ON h.biz_date = d.biz_date
WHERE d.biz_date >= (SELECT MIN(trade_date) FROM config_stock_position WHERE trade_date IS NOT NULL)
  AND d.biz_date <= (SELECT MAX(trade_date) FROM config_stock_position WHERE trade_date IS NOT NULL)
order by d.biz_date;