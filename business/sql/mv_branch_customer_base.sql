CREATE MATERIALIZED VIEW IF NOT EXISTS ads.mv_branch_customer_base
(
  customer_name COMMENT '客户姓名',
  customer_account COMMENT '资金账号',
  customer_phone COMMENT '手机号(来自dws)',
  sole_code COMMENT '订单编号',
  product_name COMMENT '产品名称',
  product_type COMMENT '原始产品类型',
  product_category COMMENT '产品归类(工具/投顾)',
  sign_type COMMENT '签约方式',
  pay_amount COMMENT '支付金额',
  pay_commission COMMENT '佣金',
  refund_amount COMMENT '退款金额',
  curr_total_asset COMMENT '总资产',
  sales_name COMMENT '销售人员',
  pay_time COMMENT '成交时间',
  pay_time_end COMMENT '支付结束时间',
  customer_layer COMMENT '客户分层'
)
COMMENT "分公司客户订单基础宽表(日更)"
DISTRIBUTED BY RANDOM
REFRESH ASYNC START("2026-03-20 15:00:00") EVERY(INTERVAL 1 DAY)
AS
WITH max_day_ads AS (
  SELECT MAX(day) AS max_day
  FROM ads.a_tg_branch_customer_detail
),
max_day_dws AS (
  SELECT MAX(day) AS max_day
  FROM dws.s_tg_product_order_df
),
phone_src AS (
  SELECT sole_code, customer_phone
  FROM (
    SELECT
      sole_code,
      customer_phone,
      ROW_NUMBER() OVER (PARTITION BY sole_code ORDER BY sole_code) AS rn
    FROM dws.s_tg_product_order_df
    WHERE day = (SELECT max_day FROM max_day_dws)
  ) t
  WHERE rn = 1
)
SELECT
  d.customer_name,
  d.customer_account,
  p.customer_phone,
  d.sole_code,
  d.product_name,
  d.product_type,
  CASE WHEN d.product_type = '增值产品' THEN '工具' ELSE '投顾' END AS product_category,
  v.order_category AS sign_type,
  d.order_amount AS pay_amount,
  d.order_commission AS pay_commission,
  d.refund_amount,
  d.curr_total_asset,
  d.customer_manager AS sales_name,
  d.pay_time,
  d.pay_time_end,
  v.customer_layer
FROM ads.a_tg_branch_customer_detail d
LEFT JOIN phone_src p
  ON d.sole_code = p.sole_code
LEFT JOIN mv_tg_user_orderlayer v
  ON d.customer_account = v.customer_account
 AND d.sole_code = v.sole_code
 AND d.product_name = v.product_name
WHERE d.day = (SELECT max_day FROM max_day_ads);