CREATE MATERIALIZED VIEW IF NOT EXISTS mv_branch_customer_detail_final
(
  customer_name COMMENT '客户姓名',
  customer_account COMMENT '资金账号',
  customer_phone COMMENT '手机号',
  sole_code COMMENT '订单编号',
  product_name COMMENT '产品名称',
  product_type COMMENT '产品类型',
  product_category COMMENT '产品归类',
  sign_type COMMENT '签约方式',
  pay_amount COMMENT '支付金额',
  pay_commission COMMENT '佣金',
  refund_amount COMMENT '退款金额',
  curr_total_asset COMMENT '总资产',
  pay_time COMMENT '成交时间',
  pay_time_end COMMENT '支付结束时间',
  customer_layer COMMENT '客户分层',
  in_month COMMENT '进线月份',
  channel COMMENT '渠道',
  sales_owner COMMENT '销售归属',
  wechat_nick COMMENT '微信昵称'
)
COMMENT "分公司客户订单最终宽表(日更15点)"
DISTRIBUTED BY RANDOM
REFRESH ASYNC START("2026-03-20 15:00:00") EVERY(INTERVAL 1 DAY)
AS
SELECT
  b.customer_name,
  b.customer_account,
  b.customer_phone,
  b.sole_code,
  b.product_name,
  b.product_type,
  b.product_category,
  b.sign_type,
  b.pay_amount,
  b.pay_commission,
  b.refund_amount,
  b.curr_total_asset,
  b.pay_time,
  b.pay_time_end,
  b.customer_layer,
  c.in_month,
  c.channel,
  c.sales_name,
  c.wechat_nick
FROM mv_branch_customer_base b
LEFT JOIN branch_customer_ext_config c
  ON b.sole_code = c.sole_code
 AND b.customer_account = c.customer_account
 AND b.product_name = c.product_name;
