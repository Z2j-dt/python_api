CREATE TABLE IF NOT EXISTS branch_customer_ext_config (
  sole_code        STRING NOT NULL COMMENT '订单编号',
  customer_account STRING NOT NULL COMMENT '资金账号',
  product_name     STRING NOT NULL COMMENT '产品名称',
  pay_time         STRING COMMENT '成交时间',
  in_month         STRING COMMENT '进线月份',
  channel          STRING COMMENT '渠道',
  wechat_nick      STRING COMMENT '微信昵称',
  sales_owner   STRING COMMENT '销售归属',

  created_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
)
PRIMARY KEY (sole_code, customer_account, product_name)
DISTRIBUTED BY HASH(sole_code) BUCKETS 8
PROPERTIES (
  "replication_num" = "3"
);



INSERT INTO branch_customer_ext_config (sole_code, customer_account, product_name, pay_time,sales_owner)
SELECT DISTINCT
  sole_code, customer_account, product_name, pay_time, sales_name
FROM mv_branch_customer_base;





INSERT INTO branch_customer_ext_config (sole_code, customer_account, product_name, pay_time, sales_owner)
SELECT
  t.sole_code, t.customer_account, t.product_name, pay_time, sales_name
FROM (
  SELECT DISTINCT
    sole_code, customer_account, product_name, pay_time, sales_name
  FROM mv_branch_customer_base
) t
LEFT JOIN cfg_branch_customer_ext c
  ON t.sole_code = c.sole_code
 AND t.customer_account = c.customer_account
 AND t.product_name = c.product_name
WHERE c.sole_code IS NULL;