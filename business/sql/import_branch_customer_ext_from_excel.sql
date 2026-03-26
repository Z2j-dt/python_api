-- Excel 手工配置导入流程（StarRocks）
-- 目标：将 Excel 中的“进线月份/渠道/销售归属/微信昵称”写入 branch_customer_ext_config
-- 说明：Excel 当前不含 sole_code，需先用 mv_branch_customer_base 关联补齐 sole_code

-- 0) 建议先把 Excel 转为 UTF-8 CSV，字段顺序如下：
-- customer_name,customer_account,product_name,in_month,channel,sales_owner,wechat_nick
-- 然后通过 Stream Load 导入到 stg_branch_customer_ext_excel

CREATE TABLE IF NOT EXISTS ads.stg_branch_customer_ext_excel (
  customer_name    STRING COMMENT '客户姓名',
  customer_account STRING COMMENT '资金账号',
  product_name     STRING COMMENT '产品名称',
  in_month         STRING COMMENT '进线月份',
  channel          STRING COMMENT '渠道',
  sales_owner      STRING COMMENT '销售归属',
  wechat_nick      STRING COMMENT '微信昵称'
)
DUPLICATE KEY(customer_account, product_name)
DISTRIBUTED BY HASH(customer_account) BUCKETS 8
PROPERTIES (
  "replication_num" = "3"
);

-- 1) 清理临时表（每次导入前执行）
TRUNCATE TABLE ads.stg_branch_customer_ext_excel;

-- 2) 将 CSV 写入 ads.stg_branch_customer_ext_excel：
--    由于 Stream Load 之前出现行数异常，这里改为直接执行离线生成的 INSERT 脚本
--    脚本路径：e:\python_api\business\sql\insert_stg_branch_customer_ext_excel.sql

-- 3) 你的 StarRocks 版本不支持 MERGE，这里改用 INSERT INTO 实现 UPSERT
--    对 PRIMARY KEY 表（branch_customer_ext_config）来说，同主键会自动更新
INSERT INTO ads.branch_customer_ext_config
(
  sole_code,
  customer_account,
  product_name,
  in_month,
  channel,
  sales_owner,
  wechat_nick,
  created_time,
  updated_time
)
SELECT
  src.sole_code,
  src.customer_account,
  src.product_name,
  src.in_month,
  src.channel,
  src.sales_owner,
  src.wechat_nick,
  COALESCE(t.created_time, CURRENT_TIMESTAMP) AS created_time,
  CURRENT_TIMESTAMP AS updated_time
FROM (
  SELECT DISTINCT
    b.sole_code,
    TRIM(s.customer_account) AS customer_account,
    TRIM(s.product_name) AS product_name,
    s.in_month,
    s.channel,
    s.sales_owner,
    s.wechat_nick
  FROM ads.stg_branch_customer_ext_excel s
  INNER JOIN ads.mv_branch_customer_base b
    ON TRIM(s.customer_account) = TRIM(b.customer_account)
   AND TRIM(s.product_name) = TRIM(b.product_name)
) src
LEFT JOIN ads.branch_customer_ext_config t
  ON t.sole_code = src.sole_code
 AND t.customer_account = src.customer_account
 AND t.product_name = src.product_name;

-- 4) 检查未匹配到 sole_code 的记录（用于排查 Excel 数据质量）
SELECT
  s.customer_name,
  s.customer_account,
  s.product_name,
  s.in_month,
  s.channel,
  s.sales_owner,
  s.wechat_nick
FROM ads.stg_branch_customer_ext_excel s
LEFT JOIN ads.mv_branch_customer_base b
  ON s.customer_account = b.customer_account
 AND s.product_name = b.product_name
WHERE b.sole_code IS NULL;
