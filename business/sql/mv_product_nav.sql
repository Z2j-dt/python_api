-- 产品净值物化视图：从结果表 product_nav_daily 同步，每 60 秒刷新
-- 计算口径本身在 product_nav_daily 表由外部任务写入，此处仅做“视图 + 定期刷新”便于查询

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_nav
COMMENT '产品净值-从 product_nav_daily 同步，每60s刷新'
REFRESH ASYNC EVERY(INTERVAL 60 SECOND)
AS
SELECT
    product_name,
    biz_date,
    nav,
    hs300_nav,
    updated_at
FROM product_nav_daily;
