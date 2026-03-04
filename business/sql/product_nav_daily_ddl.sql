-- 产品净值日结果表（计算口径与 temp3 / app._compute_nav_series 一致）
-- 由定时任务（如每 60 秒）根据 config_stock_position + hsgt_price_deliver 算好后写入
-- 接口优先查此表，无数据时再回退到实时计算

CREATE TABLE IF NOT EXISTS product_nav_daily (
    product_name   VARCHAR(64)     NOT NULL COMMENT '产品名称',
    biz_date       DATE            NOT NULL COMMENT '交易日',
    nav            DECIMAL(20,6)   NOT NULL COMMENT '组合净值',
    hs300_nav      DECIMAL(20,6)   NULL     COMMENT '沪深300净值（可选）',
    updated_at     DATETIME        NULL     COMMENT '更新时间',
    PRIMARY KEY(product_name, biz_date)
)
COMMENT '产品净值日表-供净值图接口快速查询'
DISTRIBUTED BY HASH(product_name) BUCKETS 2
PROPERTIES ("replication_num" = "1");
