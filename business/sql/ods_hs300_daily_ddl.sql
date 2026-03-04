-- 沪深300指数日收盘价表（供净值对比使用）
CREATE TABLE IF NOT EXISTS ods_hs300_daily (
    biz_date     DATE            NOT NULL COMMENT '交易日',
    close_price  DECIMAL(12,4)   NOT NULL COMMENT '收盘价',
    PRIMARY KEY(biz_date)
)
DISTRIBUTED BY HASH(biz_date) BUCKETS 1
PROPERTIES ("replication_num" = "1");
