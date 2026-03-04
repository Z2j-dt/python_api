-- 个股日收盘价表（供净值计算使用）
-- 需由 ETL 或 iFinD 脚本每日同步写入，格式与 config_stock_position 中 stock_code 一致（如 002792、688270）
CREATE TABLE IF NOT EXISTS ods_stock_daily_close (
    biz_date     DATE            NOT NULL COMMENT '交易日',
    stock_code   VARCHAR(32)     NOT NULL COMMENT '股票代码',
    close_price  DECIMAL(12,2)   NOT NULL COMMENT '收盘价',
    PRIMARY KEY(biz_date, stock_code)
)
DISTRIBUTED BY HASH(stock_code) BUCKETS 4
PROPERTIES ("replication_num" = "1");
