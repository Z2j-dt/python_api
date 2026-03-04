-- StarRocks 建表语句: 股票仓位/买卖配置表（业务配置）
-- 字段：日期、股票代码、个股、仓位、买入/卖出、成交价、产品名称
-- 使用与 config_open_channel_tag 等一致的库（请将 your_database 改为实际库名）

-- CREATE DATABASE IF NOT EXISTS your_database;
-- USE your_database;

CREATE TABLE IF NOT EXISTS config_stock_position (
    id              BIGINT          NOT NULL AUTO_INCREMENT COMMENT '主键自增',
    product_name    VARCHAR(64)     NULL     COMMENT '产品名称',
    trade_date      DATE            NULL     COMMENT '日期',
    stock_code      VARCHAR(32)     NULL     COMMENT '股票代码',
    stock_name      VARCHAR(128)    NULL     COMMENT '个股',
    position_pct    DECIMAL(10,2)   NULL     COMMENT '仓位(%)',
    side            VARCHAR(8)      NULL     COMMENT '买入/卖出',
    price           DECIMAL(12,2)   NULL     COMMENT '成交价',
    created_at      DATETIME        NULL     COMMENT '创建时间',
    updated_at      DATETIME        NULL     COMMENT '更新时间'
)
PRIMARY KEY(id)
COMMENT '业务配置-股票仓位/买卖记录表'
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
