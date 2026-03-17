-- StarRocks 建表语句: 早盘人气股战绩追踪配置表（客户中心）
-- 来源：早盘人气股.xlsx（时间、人气股、代码）
-- 使用与其它 config_* 表一致的库（请将 your_database 改为实际库名）
--
-- CREATE DATABASE IF NOT EXISTS your_database;
-- USE your_database;
--
CREATE TABLE IF NOT EXISTS config_morning_hot_stock_track (
    id          BIGINT        NOT NULL AUTO_INCREMENT COMMENT '主键自增',
    tg_name     VARCHAR(64)   NULL     COMMENT '投顾/老师名称',
    biz_date    DATE          NULL     COMMENT '日期(YYYY-MM-DD)',
    stock_name  VARCHAR(64)   NULL     COMMENT '人气股',
    stock_code  VARCHAR(16)   NULL     COMMENT '代码',
    remark      VARCHAR(512)  NULL     COMMENT '备注',
    created_at  DATETIME      NULL     COMMENT '创建时间',
    updated_at  DATETIME      NULL     COMMENT '更新时间'
)
PRIMARY KEY(id)
COMMENT '客户中心-早盘人气股战绩追踪配置表'
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

