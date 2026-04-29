-- StarRocks 建表语句: 商机线索配置表（客户中心）
-- 字段：业务大类、业务小类、线索名称、是否重要、是否启用、备注、表名
-- 使用与 config_open_channel_tag 等一致的库（请将 your_database 改为实际库名）
--
-- CREATE DATABASE IF NOT EXISTS your_database;
-- USE your_database;
--
CREATE TABLE IF NOT EXISTS config_opportunity_lead (
    id                 BIGINT         NOT NULL AUTO_INCREMENT COMMENT '主键自增',
    biz_category_big    VARCHAR(64)    NULL     COMMENT '业务大类',
    biz_category_small  VARCHAR(64)    NULL     COMMENT '业务小类',
    clue_name           VARCHAR(128)   NULL     COMMENT '线索名称',
    is_important        TINYINT        NULL     COMMENT '是否重要(0/1)',
    is_enabled          TINYINT        NULL     COMMENT '是否启用(0/1)',
    remark              VARCHAR(512)   NULL     COMMENT '备注',
    table_name          VARCHAR(128)   NULL     COMMENT '表名',
    created_at          DATETIME       NULL     COMMENT '创建时间',
    updated_at          DATETIME       NULL     COMMENT '更新时间'
)
PRIMARY KEY(id)
COMMENT '客户中心-商机线索配置表'
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

