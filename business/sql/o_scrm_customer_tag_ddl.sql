-- StarRocks 建表语句: o_scrm_customer_tag
-- 上游: MySQL enterprise_weixin.customer_tag
-- 用途: DataX 同步 ODS 层（create_time 为当日数据）

CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

CREATE TABLE IF NOT EXISTS o_scrm_customer_tag (
    id                      BIGINT,
    tag_id                  VARCHAR(255),
    customer_id             VARCHAR(255),
    user_id                 VARCHAR(255),
    external_id             VARCHAR(255),
    tag_name                VARCHAR(255),
    user_table_id           VARCHAR(255),
    tag_type                VARCHAR(64),
    expire_time             VARCHAR(64),
    update_time             VARCHAR(64),
    create_time             VARCHAR(64),
    channel_id              VARCHAR(255)
)
DUPLICATE KEY(id)
COMMENT 'SCRM 客户标签表-ODS'
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
