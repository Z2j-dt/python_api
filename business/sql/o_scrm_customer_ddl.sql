-- StarRocks 建表语句: o_scrm_customer
-- 上游: MySQL enterprise_weixin.customer
-- 用途: DataX 同步 ODS 层（create_time 为当日数据）

CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

CREATE TABLE IF NOT EXISTS o_scrm_customer (
    id                      BIGINT,
    name                    VARCHAR(255),
    external_id             VARCHAR(255),
    avatar                  VARCHAR(500),
    gender                  VARCHAR(32),
    union_id                VARCHAR(255),
    mobile                  VARCHAR(64),
    address                 VARCHAR(500),
    customer_code           VARCHAR(255),
    id_code                 VARCHAR(64),
    remark                  VARCHAR(500),
    birth_date              VARCHAR(32),
    create_time             VARCHAR(64),
    update_time             VARCHAR(64),
    real_name               VARCHAR(255),
    state                   VARCHAR(64),
    kyc                     VARCHAR(64),
    `type`                  VARCHAR(64),
    user_table_id           VARCHAR(255),
    add_time                VARCHAR(64),
    del_time                VARCHAR(64),
    add_time_before         VARCHAR(64),
    del_time_before         VARCHAR(64),
    avatar_fast_url         VARCHAR(500),
    account_no              VARCHAR(255)
)
DUPLICATE KEY(id)
COMMENT 'SCRM 客户表-ODS'
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
