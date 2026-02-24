-- StarRocks 建表语句: o_scrm_customer_employee
-- 上游: MySQL enterprise_weixin.customer_employee
-- 用途: DataX 全量/增量同步 ODS 层

CREATE DATABASE IF NOT EXISTS ods;

USE ods;

CREATE TABLE IF NOT EXISTS o_scrm_customer_employee (
    id                      BIGINT,
    customer_id             VARCHAR(255),
    user_id                 VARCHAR(255),
    state                   VARCHAR(64),
    user_name               VARCHAR(255),
    user_table_id           VARCHAR(255),
    external_id             VARCHAR(255),
    remark                  VARCHAR(500),
    description             VARCHAR(500),
    remark_corp_name        VARCHAR(255),
    remark_mobiles          VARCHAR(500),
    add_way                 VARCHAR(64),
    add_time                VARCHAR(64),
    contact_type            VARCHAR(64),
    create_time             VARCHAR(64),
    relation_update_date    VARCHAR(32),
    relation_break_time     VARCHAR(64),
    relation_update_time    VARCHAR(64),
    department_ids          VARCHAR(500)
)
DUPLICATE KEY(id)
COMMENT 'SCRM 客户员工关系表-ODS'
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
