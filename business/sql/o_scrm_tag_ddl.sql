-- StarRocks 建表语句: o_scrm_tag
-- 上游: MySQL enterprise_weixin.tag
-- 用途: DataX 同步 ODS 层（create_time 为当日数据）

CREATE DATABASE IF NOT EXISTS portal_db;

USE portal_db;

CREATE TABLE IF NOT EXISTS o_scrm_tag (
    id                      BIGINT,
    tag_id                  VARCHAR(255),
    tag_name                VARCHAR(255),
    create_time             VARCHAR(64),
    group_id                VARCHAR(255),
    `type`                  VARCHAR(64),
    disabled                VARCHAR(32),
    owner                   VARCHAR(255)
)
DUPLICATE KEY(id)
COMMENT 'SCRM 标签表-ODS'
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
