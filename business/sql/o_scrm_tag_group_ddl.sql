-- StarRocks 建表语句: o_scrm_tag_group
-- 上游: MySQL enterprise_weixin.tag_group
-- 用途: DataX 同步 ODS 层（create_time 为当日数据）

CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

CREATE TABLE IF NOT EXISTS o_scrm_tag_group (
    group_id                VARCHAR(255),
    name                    VARCHAR(255),
    create_time             VARCHAR(64),
    sort                    VARCHAR(64),
    owner                   VARCHAR(255),
    low_sort                VARCHAR(64),
    qw_order                VARCHAR(64),
    is_system               VARCHAR(32),
    update_time             VARCHAR(64)
)
DUPLICATE KEY(group_id)
COMMENT 'SCRM 标签组表-ODS'
DISTRIBUTED BY HASH(group_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
