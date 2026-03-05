-- 门户埋点日志表：由 portal/app.py 的 _log_event 直接写入
-- 使用与其他业务表相同的 StarRocks 库（请将 your_database 替换为实际库名）
--
-- CREATE DATABASE IF NOT EXISTS your_database;
-- USE your_database;
--
-- 建表语句

CREATE TABLE IF NOT EXISTS portal_event_log (
    event_date   DATE            NOT NULL COMMENT '事件日期（UTC）',
    ts           DATETIME        NOT NULL COMMENT '事件时间（UTC）',
    event        VARCHAR(64)     NOT NULL COMMENT '事件类型，如 login_success/nav_click/module_load',
    username     VARCHAR(64)     NULL     COMMENT '门户账号',
    module       VARCHAR(64)     NULL     COMMENT '模块 ID，如 sr_api/hive_metadata 等',
    view         VARCHAR(64)     NULL     COMMENT '视图/子页面，如 config_stock_position',
    label        VARCHAR(256)    NULL     COMMENT '前端文案，如菜单名称',
    ip           VARCHAR(64)     NULL     COMMENT '客户端 IP',
    ua           VARCHAR(512)    NULL     COMMENT 'User-Agent',
    path         VARCHAR(256)    NULL     COMMENT '请求路径',
    extra_json   JSON            NULL     COMMENT '原始 extra 字段 JSON'
)
PRIMARY KEY(event_date, ts, username, event)
COMMENT '统一门户-埋点事件明细'
DISTRIBUTED BY HASH(event_date) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);


-- ========== 示例物化视图 1：账号登录统计 ==========
-- 统计每个账号每天登录次数 + 最近一次登录时间，用于看活跃度和最近登录

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_portal_login_stats
COMMENT '门户账号登录统计：按日期/账号聚合登录次数与最近登录时间'
REFRESH ASYNC EVERY(INTERVAL 60 SECOND)
AS
SELECT
    event_date                                           AS dt,
    username,
    COUNT(*)                                             AS login_cnt,
    MAX(ts)                                              AS last_login_ts
FROM portal_event_log
WHERE event = 'login_success'
GROUP BY event_date, username;


-- ========== 示例物化视图 2：模块/视图使用统计 ==========
-- 统计每个模块/视图每天被打开的次数（可用于看哪些功能最常用）

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_portal_module_stats
COMMENT '门户模块使用统计：按日期/模块/视图聚合 module_load 次数'
REFRESH ASYNC EVERY(INTERVAL 60 SECOND)
AS
SELECT
    event_date                               AS dt,
    COALESCE(module, 'unknown')              AS module,
    COALESCE(view, 'unknown')                AS view,
    COUNT(*)                                 AS load_pv
FROM portal_event_log
WHERE event = 'module_load'
GROUP BY event_date, COALESCE(module, 'unknown'), COALESCE(view, 'unknown');


-- ========== 使用说明 ==========
-- 1. 在 StarRocks 中执行本文件（先替换库名 your_database）：
--      SOURCE portal_event_log_ddl.sql;
--
-- 2. 在门户容器/进程中配置 StarRocks 连接信息（与 business 一致）：
--      STARROCKS_HOST, STARROCKS_PORT, STARROCKS_USER, STARROCKS_PASSWORD, STARROCKS_DATABASE
--    可选：
--      PORTAL_EVENT_TABLE=portal_event_log
--      PORTAL_EVENT_TO_DB=1
--
-- 3. 部署后，portal/app.py 的 _log_event 会在写本地 portal_events.log 的同时，
--    追加写入 portal_event_log 表，物化视图会每 60 秒自动聚合，供报表查询。

