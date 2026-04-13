-- 门户账号-子模块权限表（单独一张表，与账号密码配置解耦）
-- 密码仍在 portal/users.json 或 auth_config.py；本表只存「谁能看/改哪个 resource_id」。
-- resource_id 约定见：portal/docs/module_permissions.md
--
-- 规则简要：
--   - 某账号在本表中没有任何行 ⇒ 该账号对任何子模块均无权限（侧栏不出现对应入口）。
--   - 一行表示一个 (username, resource_id) 的授权；access_level = read | write。
--   - 是否给 admin 插满全量行，或由代码把 admin 视为超级用户，二选一即可（见文件末尾说明）。
--
-- 在 StarRocks 中执行（库名请改为实际库，与 STARROCKS_DATABASE / portal 埋点一致）：
--   USE your_database;
--   SOURCE portal/sql/portal_user_resource_ddl.sql;
--
-- 门户启用本表权限（与 users.json 密码并存）需在环境中设置：
--   PORTAL_PERMISSION_FROM_DB=1
-- 可选：PORTAL_PERMISSION_TABLE=portal_user_resource（默认即此表名）
-- 可选：PORTAL_SUPERUSERS=admin 列表内账号不查表，视为全部 resource write（多个用逗号分隔）

CREATE TABLE IF NOT EXISTS portal_user_resource (
    username     VARCHAR(64)  NOT NULL COMMENT '门户登录名，与 users.json 等账号源一致',
    resource_id  VARCHAR(128) NOT NULL COMMENT '资源 ID，如 hive_metadata、sr_api:realtime',
    access_level VARCHAR(16)  NOT NULL COMMENT 'read=只读, write=可编辑',
    updated_at   DATETIME     NOT NULL COMMENT '授权最后变更时间（应用写入）',
    updated_by   VARCHAR(64)  NULL     COMMENT '操作人门户账号，如 admin'
)
PRIMARY KEY (username, resource_id)
COMMENT '统一门户-账号与子模块权限'
DISTRIBUTED BY HASH(username) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);


-- ========== 示例数据（按需改用户名后执行） ==========
-- INSERT INTO portal_user_resource (username, resource_id, access_level, updated_at, updated_by) VALUES
-- ('zhangsan', 'hive_metadata', 'read', NOW(), 'admin'),
-- ('zhangsan', 'sr_api:realtime', 'write', NOW(), 'admin');


-- ========== 常用查询 ==========
-- 某用户全部权限（门户登录后加载进 Cookie前查询）：
-- SELECT resource_id, access_level
-- FROM portal_user_resource
-- WHERE username = 'zhangsan';

-- 撤销某用户某一资源：
-- DELETE FROM portal_user_resource WHERE username = 'zhangsan' AND resource_id = 'sr_api:realtime';


-- ========== admin 策略（实现时二选一） ==========
-- A) 表中不存 admin，代码里若 username = admin 则视为全部 resource_id 为 write（改权限只动普通账号）。
-- B) admin 也在表中维护行（与其他用户一致），便于审计；改 admin 权限需 DBA/超级入口。


-- ========== 若使用 MySQL 而非 StarRocks，可用下方等价语句（注释掉上方 SR 段后执行） ==========
/*
CREATE TABLE IF NOT EXISTS portal_user_resource (
    username     VARCHAR(64)  NOT NULL COMMENT '门户登录名',
    resource_id VARCHAR(128) NOT NULL COMMENT '资源 ID',
    access_level VARCHAR(16)  NOT NULL COMMENT 'read 或 write',
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by   VARCHAR(64)  NULL,
    PRIMARY KEY (username, resource_id),
    CONSTRAINT chk_portal_access CHECK (access_level IN ('read', 'write'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='统一门户-账号与子模块权限';
*/
