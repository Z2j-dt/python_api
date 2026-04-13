-- 门户登录账号表（与 portal_user_resource 权限表配合；启用 PORTAL_AUTH_FROM_DB=1 后不再使用 users.json）
-- 密码存 werkzeug 哈希，生成示例：
--   python -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('你的密码'))"
--
-- USE your_database;

-- StarRocks 对列 DEFAULT（含 DEFAULT 0）支持有限，易报 1064；插入时由应用显式写入 is_superuser（0/1）。
CREATE TABLE IF NOT EXISTS portal_user (
    username      VARCHAR(64)  NOT NULL COMMENT '登录名',
    password_hash VARCHAR(512) NOT NULL COMMENT 'werkzeug 密码哈希',
    is_superuser  TINYINT      NOT NULL COMMENT '1=超级用户，登录后全权限且可进管理页；插入时必须写 0 或 1',
    updated_at    DATETIME     NOT NULL COMMENT '最后更新时间',
    created_at    DATETIME     NOT NULL COMMENT '创建时间'
)
PRIMARY KEY (username)
COMMENT '统一门户-登录账号'
DISTRIBUTED BY HASH(username) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
