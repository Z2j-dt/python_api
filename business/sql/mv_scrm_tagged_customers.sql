CREATE MATERIALIZED VIEW IF NOT EXISTS mv_scrm_tagged_customers
COMMENT '私信投流标签-客户员工标签明细，每60s刷新'
REFRESH ASYNC EVERY(INTERVAL 60 SECOND)
AS
SELECT 
    m.external_id,
    tb.name,
    m.user_id,
    m.user_name,
    FROM_UNIXTIME(CAST(m.add_time AS BIGINT)) AS add_time,
    ti.tag_id,
    ti.tag_name,
    ti.group_id,
    g.name AS group_name
FROM (
    -- 最小添加时间对应的客户（按 external_id 取 add_time 最早一条）
    SELECT 
        external_id,
        user_id,
        user_name,
        remark,
        add_time
    FROM (
        SELECT 
            external_id,
            user_id,
            user_name,
            remark,
            add_time,
            ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY add_time ASC) AS rn
        FROM portal_db.o_scrm_customer_employee ce
        JOIN portal_db.config_channel_staff cs
          ON ce.user_name = cs.staff_name
        WHERE ce.add_time IS NOT NULL 
          AND ce.add_time != ''
        --   AND CAST(ce.add_time AS BIGINT) >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    ) t
    WHERE rn = 1
) m
JOIN (
    -- 有标签的客户（Hive，取最新 day）
    SELECT DISTINCT name, external_id
    FROM portal_db.o_scrm_customer
) tb ON m.external_id = tb.external_id
JOIN (
    -- 有标签的客户（Hive，取最新 day）
    SELECT DISTINCT external_id, tag_id
    FROM portal_db.o_scrm_customer_tag
) tc ON m.external_id = tc.external_id
JOIN (
    -- 标签信息（Hive，取最新 day）
    SELECT tag_id, tag_name, group_id
    FROM hive_catalog.ods.o_scrm_tag
    WHERE day = (SELECT MAX(day) FROM hive_catalog.ods.o_scrm_tag)
) ti ON tc.tag_id = ti.tag_id
JOIN (
    -- 标签组信息（Hive，取最新 day）
    SELECT group_id, name
    FROM hive_catalog.ods.o_scrm_tag_group
    WHERE day = (SELECT MAX(day) FROM hive_catalog.ods.o_scrm_tag_group)
) g ON ti.group_id = g.group_id;



DROP MATERIALIZED VIEW IF EXISTS mv_scrm_tagged_customers;
-- 然后执行新的 CREATE 语句
