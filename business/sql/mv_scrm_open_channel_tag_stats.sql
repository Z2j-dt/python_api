CREATE MATERIALIZED VIEW IF NOT EXISTS mv_scrm_open_channel_tag_stats
COMMENT '私信投流标签-按日期/开户渠道/客户标签汇总加微数（历史+当日），每60s刷新'
REFRESH ASYNC EVERY(INTERVAL 60 SECOND)
AS
SELECT
    CAST(e.add_time AS DATE) AS dt,                    -- 日期
    c.open_channel,                                    -- 开户渠道
    e.tag_name AS wechat_customer_tag,                 -- 客户标签（企微客户标签）
    COUNT(*) AS total_add_cnt,                         -- 总加微数（按明细行计数，同一客户多次/多员工都算）
    SUM(CASE WHEN e.branch_name = '成都' THEN 1 ELSE 0 END) AS chengdu_add_cnt,  -- 成都加微数
    SUM(CASE WHEN e.branch_name = '云分' THEN 1 ELSE 0 END) AS yunfen_add_cnt,   -- 云分加微数
    SUM(CASE WHEN e.branch_name = '浙分' THEN 1 ELSE 0 END) AS zhefen_add_cnt,   -- 浙分加微数
    SUM(CASE WHEN e.branch_name = '海分' THEN 1 ELSE 0 END) AS haifen_add_cnt,   -- 海分加微数
    SUM(CASE WHEN e.branch_name = '数金' THEN 1 ELSE 0 END) AS shujin_add_cnt,   -- 数金加微数
    MAX(dy.use_amt) AS douyin_use_amt                                            -- 抖音消耗值（channel_name=open_channel，取最新分区）
FROM (
    SELECT
        ce.external_id,
        FROM_UNIXTIME(CAST(ce.add_time AS BIGINT)) AS add_time,
        cs.branch_name,
        ti.tag_name
    FROM (
        -- 历史快照（Hive 最新一天，包含历史 add_time）+ 当日增量（StarRocks/MySQL 同步表）
        SELECT external_id, user_name, add_time
        FROM hive_catalog.ods.o_scrm_customer_employee
        WHERE day = (SELECT MAX(day) FROM hive_catalog.ods.o_scrm_customer_employee)
        UNION
        SELECT external_id, user_name, add_time
        FROM test_db.o_scrm_customer_employee
    ) ce
    JOIN test_db.config_channel_staff cs
      ON ce.user_name = cs.staff_name
    JOIN (
        SELECT DISTINCT external_id, tag_id
        FROM test_db.o_scrm_customer_tag
    ) tc ON ce.external_id = tc.external_id
    JOIN (
        SELECT tag_id, tag_name
        FROM hive_catalog.ods.o_scrm_tag
        WHERE day = (SELECT MAX(day) FROM hive_catalog.ods.o_scrm_tag)
    ) ti ON tc.tag_id = ti.tag_id
    WHERE ce.add_time IS NOT NULL
      AND ce.add_time != ''
      AND CAST(ce.add_time AS BIGINT) > 0
) e
JOIN test_db.config_open_channel_tag c
  ON e.tag_name = c.wechat_customer_tag
LEFT JOIN (
    -- 抖音消耗：按渠道+日期汇总，取最新分区；channel_name 与 open_channel 关联
    SELECT
        channel,
        entry,
        use_amt
    FROM test_db.mv_channel_use_amt
) dy ON c.open_channel = dy.channel AND CAST(e.add_time AS DATE) = dy.entry
GROUP BY
    CAST(e.add_time AS DATE),
    c.open_channel,
    e.tag_name;


DROP MATERIALIZED VIEW IF EXISTS mv_scrm_open_channel_tag_stats;
-- 然后执行上面的 CREATE 语句
