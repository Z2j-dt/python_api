CREATE MATERIALIZED VIEW `mv_tag_unionid_permission` (
    `tag_name`,
    `union_id`
)
COMMENT "客户标签"
DISTRIBUTED BY RANDOM
REFRESH ASYNC START("2026-03-31 06:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
    "replicated_storage" = "true",
    "session.group_concat_max_len" = "1048576",
    "replication_num" = "3",
    "storage_medium" = "HDD"
)
AS
--  重复添加
SELECT 
    tmp.tag_name,
    GROUP_CONCAT(DISTINCT tmp.union_id SEPARATOR ',') AS union_id
FROM (
    -- 重复添加
    SELECT union_id, '重复添加' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE empy_cnts > 1

    UNION ALL
    -- 高频互动
    SELECT union_id, '高频互动' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wc_interact_cnt` >= 5

    UNION ALL
    -- 偶尔互动
    SELECT union_id, '偶尔互动' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wc_interact_cnt` >= 1
      AND `30d_wc_interact_cnt` <= 4

    UNION ALL
    -- 长期不互动
    SELECT union_id, '长期不互动' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wc_interact_cnt` = 0

    UNION ALL
    -- 已开口
    SELECT union_id, '已开口' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE is_spoken = '1'

    UNION ALL
    -- 高频观看（王柱）
    SELECT union_id, '高频观看（王柱）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangzhu_streams` >= 6

    UNION ALL
    -- 中频观看（王柱）
    SELECT union_id, '中频观看（王柱）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangzhu_streams` >= 2
      AND `30d_wangzhu_streams` <= 5

    UNION ALL
    -- 低频观看（王柱）
    SELECT union_id, '低频观看（王柱）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangzhu_streams` = 1

    UNION ALL
    -- 高频观看（王雨时）
    SELECT union_id, '高频观看（王雨时）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangyushi_streams` >= 6

    UNION ALL
    -- 中频观看（王雨时）
    SELECT union_id, '中频观看（王雨时）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangyushi_streams` >= 2
      AND `30d_wangyushi_streams` <= 5

    UNION ALL
    -- 低频观看（王雨时）
    SELECT union_id, '低频观看（王雨时）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangyushi_streams` = 1

    UNION ALL
    -- 直播商品点击（王柱）
    SELECT union_id, '直播商品点击（王柱）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangzhu_click_prod` = '1'

    UNION ALL
    -- 直播商品点击（王雨时）
    SELECT union_id, '直播商品点击（王雨时）' AS tag_name
    FROM etl_db.tag_wxcust_profit
    WHERE `30d_wangyushi_click_prod` = '1'
    ) tmp
WHERE tmp.tag_name IS NOT NULL
GROUP BY tmp.tag_name
ORDER BY tmp.tag_name ASC;