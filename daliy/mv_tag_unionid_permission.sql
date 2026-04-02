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
    GROUP_CONCAT(DISTINCT tmp.union_id ORDER BY tmp.union_id ASC SEPARATOR ',') AS union_id
FROM (
    -- 创业板
    SELECT union_id, CASE WHEN empy_cnts > 1 THEN '重复添加' END AS tag_name
    from etl_db.tag_wxcust_profit 
    ) tmp
WHERE tmp.tag_name IS NOT NULL
GROUP BY tmp.tag_name
ORDER BY tmp.tag_name ASC;