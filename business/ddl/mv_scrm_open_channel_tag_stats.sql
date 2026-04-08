CREATE MATERIALIZED VIEW `mv_scrm_open_channel_tag_stats` (`dt`, `open_channel`, `wechat_customer_tag`, `total_add_cnt`, `chengdu_add_cnt`, `yunfen_add_cnt`, `zhefen_add_cnt`, `haifen_add_cnt`, `shujin_add_cnt`, `douyin_use_amt`)
COMMENT "私信投流标签-按日期/开户渠道/客户标签汇总加微数（历史+当日），每3600s刷新"
DISTRIBUTED BY RANDOM
REFRESH ASYNC EVERY(INTERVAL 3600 SECOND)
PROPERTIES (
"replicated_storage" = "true",
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS
WITH
max_day_ce AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_customer_employee`
),
max_day_ct AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
),
max_day_ti AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_tag`
),
ce AS (
  SELECT external_id, user_name, add_time
  FROM `hive_catalog`.`ods`.`o_scrm_customer_employee`
  WHERE `day` = (SELECT max_day FROM max_day_ce)
),
tc AS (
  SELECT DISTINCT external_id, tag_id
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
  WHERE `day` = (SELECT max_day FROM max_day_ct)
),
ti AS (
  SELECT tag_id, tag_name
  FROM `hive_catalog`.`ods`.`o_scrm_tag`
  WHERE `day` = (SELECT max_day FROM max_day_ti)
),
e AS (
  SELECT
    ce.external_id,
    FROM_UNIXTIME(CAST(ce.add_time AS BIGINT)) AS add_time,
    cs.branch_name,
    ti.tag_name
  FROM ce
  INNER JOIN `portal_db`.`config_channel_staff` cs
    ON ce.user_name = cs.staff_name
  INNER JOIN tc
    ON ce.external_id = tc.external_id
  INNER JOIN ti
    ON tc.tag_id = ti.tag_id
  WHERE ce.add_time IS NOT NULL
    AND ce.add_time != ''
    AND CAST(ce.add_time AS BIGINT) > 0
    AND cs.channel_type = '开户渠道'
)
SELECT
  CAST(e.add_time AS DATE) AS dt,
  c.open_channel,
  e.tag_name AS wechat_customer_tag,
  COUNT(*) AS total_add_cnt,
  SUM(CASE WHEN e.branch_name = '成都' THEN 1 ELSE 0 END) AS chengdu_add_cnt,
  SUM(CASE WHEN e.branch_name = '云分' THEN 1 ELSE 0 END) AS yunfen_add_cnt,
  SUM(CASE WHEN e.branch_name = '浙分' THEN 1 ELSE 0 END) AS zhefen_add_cnt,
  SUM(CASE WHEN e.branch_name = '海分' THEN 1 ELSE 0 END) AS haifen_add_cnt,
  SUM(CASE WHEN e.branch_name = '数金' THEN 1 ELSE 0 END) AS shujin_add_cnt,
  MAX(dy.use_amt) AS douyin_use_amt
FROM e
INNER JOIN `portal_db`.`config_open_channel_tag` c
  ON e.tag_name = c.wechat_customer_tag
LEFT JOIN `portal_db`.`mv_channel_use_amt` dy
  ON c.open_channel = dy.channel
 AND CAST(e.add_time AS DATE) = dy.entry
GROUP BY
  CAST(e.add_time AS DATE),
  c.open_channel,
  e.tag_name;
