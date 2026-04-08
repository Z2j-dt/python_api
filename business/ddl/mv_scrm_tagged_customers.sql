CREATE MATERIALIZED VIEW `mv_scrm_tagged_customers` (`external_id`, `name`, `user_id`, `user_name`, `add_time`, `tag_id`, `tag_name`, `group_id`, `group_name`)
COMMENT "私信投流标签-客户员工标签明细，每60s刷新"
DISTRIBUTED BY RANDOM
REFRESH ASYNC EVERY(INTERVAL 3600 SECOND)
PROPERTIES (
"replicated_storage" = "true",
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS
WITH
max_day_ce AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_customer_employee`
),
max_day_c AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_customer`
),
max_day_ct AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
),
max_day_t AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_tag`
),
max_day_g AS (
  SELECT MAX(`day`) AS max_day
  FROM `hive_catalog`.`ods`.`o_scrm_tag_group`
),
m AS (
  SELECT t.external_id, t.user_id, t.user_name, t.remark, t.add_time
  FROM (
    SELECT
      ce.external_id,
      ce.user_id,
      ce.user_name,
      ce.remark,
      ce.add_time,
      ROW_NUMBER() OVER (PARTITION BY ce.external_id ORDER BY ce.add_time ASC) AS rn
    FROM `hive_catalog`.`ods`.`o_scrm_customer_employee` ce
    INNER JOIN `portal_db`.`config_channel_staff` cs
      ON ce.user_name = cs.staff_name
    WHERE ce.add_time IS NOT NULL
      AND ce.add_time != ''
      AND cs.channel_type = '开户渠道'
      AND ce.`day` = (SELECT max_day FROM max_day_ce)
  ) t
  WHERE t.rn = 1
),
tb AS (
  SELECT DISTINCT c.name, c.external_id
  FROM `hive_catalog`.`ods`.`o_scrm_customer` c
  WHERE c.`day` = (SELECT max_day FROM max_day_c)
),
tc AS (
  SELECT DISTINCT ct.external_id, ct.tag_id
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag` ct
  WHERE ct.`day` = (SELECT max_day FROM max_day_ct)
),
ti AS (
  SELECT t.tag_id, t.tag_name, t.group_id
  FROM `hive_catalog`.`ods`.`o_scrm_tag` t
  WHERE t.`day` = (SELECT max_day FROM max_day_t)
),
g AS (
  SELECT g.group_id, g.name
  FROM `hive_catalog`.`ods`.`o_scrm_tag_group` g
  WHERE g.`day` = (SELECT max_day FROM max_day_g)
)
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
FROM m
INNER JOIN tb
  ON m.external_id = tb.external_id
INNER JOIN tc
  ON m.external_id = tc.external_id
INNER JOIN ti
  ON tc.tag_id = ti.tag_id
INNER JOIN g
  ON ti.group_id = g.group_id;