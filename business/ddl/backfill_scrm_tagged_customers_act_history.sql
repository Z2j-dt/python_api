-- 全量快照回灌（固定某一天 ODS.day，标签等同日快照）
-- 执行前替换表名（若与 dwd 不一致）、日期 '2026-04-08'
-- 语法：无 WITH、无 INSERT 列清单（兼容旧版 StarRocks）

INSERT OVERWRITE `dwd_scrm_tagged_customers_act_hist`
SELECT
  ce.external_id,
  c.name,
  CAST(ce.user_id AS STRING) AS user_id,
  ce.user_name,
  CAST(CONCAT(SUBSTRING(ce.add_time, 1, 4), '-', SUBSTRING(ce.add_time, 5, 2), '-', SUBSTRING(ce.add_time, 7, 2)) AS DATE) AS add_dt,
  ce.add_time,
  t.tag_id,
  t.tag_name,
  ca.open_channel,
  t.group_id,
  g.group_name
FROM (
  SELECT
    ce.external_id,
    ce.user_id,
    ce.user_name,
    DATE_FORMAT(FROM_UNIXTIME(CAST(ce.add_time AS BIGINT)), '%Y%m%d') AS add_time
  FROM `hive_catalog`.`ods`.`o_scrm_customer_employee` ce
  INNER JOIN `portal_db`.`config_channel_staff` cs
    ON ce.user_name = cs.staff_name
   AND cs.channel_type = '活动渠道'
  WHERE ce.`day` = '2026-04-08'
    AND ce.add_time IS NOT NULL
    AND ce.add_time != ''
) ce
INNER JOIN (
  SELECT DISTINCT external_id, name
  FROM `hive_catalog`.`ods`.`o_scrm_customer`
  WHERE `day` = '2026-04-08'
) c
  ON ce.external_id = c.external_id
INNER JOIN (
  SELECT DISTINCT external_id, tag_id
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
  WHERE `day` = '2026-04-08'
) ct
  ON ce.external_id = ct.external_id
INNER JOIN (
  SELECT tag_id, tag_name, group_id
  FROM `hive_catalog`.`ods`.`o_scrm_tag`
  WHERE `day` = '2026-04-08'
) t
  ON ct.tag_id = t.tag_id
INNER JOIN (
  SELECT group_id, name AS group_name
  FROM `hive_catalog`.`ods`.`o_scrm_tag_group`
  WHERE `day` = '2026-04-08'
) g
  ON t.group_id = g.group_id
INNER JOIN `portal_db`.`config_activity_channel_tag` ca
  ON t.tag_name = ca.wechat_customer_tag
 AND ca.open_channel IS NOT NULL
 AND TRIM(ca.open_channel) <> '';
