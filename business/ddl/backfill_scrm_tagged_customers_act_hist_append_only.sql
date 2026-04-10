-- =============================================================================
-- 历史回灌（append-only 非分区表 dwd_scrm_tagged_customers_act_hist）
-- =============================================================================
-- 与 fact_scrm_tagged_customers_act_hist_append_only.sql 配套使用。
--
-- 用法 1（推荐）：固定某一天 ODS 全量快照，把其中所有历史 add_time 一次性 INSERT 进 hist
--   - 替换 ${ods_day} 为 yyyymmdd（8 位），例如 20260408
--   - 标签/客户等维度均取该 ODS 日快照（非「按 add_dt 还原历史标签」）
--
-- 用法 2：只灌「加微日」落在某区间内的行（减少脏数据或分阶段回灌）
--   - 打开「可选 WHERE」那一段，替换 ${add_dt_from}、${add_dt_to}
--
-- 重复执行会叠行（DUPLICATE 模型）。回灌前若要幂等，用下面可选 DELETE。
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 可选：整表清空后首次全量回灌（慎用，仅空表或接受全删时打开）
-- -----------------------------------------------------------------------------
-- TRUNCATE TABLE `dwd_scrm_tagged_customers_act_hist`;


-- -----------------------------------------------------------------------------
-- 可选：只删掉「加微日」在某区间内的数据再重灌（幂等，按需打开并改日期）
-- -----------------------------------------------------------------------------
-- DELETE FROM `dwd_scrm_tagged_customers_act_hist`
-- WHERE add_dt >= CAST('${add_dt_from}' AS DATE)
--   AND add_dt <= CAST('${add_dt_to}' AS DATE);


-- -----------------------------------------------------------------------------
-- 历史写入：INSERT INTO（不清其它日期已有数据；与 OVERWRITE 整表不同）
-- 替换 ${ods_day}，例如 20260408
-- -----------------------------------------------------------------------------
INSERT INTO `dwd_scrm_tagged_customers_act_hist`
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
  WHERE ce.`day` = '${ods_day}'
    AND ce.add_time IS NOT NULL
    AND ce.add_time != ''
) ce
INNER JOIN (
  SELECT DISTINCT external_id, name
  FROM `hive_catalog`.`ods`.`o_scrm_customer`
  WHERE `day` = '${ods_day}'
) c
  ON ce.external_id = c.external_id
INNER JOIN (
  SELECT DISTINCT external_id, tag_id
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
  WHERE `day` = '${ods_day}'
) ct
  ON ce.external_id = ct.external_id
INNER JOIN (
  SELECT tag_id, tag_name, group_id
  FROM `hive_catalog`.`ods`.`o_scrm_tag`
  WHERE `day` = '${ods_day}'
) t
  ON ct.tag_id = t.tag_id
INNER JOIN (
  SELECT group_id, name AS group_name
  FROM `hive_catalog`.`ods`.`o_scrm_tag_group`
  WHERE `day` = '${ods_day}'
) g
  ON t.group_id = g.group_id
INNER JOIN `portal_db`.`config_activity_channel_tag` ca
  ON t.tag_name = ca.wechat_customer_tag
 AND ca.open_channel IS NOT NULL
 AND TRIM(ca.open_channel) <> ''
;

-- 若只需灌「加微日」在 [add_dt_from, add_dt_to] 内的行：把上面分号改到条件最后，并在 TRIM 条件后追加：
-- AND CAST(CONCAT(SUBSTRING(ce.add_time, 1, 4), '-', SUBSTRING(ce.add_time, 5, 2), '-', SUBSTRING(ce.add_time, 7, 2)) AS DATE) >= CAST('${add_dt_from}' AS DATE)
-- AND CAST(CONCAT(SUBSTRING(ce.add_time, 1, 4), '-', SUBSTRING(ce.add_time, 5, 2), '-', SUBSTRING(ce.add_time, 7, 2)) AS DATE) <= CAST('${add_dt_to}' AS DATE)
