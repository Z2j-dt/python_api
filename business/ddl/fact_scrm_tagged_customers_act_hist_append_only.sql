-- =============================================================================
-- 方案：不做分区表，按天追加写入，不清历史
-- =============================================================================
-- - 表为普通 OLAP 表（无 PARTITION BY、无动态分区），历史行一直保留
-- - 每日任务：只把「加微日 add_dt = 业务日」的数据 INSERT 进去
-- - 重跑同一天：先执行可选 DELETE（删掉该 add_dt 再插），避免 DUPLICATE 重复行
--
-- 表名可按线上一致修改（例如 scrm_tagged_customers_act_hist）
-- ${biz_day} 替换为 yyyymmdd（8 位），如 20260409；ODS 各表 `day` 与此格式一致
-- 历史一次性回灌：见 backfill_scrm_tagged_customers_act_hist_append_only.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `dwd_scrm_tagged_customers_act_hist` (
  external_id   VARCHAR(255) COMMENT '企微客户external_id',
  name          VARCHAR(255) COMMENT '企微客户昵称/姓名',
  user_id       VARCHAR(255) COMMENT '销售员工ID',
  user_name     VARCHAR(255) COMMENT '销售员工名称',
  add_dt        DATE         COMMENT '加微日期(业务日，非分区键)',
  add_time      VARCHAR(8)   COMMENT '加微时间(yyyymmdd)',
  tag_id        VARCHAR(255) COMMENT '标签ID',
  tag_name      VARCHAR(255) COMMENT '标签名称(按add_time快照)',
  open_channel  VARCHAR(255) COMMENT '渠道名称',
  group_id      VARCHAR(255) COMMENT '标签组ID',
  group_name    VARCHAR(255) COMMENT '标签组名称(按add_time快照)'
)
ENGINE=OLAP
DUPLICATE KEY(`external_id`, `name`, `user_id`, `user_name`, `add_dt`, `add_time`, `tag_id`)
COMMENT "活动渠道销售员工加微标签历史表（追加写入、不按日清库）"
DISTRIBUTED BY HASH(`external_id`) BUCKETS 16
PROPERTIES (
  "replication_num" = "1"
);


-- -----------------------------------------------------------------------------
-- 可选：同一天任务重跑时先删再插（幂等）。若确定只跑一次，可整段注释掉
-- -----------------------------------------------------------------------------
-- DELETE FROM `dwd_scrm_tagged_customers_act_hist`
-- WHERE add_time = '${biz_day}';


-- -----------------------------------------------------------------------------
-- 每日追加：仅写入 add_dt = 业务日 的行（不动其它日期的历史数据）
-- ce.`day` = 当日 ODS 分区（yyyymmdd），与「只处理当天同步变更」一致；可按任务改成全量 employee
-- -----------------------------------------------------------------------------
INSERT INTO `dwd_scrm_tagged_customers_act_hist`
SELECT
  ce.external_id,
  c.name,
  CAST(ce.user_id AS STRING) AS user_id,
  ce.user_name,
  CAST(CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2)) AS DATE) AS add_dt,
  ce.add_time_ymd AS add_time,
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
    DATE_FORMAT(FROM_UNIXTIME(CAST(ce.add_time AS BIGINT)), '%Y%m%d') AS add_time_ymd
  FROM `hive_catalog`.`ods`.`o_scrm_customer_employee` ce
  INNER JOIN `portal_db`.`config_channel_staff` cs
    ON ce.user_name = cs.staff_name
   AND cs.channel_type = '活动渠道'
  WHERE ce.`day` = '${biz_day}'
    AND ce.add_time IS NOT NULL
    AND ce.add_time != ''
) ce
INNER JOIN (
  SELECT DISTINCT external_id, name, `day`
  FROM `hive_catalog`.`ods`.`o_scrm_customer`
) c
  ON ce.external_id = c.external_id
 AND c.`day` = ce.add_time_ymd
INNER JOIN (
  SELECT DISTINCT external_id, tag_id, `day`
  FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
) ct
  ON ce.external_id = ct.external_id
 AND ct.`day` = ce.add_time_ymd
INNER JOIN (
  SELECT tag_id, tag_name, group_id, `day`
  FROM `hive_catalog`.`ods`.`o_scrm_tag`
) t
  ON ct.tag_id = t.tag_id
 AND t.`day` = ce.add_time_ymd
INNER JOIN (
  SELECT group_id, name AS group_name, `day`
  FROM `hive_catalog`.`ods`.`o_scrm_tag_group`
) g
  ON t.group_id = g.group_id
 AND g.`day` = ce.add_time_ymd
INNER JOIN `portal_db`.`config_activity_channel_tag` ca
  ON t.tag_name = ca.wechat_customer_tag
 AND ca.open_channel IS NOT NULL
 AND TRIM(ca.open_channel) <> ''
WHERE ce.add_time_ymd = '${biz_day}';


-- 视图可与分区方案共用（表结构一致即可）
CREATE VIEW IF NOT EXISTS `mv_scrm_tagged_customers_act_hist_view`
COMMENT "活动渠道销售员工加微标签历史视图"
AS
SELECT
  external_id, name, user_id, user_name, add_dt, add_time,
  tag_id, tag_name, open_channel, group_id, group_name
FROM `dwd_scrm_tagged_customers_act_hist`;
