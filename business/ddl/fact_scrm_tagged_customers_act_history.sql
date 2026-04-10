-- 目标：
-- 1) 按 add_time 做增量沉淀（每日跑一次）
-- 2) 标签名称/标签组名称按 add_time 当天快照取值，保留历史，不被次日改名覆盖
--
-- 若不做分区、只按天追加写入且不清历史：见 fact_scrm_tagged_customers_act_hist_append_only.sql
--
-- 核心口径：
-- - 不再使用 MAX(day) 全量最新快照口径
-- - 统一用 add_time 对应日期（add_dt）去关联 o_scrm_* 各表的 day 分区
--
-- 使用方式：
-- - ${biz_day} 格式：YYYY-MM-DD（例如 2026-04-09）
-- - ${p_part} 格式：动态分区名 = prefix + yyyymmdd，例如 p20260409（与 dynamic_partition.prefix=p 一致）
-- - 每天跑一次下面「增量」语句；若环境不支持 INSERT...WITH，已改为内联子查询

CREATE TABLE IF NOT EXISTS `dwd_scrm_tagged_customers_act_hist` (
  external_id   VARCHAR(255) COMMENT '企微客户external_id',
  name          VARCHAR(255) COMMENT '企微客户昵称/姓名',
  user_id       VARCHAR(255) COMMENT '销售员工ID',
  user_name     VARCHAR(255) COMMENT '销售员工名称',
  add_dt        DATE         COMMENT '加微日期(分区键)',
  add_time      VARCHAR(8)   COMMENT '加微时间(yyyymmdd)',
  tag_id        VARCHAR(255) COMMENT '标签ID',
  tag_name      VARCHAR(255) COMMENT '标签名称(按add_time快照)',
  open_channel  VARCHAR(255) COMMENT '渠道名称',
  group_id      VARCHAR(255) COMMENT '标签组ID',
  group_name    VARCHAR(255) COMMENT '标签组名称(按add_time快照)'
)
ENGINE=OLAP
DUPLICATE KEY(`external_id`, `name`, `user_id`, `user_name`, `add_dt`, `add_time`, `tag_id`)
COMMENT "活动渠道销售员工加微标签历史事实表（按add_time快照口径）"
PARTITION BY RANGE(`add_dt`) (
  PARTITION p_init VALUES [("1900-01-01"), ("1900-01-02"))
)
DISTRIBUTED BY HASH(`external_id`) BUCKETS 16
PROPERTIES (
  "replication_num" = "1",
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.start" = "-365",
  "dynamic_partition.end" = "7",
  "dynamic_partition.history_partition_num" = "365"
);


-- 每日增量装载（按 biz_day 覆盖当天分区，幂等）
-- 说明：
-- - ce.day = biz_day：只处理当天增量变更数据
-- - add_dt = FROM_UNIXTIME(add_time) 对应日：作为历史口径日
-- - 其余表一律按 add_dt 对应的 day 去取快照值（历史还原）
-- - 活动渠道员工仍受 config_channel_staff(channel_type='活动渠道') 约束

-- 增量：只覆盖「加微日 add_dt = ${biz_day}」对应分区；执行前替换 ${biz_day}、${p_part}
-- 若 PARTITION 报错，把 ${p_part} 改成你库里实际分区名（SHOW PARTITIONS FROM table;）
INSERT OVERWRITE `dwd_scrm_tagged_customers_act_hist`
PARTITION (${p_part})
SELECT
  s.external_id,
  s.name,
  s.user_id,
  s.user_name,
  s.add_dt,
  s.add_time,
  s.tag_id,
  s.tag_name,
  s.open_channel,
  s.group_id,
  s.group_name
FROM (
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
   AND c.`day` = CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2))
  INNER JOIN (
    SELECT DISTINCT external_id, tag_id, `day`
    FROM `hive_catalog`.`ods`.`o_scrm_customer_tag`
  ) ct
    ON ce.external_id = ct.external_id
   AND ct.`day` = CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2))
  INNER JOIN (
    SELECT tag_id, tag_name, group_id, `day`
    FROM `hive_catalog`.`ods`.`o_scrm_tag`
  ) t
    ON ct.tag_id = t.tag_id
   AND t.`day` = CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2))
  INNER JOIN (
    SELECT group_id, name AS group_name, `day`
    FROM `hive_catalog`.`ods`.`o_scrm_tag_group`
  ) g
    ON t.group_id = g.group_id
   AND g.`day` = CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2))
  INNER JOIN `portal_db`.`config_activity_channel_tag` ca
    ON t.tag_name = ca.wechat_customer_tag
   AND ca.open_channel IS NOT NULL
   AND TRIM(ca.open_channel) <> ''
  WHERE CAST(CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2)) AS DATE) = CAST('${biz_day}' AS DATE)
) s;


-- 查询视图（给业务接口用）：
CREATE VIEW IF NOT EXISTS `mv_scrm_tagged_customers_act_hist_view`
COMMENT "活动渠道销售员工加微标签历史视图（来源dwd_scrm_tagged_customers_act_hist）"
AS
SELECT
  external_id, name, user_id, user_name, add_dt, add_time,
  tag_id, tag_name, open_channel, group_id, group_name
FROM `dwd_scrm_tagged_customers_act_hist`;

