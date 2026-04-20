delete from portal_db.scrm_tagged_customers_act_hist where add_time = '${system.biz.date}';
INSERT INTO portal_db.scrm_tagged_customers_act_hist
WITH ca AS (
  SELECT
    TRIM(wechat_customer_tag) AS wechat_customer_tag,
    MIN(open_channel) AS open_channel
  FROM `portal_db`.`config_activity_channel_tag`
  WHERE open_channel IS NOT NULL
    AND TRIM(open_channel) <> ''
  GROUP BY TRIM(wechat_customer_tag)
),
cs AS (
  SELECT DISTINCT
    TRIM(staff_name) AS staff_name
  FROM `portal_db`.`config_channel_staff`
  WHERE channel_type = '活动渠道'
),
t AS (
  SELECT
    tag_id,
    tag_name,
    group_id,
    create_time
  FROM (
    SELECT
      tag_id,
      tag_name,
      group_id,
      create_time,
      ROW_NUMBER() OVER (
        PARTITION BY tag_id
        ORDER BY CAST(create_time AS BIGINT) DESC, tag_name DESC
      ) AS rn
    FROM `hive_catalog`.`ods`.`o_scrm_tag`
    WHERE `day` = '${system.biz.date}'
  ) ranked_t
  WHERE rn = 1
),
t_cfg AS (
  SELECT
    t.tag_id,
    t.tag_name,
    t.group_id,
    ca.open_channel
  FROM ca
  INNER JOIN t
    ON ca.wechat_customer_tag = TRIM(t.tag_name)
),
ct AS (
  SELECT
    external_id,
    user_id,
    tag_id
  FROM (
    SELECT
      ct.external_id,
      CAST(ct.user_id AS STRING) AS user_id,
      ct.tag_id,
      ROW_NUMBER() OVER (
        PARTITION BY ct.external_id, CAST(ct.user_id AS STRING), ct.tag_id
        ORDER BY CAST(COALESCE(NULLIF(ct.update_time, ''), ct.create_time, '0') AS BIGINT) DESC,
                 CAST(COALESCE(ct.id, 0) AS BIGINT) DESC
      ) AS rn
    FROM `hive_catalog`.`ods`.`o_scrm_customer_tag` ct
    INNER JOIN t
      ON ct.tag_id = t.tag_id
    WHERE ct.`day` = '${system.biz.date}'
  ) ranked_ct
  WHERE rn = 1
),
g AS (
  SELECT
    group_id,
    group_name
  FROM (
    SELECT
      group_id,
      name AS group_name,
      ROW_NUMBER() OVER (
        PARTITION BY group_id
        ORDER BY name DESC
      ) AS rn
    FROM `hive_catalog`.`ods`.`o_scrm_tag_group`
    WHERE `day` = '${system.biz.date}'
  ) ranked_g
  WHERE rn = 1
),
ce AS (
  SELECT
    external_id,
    user_id,
    user_name,
    add_time_ymd
  FROM (
    SELECT
      ce.external_id,
      CAST(ce.user_id AS STRING) AS user_id,
      ce.user_name,
      DATE_FORMAT(FROM_UNIXTIME(CAST(ce.add_time AS BIGINT)), '%Y%m%d') AS add_time_ymd,
      ROW_NUMBER() OVER (
        PARTITION BY ce.external_id
        ORDER BY CAST(ce.add_time AS BIGINT) DESC, CAST(ce.user_id AS STRING) DESC
      ) AS rn
    FROM `hive_catalog`.`ods`.`o_scrm_customer_employee` ce
    INNER JOIN cs
      ON TRIM(ce.user_name) = cs.staff_name
    WHERE `day` = '${system.biz.date}'
      AND ce.add_time IS NOT NULL
      AND ce.add_time != ''
      AND DATE_FORMAT(FROM_UNIXTIME(CAST(ce.add_time AS BIGINT)), '%Y%m%d') = '${system.biz.date}'
  ) ranked_ce
  WHERE rn = 1
),
c AS (
  SELECT DISTINCT
    external_id,
    name
  FROM `hive_catalog`.`ods`.`o_scrm_customer`
  WHERE `day` = '${system.biz.date}'
)
SELECT DISTINCT
  ce.external_id,
  c.name,
  ce.user_id,
  ce.user_name,
  CAST(CONCAT(SUBSTRING(ce.add_time_ymd, 1, 4), '-', SUBSTRING(ce.add_time_ymd, 5, 2), '-', SUBSTRING(ce.add_time_ymd, 7, 2)) AS DATE) AS add_dt,
  ce.add_time_ymd AS add_time,
  t_cfg.tag_id,
  t_cfg.tag_name,
  t_cfg.open_channel,
  t_cfg.group_id,
  g.group_name
FROM ce
LEFT JOIN ct
  ON ce.external_id = ct.external_id
 AND ce.user_id = ct.user_id
INNER JOIN t_cfg
  ON ct.tag_id = t_cfg.tag_id
LEFT JOIN g
  ON t_cfg.group_id = g.group_id
LEFT JOIN c
  ON ce.external_id = c.external_id;