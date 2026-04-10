-- =============================================================================
-- 销售每日进线 · 月度转化率（复杂版 + 简易版视图）
-- =============================================================================
-- 依赖表：
--   - scrm_tagged_customers_act_hist（加微明细，add_time 为 yyyymmdd）
--   - mv_branch_customer_detail_final（订单宽表）
--   - branch_customer_ext_config（销售归属等，与宽表 sole_code/account/product 关联）
--
-- 口径说明：
--   - 渠道：活动 / 继承 / 共享；「导流」并入活动
--   - 订单侧过滤 channel 为空或纯空白的行
--   - 进线月份：从 in_month 文本中正则提取首个 yyyy-mm，与加微侧 month 对齐
--   - 加微数：按 add_time 所在自然月（yyyy-mm）汇总
--   - 订单数：按进线月份 in_month 提取的 yyyy-mm 汇总（与加微同月对齐算转化率）
--   - 销售归并键：剔除非汉字（近似应用层「只保留汉字」；若与线上一致性要求高可改为 UDF）
--
-- 使用：
--   SELECT * FROM v_sales_inflow_conversion_monthly_complex WHERE month = '2026-04';
--   SELECT * FROM v_sales_inflow_conversion_monthly_simple WHERE month = '2026-04';
-- =============================================================================
-- 维护提示：修改渠道口径、归并规则或关联键时，同步检查 add_base / ord_line 两处逻辑。
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS mv_sales_inflow_conversion_monthly_complex;
CREATE MATERIALIZED VIEW  `mv_sales_inflow_conversion_monthly_complex`
COMMENT '销售每日进线-月度转化率（复杂版）：按销售归并键与yyyy-mm汇总加微、升佣/现金订单分渠道及转化率；转化率列保留5位小数'
REFRESH ASYNC
    START ('2026-04-09 15:30:00')      -- 第一次刷新的时间（FE 时间）
    EVERY (INTERVAL 1 DAY)             -- 之后每 1 天刷新一次
AS
WITH
-- ---------------------------------------------------------------------------
-- add_base：加微侧按「销售归并键 × 自然月」汇总（月来自 add_time 前 6 位 yyyymm）
-- 汉字归并键：regexp 剔除非 CJK；若与线上一致性不足，可改为 UDF 或与 app.py 同口径
-- ---------------------------------------------------------------------------
add_base AS (
  SELECT
    NULLIF(
      TRIM(
        regexp_replace(
          CAST(`user_name` AS STRING),
          '[^\\x{4e00}-\\x{9fff}]',
          ''
        )
      ),
      ''
    ) AS sales_key,
    CONCAT(
      SUBSTRING(CAST(`add_time` AS STRING), 1, 4),
      '-',
      SUBSTRING(CAST(`add_time` AS STRING), 5, 2)
    ) AS `month`,
    SUM(
      CASE
        WHEN CAST(`open_channel` AS STRING) LIKE '%活动%'
          OR CAST(`open_channel` AS STRING) LIKE '%导流%'
        THEN 1 ELSE 0
      END
    ) AS activity_add_cnt,
    SUM(
      CASE WHEN CAST(`open_channel` AS STRING) LIKE '%继承%' THEN 1 ELSE 0 END
    ) AS inherit_add_cnt,
    SUM(
      CASE WHEN CAST(`open_channel` AS STRING) LIKE '%共享%' THEN 1 ELSE 0 END
    ) AS share_add_cnt
    ,
    SUM(
      CASE WHEN CAST(`open_channel` AS STRING) LIKE '%未开户%' THEN 1 ELSE 0 END
    ) AS unopened_add_cnt
  FROM `scrm_tagged_customers_act_hist`
  WHERE `add_time` IS NOT NULL
    AND LENGTH(CAST(`add_time` AS STRING)) >= 6
  GROUP BY 1, 2
  HAVING sales_key IS NOT NULL AND `month` IS NOT NULL
),
-- ---------------------------------------------------------------------------
-- add_agg：在 add_base 上增加 total_add_cnt（三类加微之和）
-- ---------------------------------------------------------------------------
add_agg AS (
  SELECT
    sales_key,
    `month`,
    activity_add_cnt,
    inherit_add_cnt,
    share_add_cnt,
    unopened_add_cnt,
    activity_add_cnt + inherit_add_cnt + share_add_cnt AS total_add_cnt
  FROM add_base
),
-- ---------------------------------------------------------------------------
-- add_override：历史加微覆盖（用于 2025-11 ~ 2026-03 校准）
-- ---------------------------------------------------------------------------
add_override AS (
  SELECT
    NULLIF(
      TRIM(
        regexp_replace(CAST(`sales_key` AS STRING), '[^\\x{4e00}-\\x{9fff}]', '')
      ),
      ''
    ) AS sales_key,
    CAST(`month` AS STRING) AS `month`,
    -- 同一 (month, sales_key) 可能同时存在“常规三渠道行”和“未开户行”，这里聚合成一行避免 join 产生重复
    MAX(COALESCE(`activity_add_cnt`, 0)) AS activity_add_cnt,
    MAX(COALESCE(`inherit_add_cnt`, 0)) AS inherit_add_cnt,
    MAX(COALESCE(`share_add_cnt`, 0)) AS share_add_cnt,
    MAX(COALESCE(`unopened_add_cnt`, 0)) AS unopened_add_cnt
  FROM `fact_sales_inflow_add_cnt_override`
  WHERE `month` >= '2025-11' AND `month` <= '2026-03'
  GROUP BY 1, 2
),
-- ---------------------------------------------------------------------------
-- add_effective_agg：历史区间(2025-11~2026-03)完全以覆盖表为准；其他月份仍用底表计算。
-- 说明：底表历史缺陷可能产生同一销售同月的“脏行”，因此历史区间不与底表做 merge。
-- ---------------------------------------------------------------------------
add_effective_agg AS (
  -- 非历史区间：正常走底表汇总
  SELECT
    sales_key,
    `month`,
    activity_add_cnt,
    inherit_add_cnt,
    share_add_cnt,
    unopened_add_cnt,
    total_add_cnt
  FROM add_agg
  WHERE `month` < '2025-11' OR `month` > '2026-03'

  UNION ALL

  -- 历史区间：以覆盖表为准（已在 add_override 聚合成一行）
  SELECT
    o.sales_key,
    o.`month`,
    o.activity_add_cnt,
    o.inherit_add_cnt,
    o.share_add_cnt,
    o.unopened_add_cnt,
    o.activity_add_cnt + o.inherit_add_cnt + o.share_add_cnt AS total_add_cnt
  FROM add_override o
),
-- ---------------------------------------------------------------------------
-- ord_line：订单明细行（已付有效单、渠道非空）；销售=配置表 sales_owner 优先，否则宽表 sales_name
-- ch_bucket：活动(含导流)/继承/共享；sign_type 含「升佣」「现金」分别打标
-- ---------------------------------------------------------------------------
ord_line AS (
  SELECT
    NULLIF(
      TRIM(
        regexp_replace(
          CAST(
            COALESCE(
              NULLIF(TRIM(CAST(c.`sales_owner` AS STRING)), ''),
              NULLIF(TRIM(CAST(t.`sales_name` AS STRING)), '')
            )
          AS STRING),
          '[^\\x{4e00}-\\x{9fff}]',
          ''
        )
      ),
      ''
    ) AS sales_key,
    COALESCE(
      -- 1) 形如 2026-03
      regexp_extract(TRIM(CAST(t.`in_month` AS STRING)), '([0-9]{4}-[0-9]{2})', 1),
      -- 2) 形如 2026年03月 / 2026年3月
      CONCAT(
        regexp_extract(TRIM(CAST(t.`in_month` AS STRING)), '([0-9]{4})\\s*年', 1),
        '-',
        LPAD(regexp_extract(TRIM(CAST(t.`in_month` AS STRING)), '年\\s*([0-9]{1,2})\\s*月', 1), 2, '0')
      ),
      -- 3) 形如 202603 / 202603xx
      CONCAT(
        SUBSTRING(TRIM(CAST(t.`in_month` AS STRING)), 1, 4),
        '-',
        SUBSTRING(TRIM(CAST(t.`in_month` AS STRING)), 5, 2)
      )
    ) AS `month`,
    CASE
      WHEN TRIM(CAST(t.`channel` AS STRING)) LIKE '%未开户%' THEN 'unopened'
      WHEN TRIM(CAST(t.`channel` AS STRING)) LIKE '%共享%' THEN 'share'
      WHEN TRIM(CAST(t.`channel` AS STRING)) LIKE '%继承%' THEN 'inherit'
      WHEN TRIM(CAST(t.`channel` AS STRING)) LIKE '%活动%'
        OR TRIM(CAST(t.`channel` AS STRING)) LIKE '%导流%'
        OR TRIM(CAST(t.`channel` AS STRING)) LIKE '%转介绍%'
        OR TRIM(CAST(t.`channel` AS STRING)) LIKE '%开户%'
        OR TRIM(CAST(t.`channel` AS STRING)) LIKE '%攻守道%'
      THEN 'activity'
      ELSE 'other'
    END AS ch_bucket,
    CASE WHEN CAST(t.`sign_type` AS STRING) LIKE '%升佣%' THEN 1 ELSE 0 END AS is_commission,
    CASE WHEN CAST(t.`sign_type` AS STRING) LIKE '%现金%' THEN 1 ELSE 0 END AS is_cash
  FROM `mv_branch_customer_detail_final` t
  LEFT JOIN `branch_customer_ext_config` c
    ON t.`sole_code` = c.`sole_code`
   AND t.`customer_account` = c.`customer_account`
   AND t.`product_name` = c.`product_name`
  WHERE t.`pay_amount` IS NOT NULL
    AND CAST(t.`pay_amount` AS STRING) != '0.00000'
    AND t.`pay_time` IS NOT NULL
    AND TRIM(CAST(t.`pay_time` AS STRING)) != ''
    AND t.`channel` IS NOT NULL
    AND TRIM(CAST(t.`channel` AS STRING)) != ''
),
-- ---------------------------------------------------------------------------
-- ord_bucket：按 销售键 × 进线月 × 渠道桶汇总升佣笔数、现金笔数、总订单笔数
-- ---------------------------------------------------------------------------
ord_bucket AS (
  SELECT
    sales_key,
    `month`,
    ch_bucket,
    SUM(is_commission) AS commission_cnt,
    SUM(is_cash) AS cash_cnt,
    COUNT(*) AS total_cnt
  FROM ord_line
  WHERE ch_bucket IN ('activity', 'inherit', 'share', 'unopened')
    AND `month` IS NOT NULL
    AND LENGTH(`month`) = 7
    AND sales_key IS NOT NULL
  GROUP BY 1, 2, 3
),
-- ---------------------------------------------------------------------------
-- ord_pivot：将三渠道桶展平为列（升佣/现金/总订单的分渠道与合计）
-- order_total_*：该渠道下订单总行数（升佣+现金+其他签约方式，以行计）
-- ---------------------------------------------------------------------------
ord_pivot AS (
  SELECT
    sales_key,
    `month`,
    SUM(CASE WHEN ch_bucket = 'activity' THEN commission_cnt ELSE 0 END) AS commission_order_activity,
    SUM(CASE WHEN ch_bucket = 'inherit' THEN commission_cnt ELSE 0 END) AS commission_order_inherit,
    SUM(CASE WHEN ch_bucket = 'share' THEN commission_cnt ELSE 0 END) AS commission_order_share,
    SUM(commission_cnt) AS commission_order_total,
    SUM(CASE WHEN ch_bucket = 'activity' THEN cash_cnt ELSE 0 END) AS cash_order_activity,
    SUM(CASE WHEN ch_bucket = 'inherit' THEN cash_cnt ELSE 0 END) AS cash_order_inherit,
    SUM(CASE WHEN ch_bucket = 'share' THEN cash_cnt ELSE 0 END) AS cash_order_share,
    SUM(cash_cnt) AS cash_order_total,
    SUM(CASE WHEN ch_bucket = 'activity' THEN total_cnt ELSE 0 END) AS order_total_activity,
    SUM(CASE WHEN ch_bucket = 'inherit' THEN total_cnt ELSE 0 END) AS order_total_inherit,
    SUM(CASE WHEN ch_bucket = 'share' THEN total_cnt ELSE 0 END) AS order_total_share,
    SUM(CASE WHEN ch_bucket = 'unopened' THEN total_cnt ELSE 0 END) AS order_total_unopened,
    SUM(total_cnt) AS total_order_cnt
  FROM ord_bucket
  GROUP BY 1, 2
),
-- ---------------------------------------------------------------------------
-- all_keys：加微与订单两侧出现过的 (销售键, 月) 全集，避免只存在一侧时丢行
-- ---------------------------------------------------------------------------
all_keys AS (
  SELECT sales_key, `month` FROM add_effective_agg
  UNION
  SELECT sales_key, `month` FROM ord_pivot
)
-- ---------------------------------------------------------------------------
-- 最终输出（复杂版一行 = 某销售在某「对齐月」下的汇总）
--  对齐月：加微按 add_time 自然月；订单按 in_month 提取的 yyyy-mm，用于算转化率
-- ---------------------------------------------------------------------------
SELECT
  k.sales_key AS sales_name, -- 销售（汉字归并键，与输出列名 sales_name 一致）
  k.`month`, -- 月份 yyyy-mm；筛选示例：WHERE `month` = '2026-04'
  COALESCE(a.total_add_cnt, 0) AS total_add_cnt, -- 总加微数
  COALESCE(a.activity_add_cnt, 0) AS activity_add_cnt, -- 活动加微（含导流）
  COALESCE(a.inherit_add_cnt, 0) AS inherit_add_cnt, -- 继承加微
  COALESCE(a.share_add_cnt, 0) AS share_add_cnt, -- 共享加微
  COALESCE(a.unopened_add_cnt, 0) AS unopened_add_cnt, -- 未开户加微
  COALESCE(o.commission_order_total, 0) AS commission_order_total, -- 升佣总订单数
  COALESCE(o.commission_order_activity, 0) AS commission_order_activity, -- 升佣·活动
  COALESCE(o.commission_order_inherit, 0) AS commission_order_inherit, -- 升佣·继承
  COALESCE(o.commission_order_share, 0) AS commission_order_share, -- 升佣·共享
  COALESCE(o.cash_order_total, 0) AS cash_order_total, -- 现金总订单数
  COALESCE(o.cash_order_activity, 0) AS cash_order_activity, -- 现金·活动
  COALESCE(o.cash_order_inherit, 0) AS cash_order_inherit, -- 现金·继承
  COALESCE(o.cash_order_share, 0) AS cash_order_share, -- 现金·共享
  COALESCE(o.total_order_cnt, 0) AS total_order_cnt, -- 总订单数（三渠道笔数合计）
  COALESCE(o.order_total_activity, 0) AS order_total_activity, -- 分渠道总订单数·活动
  COALESCE(o.order_total_inherit, 0) AS order_total_inherit, -- 分渠道总订单数·继承
  COALESCE(o.order_total_share, 0) AS order_total_share, -- 分渠道总订单数·共享
  COALESCE(o.order_total_unopened, 0) AS order_total_unopened, -- 分渠道总订单数·未开户
  ROUND(
    CAST(
      CASE
        WHEN COALESCE(a.activity_add_cnt, 0) > 0
        THEN COALESCE(o.order_total_activity, 0) / CAST(a.activity_add_cnt AS DOUBLE)
        ELSE NULL
      END AS DOUBLE
    ),
    5
  ) AS conversion_rate_activity, -- 转化率（活动）= order_total_activity / activity_add_cnt
  ROUND(
    CAST(
      CASE
        WHEN COALESCE(a.inherit_add_cnt, 0) > 0
        THEN COALESCE(o.order_total_inherit, 0) / CAST(a.inherit_add_cnt AS DOUBLE)
        ELSE NULL
      END AS DOUBLE
    ),
    5
  ) AS conversion_rate_inherit, -- 转化率（继承）
  ROUND(
    CAST(
      CASE
        WHEN COALESCE(a.share_add_cnt, 0) > 0
        THEN COALESCE(o.order_total_share, 0) / CAST(a.share_add_cnt AS DOUBLE)
        ELSE NULL
      END AS DOUBLE
    ),
    5
  ) AS conversion_rate_share, -- 转化率（共享）
  ROUND(
    CAST(
      CASE
        WHEN COALESCE(a.unopened_add_cnt, 0) > 0
        THEN COALESCE(o.order_total_unopened, 0) / CAST(a.unopened_add_cnt AS DOUBLE)
        ELSE NULL
      END AS DOUBLE
    ),
    5
  ) AS conversion_rate_unopened, -- 转化率（未开户）
  ROUND(
    CAST(
      CASE
        WHEN COALESCE(a.total_add_cnt, 0) > 0
        THEN COALESCE(o.total_order_cnt, 0) / CAST(a.total_add_cnt AS DOUBLE)
        ELSE NULL
      END AS DOUBLE
    ),
    5
  ) AS conversion_rate_total -- 总计转化率 = total_order_cnt / total_add_cnt
FROM all_keys k
LEFT JOIN add_effective_agg a -- 左连接：仅有订单无加微时加微列为 0
  ON k.sales_key = a.sales_key AND k.`month` = a.`month`
LEFT JOIN ord_pivot o -- 左连接：仅有加微无订单时订单列为 0
  ON k.sales_key = o.sales_key AND k.`month` = o.`month`
WHERE k.sales_key IS NOT NULL AND k.`month` IS NOT NULL; -- 排除无效键



