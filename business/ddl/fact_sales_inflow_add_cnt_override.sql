-- =============================================================================
-- 销售每日进线 · 月度加微覆盖表（用于历史数据校准）
-- =============================================================================
-- 背景：
-- 2025-11 ~ 2026-03 加微底表存在缺陷且不可追溯，用人工提供的月度汇总数据覆盖分母，
-- 以保证转化率计算准确；其他月份仍以 scrm_tagged_customers_act_hist 计算为准。
--
-- 字段说明：
-- - sales_key：销售归并键（建议仅汉字；与 mv_sales_inflow_conversion_monthly_complex 同口径）
-- - month：yyyy-mm
-- - activity_add_cnt：活动/导流加微数
-- - inherit_add_cnt：继承加微数
-- - share_add_cnt：共享加微数
-- - unopened_add_cnt：未开户加微数（2026-03 起有；之前可为 0）
-- =============================================================================

CREATE TABLE IF NOT EXISTS `fact_sales_inflow_add_cnt_override`
COMMENT '销售每日进线-月度加微覆盖表：用于校准 2025-11~2026-03 历史加微分母（其余月份仍按明细表计算）'
(
  `month` VARCHAR(7) NOT NULL COMMENT 'yyyy-mm',
  `sales_key` VARCHAR(64) NOT NULL COMMENT '销售归并键（建议仅汉字）',
  `activity_add_cnt` BIGINT NOT NULL DEFAULT '0',
  `inherit_add_cnt` BIGINT NOT NULL DEFAULT '0',
  `share_add_cnt` BIGINT NOT NULL DEFAULT '0',
  `unopened_add_cnt` BIGINT NOT NULL DEFAULT '0',
  `updated_at` DATETIME NULL COMMENT '可选：更新标记时间'
)
ENGINE=OLAP
DUPLICATE KEY(`month`, `sales_key`)
DISTRIBUTED BY HASH(`month`, `sales_key`) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

