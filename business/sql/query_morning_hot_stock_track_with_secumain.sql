-- 早盘人气股战绩追踪：关联行情与证券主表（StarRocks）
--
-- 需求：
-- - config_morning_hot_stock_track.biz_date (YYYY-MM-DD) 关联 dailyquote.day (yyyymmdd)
-- - innercode/secucode/stock_code 按原始值关联（不做补零/格式化）
-- - 增加 ods.o_jy_jydbfk_secumain：取最大 day 那天，且 secucategory='1'
-- - 用 dailyquote.innercode -> secumain.innercode 映射到 secucode，再用 secucode 匹配 config.stock_code
-- - dailyquote 口径：o_jy_jydbfk_qt_dailyquote UNION o_jy_jydbfk_lc_stibdailyquote
-- - 取 biz_date 当天 openprice、以及后续“有数据日”的第 1 个/第 3 个日期 highprice；无则 NULL

WITH cfg AS (
    SELECT
        id,
        tg_name,
        biz_date,
        stock_name,
        stock_code,
        CAST(REPLACE(CAST(biz_date AS VARCHAR), '-', '') AS INT) AS biz_day_int
    FROM config_morning_hot_stock_track
),

secumain_latest AS (
    SELECT
        secucode,
        innercode,
        secuabbr
    FROM hive_catalog.ods.o_jy_jydbfk_secumain
    WHERE
      `day` = (
          SELECT MAX(`day`)
          FROM hive_catalog.ods.o_jy_jydbfk_secumain
          WHERE secucategory = '1'
      )
      AND (secucategory = '1' OR secuabbr = '512880')
),

q_union AS (
    SELECT
        `day`,
        innercode,
        CAST(openprice AS DECIMAL(18, 6)) AS openprice,
        CAST(highprice AS DECIMAL(18, 6)) AS highprice
    FROM hive_catalog.ods.o_jy_jydbfk_qt_dailyquote
    UNION ALL
    SELECT
        `day`,
        innercode,
        CAST(openprice AS DECIMAL(18, 6)) AS openprice,
        CAST(highprice AS DECIMAL(18, 6)) AS highprice
    FROM hive_catalog.ods.o_jy_jydbfk_lc_stibdailyquote
),

q_mapped AS (
    SELECT
        q.`day` AS q_day_int,   -- yyyymmdd
        s.secuabbr,
        s.secucode,
        s.innercode,
        q.openprice,
        q.highprice,
        LEAD(q.highprice, 1) OVER (
            PARTITION BY s.secucode
            ORDER BY q.`day`
        ) AS highprice_next_date,
        MAX(q.highprice) OVER (
            PARTITION BY s.secucode
            ORDER BY q.`day`
            ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
        ) AS highprice_t1_t3_max
    FROM q_union q
    JOIN secumain_latest s
      ON q.innercode = s.innercode
),

base AS (
    SELECT
        c.*,
        qm.openprice AS openprice_on_biz_date,
        qm.secuabbr,
        qm.innercode,
        qm.highprice_next_date,
        qm.highprice_t1_t3_max
    FROM cfg c
    LEFT JOIN q_mapped qm
      ON qm.q_day_int = c.biz_day_int
     AND qm.secucode = c.stock_code
)

SELECT
    b.id,
    b.tg_name,
    b.biz_date,
    b.stock_name,
    b.stock_code,
    b.innercode,
    b.secuabbr,

    b.openprice_on_biz_date AS openprice,

    b.highprice_next_date AS highprice_next_date,
    b.highprice_t1_t3_max AS highprice_third_next_date,

    CASE
        WHEN b.openprice_on_biz_date IS NULL OR b.openprice_on_biz_date = 0 OR b.highprice_next_date IS NULL THEN NULL
        ELSE CONCAT(CAST(ROUND(((b.highprice_next_date - b.openprice_on_biz_date) / b.openprice_on_biz_date) * 100, 2) AS VARCHAR), '%')
    END AS next_day_high_chg_pct,

    CASE
        WHEN b.openprice_on_biz_date IS NULL OR b.openprice_on_biz_date = 0 OR b.highprice_t1_t3_max IS NULL THEN NULL
        ELSE CONCAT(CAST(ROUND(((b.highprice_t1_t3_max - b.openprice_on_biz_date) / b.openprice_on_biz_date) * 100, 2) AS VARCHAR), '%')
    END AS t3_high_chg_pct
FROM base b
order by biz_date
;

