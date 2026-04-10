
impala-shell  -q "
invalidate metadata dwd.d_uf20_customer_info_di ;
invalidate metadata dwd.d_uf20_customer_curr_profit_shid_di   ;
invalidate metadata dwd.d_uf20_customer_trade_df;
invalidate metadata ods.o_crm_ecif_tkhzhsj;
invalidate metadata ods.o_crm_cis_tryxx ;
invalidate metadata dwd.d_sd_open_account_info_df;
invalidate metadata ods.o_uf20_hs_user_bfare2 ;
invalidate metadata ods.o_uf20_hs_asset_fundaccount;
invalidate metadata dwd.d_uf20_customer_asset_df;
invalidate metadata ods.o_uf20_hs_asset_stockholder;
invalidate metadata ods.o_uf20_hs_asset_secumholder;
invalidate metadata ods.o_tg_advisor_order_info;
invalidate metadata ods.o_sd_thk_fxckhdata_t_stkkh_channel_info;
invalidate metadata ods.o_sd_thk_fxckhdata_t_stkkh_tiktok_data_return;
invalidate metadata ads.a_scrm_customer_tags;
invalidate metadata ods.o_uf20_hs_user_stbfare2;
invalidate metadata ods.o_uf20_hs_user_offare2;

WITH customer_info AS (
  SELECT 
    fund_account,
    client_id,
    name AS cust_name,
    phone as phone_number,
    branch_no as branch_no,
    branch_no_name AS branch_name,
    open_date,
    is_rzrq AS has_margin_account,
    is_star_market AS has_star_market_access,
    is_gem_board AS has_gem_access,
    is_bse AS has_bsex_access,
    is_stock_connect AS has_stock_connect_access,
    is_qib AS is_qualified_private_fund_investor,
    is_am AS is_qualified_asset_mgmt_investor
  FROM dwd.d_uf20_customer_info_di 
  WHERE day = '${day}'
    AND asset_prop = '0'
    AND branch_no NOT IN ('5200','9901','9902','9909')
),

customer_asset AS (
  SELECT 
    client_id,
    curr_total_asset AS asset,
    is_effective_account_c,
    avg_20_asset AS avg_daily_asset,
    curr_fund_asset,
    pvg_zqsz
  FROM dwd.d_uf20_customer_curr_profit_shid_di 
  WHERE day = '${day}'
),

account_classification AS (  -- 修改后的CTE名称
  SELECT 
    client_id,
    CASE   
      WHEN total_balance = 0 THEN '无标识'
      WHEN fin_ratio >= 0.9 THEN '理财产品户'
      WHEN stk_ratio >= 0.9 THEN '股票交易户'
      WHEN stock_balance > fin_balance THEN '偏股户'
      ELSE '偏理财户'
    END AS fin_or_stk_account
  FROM (
    SELECT 
      client_id,
      stock_balance,
      fin_balance,
      total_balance,
      CASE WHEN total_balance > 0 THEN fin_balance / total_balance ELSE 0 END AS fin_ratio,
      CASE WHEN total_balance > 0 THEN stock_balance / total_balance ELSE 0 END AS stk_ratio
    FROM (
      SELECT 
        client_id,
        COALESCE(ag_shares_bus_balance, 0) 
        + COALESCE(hb_shares_bus_balance, 0)
        + COALESCE(shb_shares_bus_balance, 0)
        + COALESCE(ggt_balance, 0)
        + COALESCE(bjs_balance, 0) AS stock_balance,
        COALESCE(jj_sale, 0)
        + COALESCE(sypz_sale, 0)
        + COALESCE(sm_sale, 0)
        + COALESCE(zg_sale, 0) AS fin_balance,
        COALESCE(ag_shares_bus_balance, 0) 
        + COALESCE(hb_shares_bus_balance, 0)
        + COALESCE(shb_shares_bus_balance, 0)
        + COALESCE(jj_balance, 0)
        + COALESCE(zq_balance, 0)
        + COALESCE(ggt_balance, 0)
        + COALESCE(bjs_balance, 0)
        + COALESCE(nhg_balance, 0)
        + COALESCE(jj_sale, 0)
        + COALESCE(sypz_sale, 0)
        + COALESCE(sm_sale, 0)
        + COALESCE(zg_sale, 0) AS total_balance
      FROM dwd.d_uf20_customer_trade_df
      WHERE day = '${day}'
    ) balance_calc
  ) ratio_calc
),

brokers AS (
  SELECT id
  FROM hive_catalog.ods.o_crm_cis_tryxx
  WHERE day = '${day}'
    AND ryfl = '4542' -- 经纪人
),

financial_manager AS (
  SELECT 
    e.khh AS client_id,
    COALESCE(
      NULLIF(e.FWRYMC, '无对应人员'), 
      CASE
        WHEN b.id IS NULL THEN NULLIF(e.KHJLMC, '无对应人员')
        ELSE NULL
      END,
      oa.recommend_name
    ) AS financial_manager,
    COALESCE(
      fw.zjbh, 
      CASE
        WHEN b.id IS NULL THEN kh.zjbh
        ELSE NULL
      END,
      oa.recommend_card
    ) AS financial_manager_id,
    NULLIF(e.FWRYMC, '无对应人员') AS srv_rel,
    fw.zjbh AS srv_rel_id,
    CASE
      WHEN b.id IS NULL THEN NULLIF(e.KHJLMC, '无对应人员')
      ELSE NULL
    END AS dev_rel,
    CASE
      WHEN b.id IS NULL THEN kh.zjbh
      ELSE NULL
    END AS dev_rel_id,
    oa.recommend_name,  -- 添加推荐人姓名
    oa.recommend_card   -- 添加推荐人身份证号
  FROM ods.o_crm_ecif_tkhzhsj e
  LEFT JOIN ods.o_crm_cis_tryxx fw 
    ON fw.day = '${day}' 
    AND fw.id = e.fwry 
  LEFT JOIN ods.o_crm_cis_tryxx kh 
    ON kh.day = '${day}' 
    AND kh.id = e.khjl 
  LEFT JOIN brokers b
    ON b.id = e.khjl
  LEFT JOIN dwd.d_sd_open_account_info_df oa 
    ON oa.day = '${day}' 
    AND oa.fund_account = e.khh
  WHERE e.day = '${day}'
),

channel_name AS (
  SELECT 
    fund_account,
    system_channel_name AS channel_name
  FROM dwd.d_sd_open_account_info_df
  WHERE day = '${day}'
),

-- commission_rate AS (
--   SELECT 
--     fund_account,
--     commission_rate_general
--   FROM (
--     SELECT 
--       b.FUND_ACCOUNT AS fund_account,
--       CAST(a.BALANCE_RATIO AS DECIMAL(20,10)) AS commission_rate_general,
--       ROW_NUMBER() OVER (PARTITION BY b.FUND_ACCOUNT ORDER BY a.BALANCE_RATIO desc) AS rn
--     FROM (
--       SELECT 
--         balance_ratio,
--         fare_kind
--       FROM ods.o_uf20_hs_user_bfare2 
--       WHERE day = '${day}'
--         AND fare_type = '0'
--         AND exchange_type IN ('1','2')
--         AND stock_type = '0'
--         AND entrust_bs IN ('1','2')
--         AND entrust_type = '!'
--         AND entrust_way = '!'
--         AND CAST(balance_ratio AS DECIMAL(20,10)) <= 0.0001354
--     ) a
--     INNER JOIN (
--       SELECT 
--         SUBSTR(FARE_KIND_STR, 5, 4) AS FARE_KIND,
--         FUND_ACCOUNT
--       FROM ods.o_uf20_hs_asset_fundaccount
--       WHERE day = '${day}'
--         AND ASSET_PROP = '0'
--     ) b ON a.FARE_KIND = b.FARE_KIND
--   ) t
--   WHERE rn = 1
-- ),

commission_rate AS (
  SELECT 
    fund_account,
    commission_rate_general
  FROM (
    SELECT 
      b.FUND_ACCOUNT AS fund_account,
      CAST(a.BALANCE_RATIO AS DECIMAL(20,10)) AS commission_rate_general
    FROM (
      SELECT 
        balance_ratio,
        fare_kind
      FROM ods.o_uf20_hs_user_bfare2 
      WHERE day = '${day}'
        AND fare_type = '0'
        -- AND exchange_type IN ('1','2')
        AND exchange_type IN ('1')
        AND stock_type = '0'
        AND entrust_bs IN ('1','2')
        AND entrust_type = '!'
        AND entrust_way = '!'
        -- AND CAST(balance_ratio AS DECIMAL(20,10)) <= 0.0001354
    ) a
    INNER JOIN (
      SELECT 
        SUBSTR(FARE_KIND_STR, 5, 4) AS FARE_KIND,
        FUND_ACCOUNT
      FROM ods.o_uf20_hs_asset_fundaccount
      WHERE day = '${day}'
        AND ASSET_PROP = '0'
    ) b ON a.FARE_KIND = b.FARE_KIND
  ) t
),

total_debit AS (
  SELECT 
    client_id,
    total_debit
  FROM dwd.d_uf20_customer_asset_df
  WHERE day = '${day}'
),

account_flags AS (
  SELECT 
    s.fund_account,
    MAX(CASE WHEN s.holder_rights LIKE '%z%' THEN 1 ELSE 0 END) AS sh_hk_enabled,
    MAX(CASE WHEN s.holder_rights LIKE '%c%' THEN 1 ELSE 0 END) AS sz_hk_enabled,
    MAX(CASE WHEN s.holder_rights LIKE '%{%' THEN 1 ELSE 0 END) AS cb_trading_enabled,
    MAX(CASE WHEN sc.secum_account IS NOT NULL THEN 1 ELSE 0 END) AS wma_active
  FROM ods.o_uf20_hs_asset_stockholder s
  LEFT JOIN (
    SELECT DISTINCT fund_account, secum_account 
    FROM ods.o_uf20_hs_asset_secumholder
    WHERE day = '${day}'
  ) sc ON s.fund_account = sc.fund_account
  WHERE s.day = '${day}'
  GROUP BY s.fund_account
),

service_pkg AS (
  SELECT 
    customer_account AS fund_account,
    MAX(CASE WHEN date_add(
             to_date(pay_time), 
             cast(service_day as int)
           ) >= to_date('${day}') THEN 1 ELSE 0 END) AS is_currently_subscribed_service_pkg,
    MAX(CASE WHEN date_add(
             to_date(pay_time), 
             cast(service_day as int)
           ) >= date_sub(to_date('${day}'), 180) THEN 1 ELSE 0 END) AS has_historically_subscribed_service_pkg
  FROM ods.o_tg_advisor_order_info
  WHERE status = '3'
    AND cancel_status = '0'
    AND del_flag = '0'
  GROUP BY customer_account
),

trade_volume AS (
  SELECT
    client_id,
    SUM(
      COALESCE(ag_shares_bus_balance, 0) + 
      COALESCE(hb_shares_bus_balance, 0) + 
      COALESCE(shb_shares_bus_balance, 0) + 
      COALESCE(jj_balance, 0) + 
      COALESCE(zq_balance, 0) + 
      COALESCE(ggt_balance, 0) + 
      COALESCE(bjs_balance, 0) + 
      COALESCE(nhg_balance, 0)
    ) AS curr_buss_balance_month
  FROM dwd.d_uf20_customer_trade_df
  WHERE day BETWEEN 
        from_unixtime(
          unix_timestamp('${day}', 'yyyyMMdd') - 29*24*60*60, 
          'yyyyMMdd'
        ) 
        AND '${day}'
  GROUP BY client_id
),

net_commission AS (
  SELECT
    client_id,
    SUM(CASE WHEN substr(day, 1, 6) = substr('${day}', 1, 6) 
             THEN COALESCE(fare, 0) ELSE 0 END) AS fare_curr,
    SUM(COALESCE(fare, 0)) AS fare_all
  FROM dwd.d_uf20_customer_curr_profit_shid_di
  WHERE day <= '${day}'
  GROUP BY client_id
),

channel_type AS (
  WITH customer_base AS (
    SELECT 
      fund_account,
      open_date,
      branch_no
    FROM dwd.d_uf20_customer_info_di 
    WHERE day = '${day}'
      AND asset_prop = '0'
      AND branch_no NOT IN ('5200', '9901', '9902', '9909')
  ),
  online_channels AS (
    SELECT 
      channel_name,
      channel_type
    FROM ods.o_sd_thk_fxckhdata_t_stkkh_channel_info
    WHERE day = '${day}'
      AND is_online = '1'
  ),
  account_channels AS (
    SELECT 
      oa.fund_account,
      CASE 
        WHEN oc.channel_type = '7' THEN '数金自营渠道'
        ELSE '数金非自营渠道'
      END AS channel_type
    FROM dwd.d_sd_open_account_info_df oa
    JOIN online_channels oc 
      ON oa.system_channel_name = oc.channel_name
    WHERE oa.day = '${day}'
      AND oa.open_day >= '20240101'
      AND oa.fund_account IS NOT NULL
  )
  SELECT 
    cb.fund_account,
    CASE
      WHEN cb.open_date < '20240101' THEN '线下营业部'
      WHEN cb.open_date >= '20240101' THEN
        CASE
          WHEN ac.channel_type = '数金自营渠道' THEN '数金自营渠道'
          WHEN ac.channel_type = '数金非自营渠道' THEN '数金非自营渠道'
          ELSE '线下营业部'
        END
    END AS channel_type
  FROM customer_base cb
  LEFT JOIN account_channels ac
    ON cb.fund_account = ac.fund_account
)


, 

unionid as-----unionid
(
	select 
		b.fund_account	 fund_account
		,GROUP_CONCAT(a.unionid,',')  unionid
	from ods.o_sd_thk_fxckhdata_t_stkkh_tiktok_data_return a
	join dwd.d_sd_open_account_info_df b
	on a.day='${day}'  and  b.day='${day}'   and a.user_id=b.user_id
	group by b.fund_account
)




,

-- 基金账户信息
fund_account_info AS (
    SELECT
        client_id,
        fund_account,
        SUBSTRING(fare_kind_str, 5, 4) AS gp_fare_kind,     -- 5-8位:股票费用类型
        SUBSTRING(fare_kind_str, 13, 4) AS etf_fare_kind,  -- 13-16位:标准场内基金交易费用类型
        SUBSTRING(fare_kind_str, 62, 4) AS hk_fare_kind,   -- 62-65位:港股费用类型
        SUBSTRING(fare_kind_str, 66, 4) AS st_fare_kind    -- 66-69位:全国股转费用类型
    FROM ods.o_uf20_hs_asset_fundaccount
    WHERE day = '${day}'
        AND asset_prop = '0'
),

-- ETF佣金费率
etf_fare AS (
    SELECT
        fare_kind,
        balance_ratio AS offare_ratio
    FROM (
        SELECT
            fare_kind,
            balance_ratio,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_offare2
        WHERE day = '${day}'
            AND stock_type = 'T'
            AND exchange_type = '1'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
),

-- 港股通佣金费率
hk_fare AS (
    SELECT
        fare_kind,
        balance_ratio AS hkfare_ratio
    FROM (
        SELECT
            fare_kind,
            balance_ratio,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_bfare2
        WHERE day = '${day}'
            AND stock_type IN ('0', 'T')
            AND exchange_type = 'G'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
),

-- 北交所佣金费率
st_fare AS (
    SELECT
        fare_kind,
        balance_ratio AS stfare_ratio
    FROM (
        SELECT
            fare_kind,
            balance_ratio,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_stbfare2
        WHERE day = '${day}'
            AND stock_type = 'z'
            AND sub_stock_type = 'z3'
            AND exchange_type = '9'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
),

-- 股票是否免五
 gp_minfare_free AS (
    SELECT 
        fare_kind,
        CASE WHEN min_fare = '5' THEN '否' ELSE '是' END AS gp_minfare_free
    FROM (
        SELECT 
            fare_kind,
            min_fare,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_bfare2
        WHERE day = '${day}'
            AND fare_type = '0' -- 前台的收费类别
            AND stock_type = '0'
            AND exchange_type = '1'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
),

-- ETF是否免五
 etf_minfare_free AS (
    SELECT 
        fare_kind,
        CASE WHEN min_fare = '5' THEN '否' ELSE '是' END AS etf_minfare_free
    FROM (
        SELECT 
            fare_kind,
            min_fare,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_offare2
        WHERE day = '${day}'
            AND fare_type = '0' -- 前台的收费类别
            AND stock_type = 'T'
            AND exchange_type = '1'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
),

-- 港股通是否免五
 ggt_minfare_free AS (
    SELECT 
        fare_kind,
        CASE WHEN min_fare = '5' THEN '否' ELSE '是' END AS ggt_minfare_free
    FROM (
        SELECT 
            fare_kind,
            min_fare,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_bfare2
        WHERE day = '${day}'
            AND fare_type = '0' -- 前台的收费类别
            AND stock_type IN ('0', 'T')
            AND exchange_type = 'G'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
),

-- 北交所是否免五
 bjs_minfare_free AS (
    SELECT 
        fare_kind,
        CASE WHEN min_fare = '5' THEN '否' ELSE '是' END AS bjs_minfare_free
    FROM (
        SELECT 
            fare_kind,
            min_fare,
            ROW_NUMBER() OVER (PARTITION BY fare_kind ORDER BY position_str_d DESC) AS rk
        FROM ods.o_uf20_hs_user_stbfare2
        WHERE day = '${day}'
            AND fare_type = '0' -- 前台的收费类别
            AND stock_type = 'z'
            AND sub_stock_type = 'z3'
            AND exchange_type = '9'
            AND entrust_bs = '1'
    ) t
    WHERE rk = 1
)










insert overwrite table ads.a_scrm_customer_tags partition (day='${day}')
SELECT distinct
  ci.cust_name as cust_name,
  ci.fund_account as fund_account,
  ci.phone_number as phone_number,
  cast(ca.asset as decimal(16,2)) as asset,
  ac.fin_or_stk_account as fin_or_stk_account,  -- 使用修改后的别名ac
  fm.financial_manager as financial_manager,
  fm.financial_manager_id as financial_manager_id,
  fm.srv_rel as srv_rel,
  fm.srv_rel_id as srv_rel_manager_id,
  fm.dev_rel as dev_rel,
  fm.dev_rel_id as dev_rel_manager_id,
  fm.recommend_name as rmd_rel,
  fm.recommend_card as rmd_rel_manager_id,
  ci.branch_no as branch_no,
  ci.branch_name as branch_name,
  cn.channel_name as channel_name,
  cast(cr.commission_rate_general as decimal(20,10)) as commission_rate_general,
  cast(ca.is_effective_account_c as string) as is_effective_account_c,
  cast(sp.is_currently_subscribed_service_pkg as string) as is_currently_subscribed_service_pkg,
  cast(sp.has_historically_subscribed_service_pkg as string) as has_historically_subscribed_service_pkg,
  cast(ca.avg_daily_asset as decimal(16,2)) as avg_daily_asset,
  cast(tv.curr_buss_balance_month as decimal(16,2)) as curr_buss_balance_month,
  cast(td.total_debit as decimal(16,2)) as total_debit,
  cast(ca.curr_fund_asset as decimal(16,2)) as curr_fund_asset,
  cast(nc.fare_curr as decimal(16,2)) as fare_curr,
  cast(nc.fare_all as decimal(16,2)) as fare_all,
  ci.has_margin_account as has_margin_account,
  ci.has_star_market_access as has_star_market_access,
  ci.has_gem_access as has_gem_access,
  ci.has_bsex_access as has_bsex_access,
  ci.has_stock_connect_access as has_stock_connect_access,
  cast(af.sh_hk_enabled as string) as sh_hk_enabled,
  cast(af.sz_hk_enabled as string) as sz_hk_enabled,
  cast(af.cb_trading_enabled as string) as cb_trading_enabled,
  cast(af.wma_active as string) as wma_active,
  cast(ci.is_qualified_private_fund_investor as string) as is_qualified_private_fund_investor,
  cast(ci.is_qualified_asset_mgmt_investor as string) as is_qualified_asset_mgmt_investor,
  ci.open_date,
  cast(ca.pvg_zqsz as decimal(16,2)) as pvg_zqsz,
  ct.channel_type,
  ud.unionid,
  -- null as unionid,
  cast(ef.offare_ratio as decimal(20,10)) AS offare_ratio,
  cast(hf.hkfare_ratio as decimal(20,10)) AS hkfare_ratio,
  cast(sf.stfare_ratio as decimal(20,10)) AS stfare_ratio,
  
   COALESCE(gp.gp_minfare_free, '否') AS gp_minfare_free,
   COALESCE(etf.etf_minfare_free, '否') AS etf_minfare_free,
   COALESCE(ggt.ggt_minfare_free, '否') AS ggt_minfare_free,
   COALESCE(bjs.bjs_minfare_free, '否') AS bjs_minfare_free
FROM customer_info ci
LEFT JOIN customer_asset ca ON ci.client_id = ca.client_id
LEFT JOIN account_classification ac ON ci.client_id = ac.client_id  -- 使用修改后的别名
LEFT JOIN financial_manager fm ON ci.client_id = fm.client_id
LEFT JOIN channel_name cn ON ci.fund_account = cn.fund_account
LEFT JOIN commission_rate cr ON ci.fund_account = cr.fund_account
LEFT JOIN total_debit td ON ci.client_id = td.client_id
LEFT JOIN account_flags af ON ci.fund_account = af.fund_account
LEFT JOIN service_pkg sp ON ci.fund_account = sp.fund_account
LEFT JOIN trade_volume tv ON ci.client_id = tv.client_id
LEFT JOIN net_commission nc ON ci.client_id = nc.client_id
LEFT JOIN channel_type ct ON ci.fund_account = ct.fund_account

LEFT JOIN unionid ud ON ci.fund_account = ud.fund_account

LEFT JOIN fund_account_info fai ON ci.fund_account = fai.fund_account
LEFT JOIN etf_fare ef ON fai.etf_fare_kind = ef.fare_kind
LEFT JOIN hk_fare hf ON fai.hk_fare_kind = hf.fare_kind
LEFT JOIN st_fare sf ON fai.st_fare_kind = sf.fare_kind

LEFT JOIN gp_minfare_free gp ON fai.gp_fare_kind = gp.fare_kind
LEFT JOIN etf_minfare_free etf ON fai.etf_fare_kind = etf.fare_kind
LEFT JOIN ggt_minfare_free ggt ON fai.hk_fare_kind = ggt.fare_kind
LEFT JOIN bjs_minfare_free bjs ON fai.st_fare_kind = bjs.fare_kind
;
"

