CREATE TABLE `ads.a_tg_branch_customer_detail`(	
  `sole_code` string COMMENT '唯一代码', 	
  `customer_name` string COMMENT '客户姓名', 	
  `customer_account` string COMMENT '客户账号', 	
  `customer_phone` string COMMENT '客户电话', 	
  `customer_risk_level` string COMMENT '客户风险等级', 	
  `product_name` string COMMENT '产品名称', 	
  `product_type` string COMMENT '产品类型', 	
  `advisor_name` string COMMENT '顾问名称', 	
  `pay_type` string COMMENT '支付方式', 	
  `order_amount` decimal(10,5) COMMENT '订单金额', 	
  `order_cash` decimal(10,5) COMMENT '订单现金', 	
  `order_commission` decimal(10,5) COMMENT '订单佣金', 	
  `refund_amount` decimal(10,5) COMMENT '退款金额', 	
  `cancel_status` string COMMENT '取消状态', 	
  `inputtime` string COMMENT '输入时间', 	
  `pay_time` string COMMENT '支付时间', 	
  `service_day` int COMMENT '服务天数', 	
  `pay_time_end` string COMMENT '支付结束时间', 	
  `branch_name` string COMMENT '分公司名称', 	
  `introducer_name` string COMMENT '介绍人名称', 	
  `fwrymc` string COMMENT '未知字段', 	
  `khjlmc` string COMMENT '无对应人员', 	
  `customer_manager` string COMMENT '客户经理', 	
  `system_channel_name` string COMMENT '系统渠道名称', 	
  `channel_own` int COMMENT '渠道所有者', 	
  `curr_total_asset` decimal(16,4) COMMENT '当前总资产', 	
  `avg_total_asset` decimal(16,4) COMMENT '平均总资产', 	
  `avg_stockfund_balance` decimal(16,4) COMMENT '平均股票基金余额', 	
  `wsjyagyjl` decimal(16,10), 	
  `open_date` string COMMENT '开户日期', 	
  `gender` string COMMENT '性别', 	
  `degree` string COMMENT '学历', 	
  `card` string COMMENT '身份证号', 	
  `age` int COMMENT '年龄', 	
  `curr_posi` decimal(16,4) COMMENT '当前持仓', 	
  `day` string COMMENT '日期', 	
  `farex` decimal(16,4) COMMENT '未知字段，假设为小数', 	
  `is_yg` int COMMENT '是否有效', 	
  `sales_name` string COMMENT '销售关系')	
ROW FORMAT SERDE 	
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 	
STORED AS INPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 	
OUTPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'	
LOCATION	
  'hdfs://hadoop101:8020/user/hive/warehouse/ads.db/a_tg_branch_customer_detail'	
TBLPROPERTIES (	
  'transient_lastDdlTime'='1772417561')	



impala-shell  -q "
     
INVALIDATE METADATA  dws.s_tg_product_order_df ;
INVALIDATE METADATA  ods.o_uf20_hs_his_his_deliver;
INVALIDATE METADATA  dwd.d_uf20_customer_info_di ;
INVALIDATE METADATA ods.tmp_cc;
INVALIDATE METADATA ods.o_fr_hadata_tg_app_order_sales_import;
 

 insert overwrite table ads.a_tg_branch_customer_detail
 SELECT 
 t1.sole_code,
 t1.customer_name,
 t1.customer_account,
 t1.customer_phone,
 t1.customer_risk_level,
 t1.product_name,
 t1.product_type,
 t1.advisor_name,
 t1.pay_type,
 CAST(t1.order_amount AS DECIMAL(10,5)) AS order_amount,
 t1.order_cash,
 CAST(t1.order_commission AS DECIMAL(10,5)) AS order_commission, 
 CAST(t1.refund_amount AS DECIMAL(10,5)) AS refund_amount,
 t1.cancel_status,
 cast(inputtime as string),
  cast(t1.pay_time as string),
 t1.service_day,
   cast(t1.pay_time_end as string),
 t1.branch_name,
 t1.introducer_name,
 t1.fwrymc,
 t1.khjlmc,
 t1.customer_manager,
 t1.system_channel_name,
 t1.channel_own,
 CAST(t1.curr_total_asset AS DECIMAL(16,4)) AS curr_total_asset, -- 确保当前总资产的类型为 DECIMAL(10,2)
 CAST(t1.avg_total_asset AS DECIMAL(16,4)) AS avg_total_asset, -- 确保平均总资产的类型为 DECIMAL(10,2)
 CAST(t1.avg_stockfund_balance AS DECIMAL(16,4)) AS avg_stockfund_balance, 
 CAST(t1.wsjyagyjl AS DECIMAL(16,10)) AS wsjyagyjl, 
 t1.open_date,
 t1.gender,
 t1.degree,
 t1.card,
 t1.age,
 CAST(t1.curr_posi AS DECIMAL(16,4)) AS curr_posi, 
 t1.day,
 CAST(t4.farex_ty AS DECIMAL(16,4)) AS farex, 
 CASE WHEN LENGTH(t3.card) > 1 THEN 1 ELSE 0 END AS is_yg,
 sales.sales_name 
from
(   
    SELECT sole_code, customer_name, customer_account,
    concat(substr(customer_phone,1,4),'****',substr(customer_phone,9,11)) as customer_phone, 
    customer_risk_level, product_name, product_type, advisor_name, pay_type, order_amount, order_cash,
    order_commission, refund_amount, cancel_status, inputtime, pay_time, service_day, pay_time_end,
    branch_name, introducer_name, fwrymc, khjlmc, customer_manager, system_channel_name, channel_own,
    curr_total_asset, avg_total_asset, avg_stockfund_balance, wsjyagyjl, open_date, gender
    , degree, concat(substr(card,1,6),'********',substr(card,15,18)) as card, age, curr_posi, day
    from   dws.s_tg_product_order_df   
    where day='${day}'
) t1 
-- left join 
-- (
--   SELECT t31.client_id  as client_id,
--            t32.fund_account as fund_account,
--            sum(CASE WHEN exchange_type IN('A', 'D') THEN cast(farex AS float)*7.109
--                 WHEN exchange_type IN('H') THEN cast(farex AS float)*0.91496
--                 ELSE cast(farex AS float)
--            END) AS farex--提佣金额
--     FROM ods.o_uf20_hs_his_his_deliver t31
--     ,dwd.d_uf20_customer_info_di t32
--     WHERE t31.day>='20240901'  --从9月份开始升拥业务
--     and  t32.day<='${day}'
--     and t32.day='${day}'
--   and  fare_remark like '服务佣金%' 
--     and t31.client_id = t32.client_id
--     GROUP BY t31.client_id,t32.fund_account
-- ) t2  on t1.customer_account= t2.fund_account
left join
(
    select distinct a.client_id,b.card
    from dwd.d_uf20_customer_info_di a
    left join ods.tmp_cc b on a.card = b.card
    where a.day = '${day}'
) t3 on t3.client_id = t1.customer_account
 
left join (
select sole_code, sum(farex_ty) farex_ty
from dws.s_tg_customer_upgrade_income
group by sole_code
) t4 on t1.sole_code = t4.sole_code

left join 
(
	select    order_no
			 ,min(sales_name) as sales_name
	from     ods.o_fr_hadata_tg_app_order_sales_import
	where    day='${day}' group by order_no
) sales
on t1.sole_code=sales.order_no
"
