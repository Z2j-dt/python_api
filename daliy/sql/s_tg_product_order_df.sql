CREATE TABLE `dws.s_tg_product_order_df`(	
  `sole_code` string COMMENT '订单号', 	
  `customer_name` string COMMENT '用户姓名', 	
  `customer_account` string COMMENT '用户资金账号', 	
  `customer_phone` string COMMENT '用户电话号码', 	
  `customer_risk_level` string COMMENT '客户风险等级', 	
  `product_name` string COMMENT '产品名称', 	
  `product_type` string COMMENT '产品类型', 	
  `advisor_name` string COMMENT '产品投顾姓名', 	
  `pay_type` string COMMENT '支付方式', 	
  `order_amount` decimal(10,5) COMMENT '订单金额&提佣', 	
  `order_cash` decimal(10,5) COMMENT '订单金额', 	
  `order_commission` decimal(10,5) COMMENT '提佣', 	
  `refund_amount` string COMMENT '退款金额', 	
  `cancel_status` string COMMENT '取消状态', 	
  `inputtime` timestamp COMMENT '订单创建时间', 	
  `pay_time` timestamp COMMENT '订单支付时间', 	
  `service_day` int COMMENT '订单有效期', 	
  `pay_time_end` timestamp COMMENT '订单有效期', 	
  `branch_name` string COMMENT '营业部名称', 	
  `introducer_name` string COMMENT '推荐人姓名', 	
  `fwrymc` string COMMENT '服务人员', 	
  `khjlmc` string COMMENT '开发人员', 	
  `customer_manager` string COMMENT '核算客户经理', 	
  `system_channel_name` string COMMENT '二级渠道', 	
  `channel_own` int COMMENT '一级渠道', 	
  `curr_total_asset` decimal(16,2) COMMENT '当前时点总资产', 	
  `avg_total_asset` decimal(10,2) COMMENT '签约时间段日均资产', 	
  `avg_stockfund_balance` decimal(16,2) COMMENT '签约时间段股基交易量', 	
  `wsjyagyjl` string COMMENT '签约时佣金费率', 	
  `open_date` string COMMENT '开户日期', 	
  `gender` string COMMENT '性别', 	
  `degree` string COMMENT '学历', 	
  `card` string COMMENT '身份证号', 	
  `age` int COMMENT '年龄', 	
  `curr_posi` decimal(10,2) COMMENT '当前仓位', 	
  `sales_name` string COMMENT '订单销售人')	
COMMENT '同花顺投顾产品订单明细全量表'	
PARTITIONED BY ( 	
  `day` string)	
ROW FORMAT SERDE 	
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 	
STORED AS INPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 	
OUTPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'	
LOCATION	
  'hdfs://hadoop101:8020/user/hive/warehouse/dws.db/s_tg_product_order_df'	
TBLPROPERTIES (	
  'DO_NOT_UPDATE_STATS'='true', 	
  'transient_lastDdlTime'='1768975909')	
