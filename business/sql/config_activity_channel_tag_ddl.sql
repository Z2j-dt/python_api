-- 活动渠道字典配置表（结构与 config_open_channel_tag 保持一致）
CREATE TABLE IF NOT EXISTS config_activity_channel_tag (
  id BIGINT NOT NULL,
  open_channel STRING NOT NULL COMMENT '活动渠道',
  wechat_customer_tag STRING NOT NULL COMMENT '企微客户标签',
  created_at DATETIME NULL COMMENT '创建时间',
  updated_at DATETIME NULL COMMENT '更新时间'
)
COMMENT "活动渠道字典配置表"
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);
