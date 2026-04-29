# 门户子模块资源清单（权限 ID 草稿）

> 根据 `portal/templates/index.html` 自动生成，**请按需改资源 ID** 后再进入开发。  
> **约定**：非业务模块用单一 ID；业务模块（同载 `sr_api`）用 `sr_api:<data-view>`，与前端 `data-view`、埋点 `view` 一致。  
> **新建账号**：**默认没有任何资源权限**（侧栏不展示、不可进子模块），需 admin **按 `resource_id` 逐项**授权为 `read`（只读）或 `write`（可编辑）。  
> 下表仅列出**可被授权**的入口与 ID，不表示新建用户自动拥有其中任一权限。

## 资源 ID 规则

| 类型 | `resource_id` 格式 | 示例 |
|------|-------------------|------|
| 数据中心 / 技术支持 | 与 `data-module` 相同 | `hive_metadata` |
| 市场 / 直销 / 投顾 / 客户中心 | `sr_api:` + `data-view` | `sr_api:realtime` |

单资源授权级别：`read`（只读） / `write`（可编辑）；未配置的 `resource_id` 视为无权限（等价于不可见，实现上可用 `none` 或缺省）。

---

## 数据中心（`data_center`）

| 大类 | 菜单名称 | `data-module` | `data-view` | **resource_id** | 地址 hash |
|------|----------|---------------|-------------|-----------------|-----------|
| 数据中心 | 数据字典 | hive_metadata | （空） | `hive_metadata` | `#hive_metadata` |
| 数据中心 | 数据血缘 | sql_lineage | （空） | `sql_lineage` | `#sql_lineage` |
| 数据中心 | 离线上传 | excel_to_hive | （空） | `excel_to_hive` | `#excel_to_hive` |
| 数据中心 | 任务管理 | dolphin_failed | （空） | `dolphin_failed` | `#dolphin_failed` |
| 数据中心 | 数据导出 | sql_to_excel | （空） | `sql_to_excel` | `#sql_to_excel` |

---

## 市场中心（`market_center`）

| 大类 | 菜单名称 | `data-module` | `data-view` | **resource_id** | 地址 hash |
|------|----------|---------------|-------------|-----------------|-----------|
| 市场中心 | 实时加微名单 | sr_api | realtime | `sr_api:realtime` | `#sr_api_realtime` |
| 市场中心 | 平台投流账号配置 | sr_api | config_code_mapping | `sr_api:config_code_mapping` | `#sr_api_config_code_mapping` |

---

## 直销中心（`direct_center`）

| 大类 | 菜单名称 | `data-module` | `data-view` | **resource_id** | 地址 hash |
|------|----------|---------------|-------------|-----------------|-----------|
| 直销中心 | 自营渠道加微统计 | sr_api | open_channel_daily | `sr_api:open_channel_daily` | `#sr_api_open_channel_daily` |
| 直销中心 | 渠道字典配置 | sr_api | config_open | `sr_api:config_open` | `#sr_api_config_open` |
| 直销中心 | 承接人员配置 | sr_api | config_staff | `sr_api:config_staff` | `#sr_api_config_staff` |

---

## 投顾中心（`advisor_center`）

| 大类 | 菜单名称 | `data-module` | `data-view` | **resource_id** | 地址 hash |
|------|----------|---------------|-------------|-----------------|-----------|
| 投顾中心 | 产品净值 | sr_api | config_stock_position | `sr_api:config_stock_position` | `#sr_api_config_stock_position` |
| 投顾中心 | 销售订单配置 | sr_api | config_sales_order | `sr_api:config_sales_order` | `#sr_api_config_sales_order` |
| 投顾中心 | 签约客户群管理配置 | sr_api | config_sign_customer_group | `sr_api:config_sign_customer_group` | `#sr_api_config_sign_customer_group` |
| 投顾中心 | 活动渠道字典配置 | sr_api | config_activity_channel | `sr_api:config_activity_channel` | `#sr_api_config_activity_channel` |
| 投顾中心 | 销售每日进线数据表 | sr_api | sales_daily_leads | `sr_api:sales_daily_leads` | `#sr_api_sales_daily_leads` |

---

## 客户中心（`customer_center`）

| 大类 | 菜单名称 | `data-module` | `data-view` | **resource_id** | 地址 hash |
|------|----------|---------------|-------------|-----------------|-----------|
| 客户中心 | 商机线索配置 | sr_api | config_opportunity_lead | `sr_api:config_opportunity_lead` | `#sr_api_config_opportunity_lead` |
| 客户中心 | 早盘人气股战绩追踪配置 | sr_api | config_morning_hot_stock_track | `sr_api:config_morning_hot_stock_track` | `#sr_api_config_morning_hot_stock_track` |

---

## 全量 `resource_id` 列表（复制用）

```
hive_metadata
sql_lineage
excel_to_hive
dolphin_failed
sql_to_excel
sr_api:realtime
sr_api:config_code_mapping
sr_api:open_channel_daily
sr_api:config_open
sr_api:config_staff
sr_api:config_stock_position
sr_api:config_sales_order
sr_api:config_sign_customer_group
sr_api:config_activity_channel
sr_api:sales_daily_leads
sr_api:config_opportunity_lead
sr_api:config_morning_hot_stock_track
```

---

## 与代码的对应关系

- **库表**：账号与各 `resource_id` 的 `read` / `write` 授权见 `portal/sql/portal_user_resource_ddl.sql`（表 `portal_user_resource`）；表中无行的用户即无任何子模块权限。
- **业务 API 写保护**：`business/app.py` 在 `PORTAL_PERMISSION_FROM_DB=1` 时，各配置类 POST/PUT/DELETE 会校验签名 Cookie / `portal_token` 中的 `r`，要求对应 `resource_id` 为 `write`（与上表一致）；`PORTAL_SUPERUSERS` 与门户相同。单端口部署下浏览器会带上 `portal_auth`；门户与业务不同端口时，需保证请求仍能携带门户凭证（如网关注入头或前端透传 token）。
- 侧栏：`data-module` / `data-view` → `portal/templates/index.html`
- Hash 兜底：`hashToModuleView` 同文件内脚本
- iframe URL 参数：`buildModuleUrl()` 内 `view=` / `tab=` 映射
- 门户路径 → 模块：`_PATH_TO_MODULE` / `_module_from_path` → `portal/app.py`

新增菜单时：**先在本表增加一行并定 `resource_id`，再改 HTML 与 JS**，避免权限与导航脱节。
