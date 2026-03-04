# 净值计算与价格数据说明

## 收盘价来源：StarRocks

| 数据       | 表名                 | 关键字段                          |
|------------|----------------------|-----------------------------------|
| 个股收盘价 | `hsgt_price_deliver` | biz_date(yyyymmdd), stock_code, last_price |
| 沪深300日线 | `hsgt_price_deliver` | stock_code=**000300**，last_price |
| 交易记录   | `config_stock_position` | trade_date, stock_code, side, position_pct, price |

## 物化视图直接计算净值

**数据都在 StarRocks 里，可以用物化视图完成净值计算。** 见 `mv_product_nav_compute.sql`：

- 源表：`config_stock_position` + `hsgt_price_deliver`（沪深300 用 stock_code=000300）
- 每 60 秒异步刷新
- **近似口径**：卖出时按目标股数计算，不 cap 于当日持仓（StarRocks 不支持递归 CTE，无法严格实现 min(目标量, 持仓)）。若实际交易中卖出从未超过持仓，则与 temp3 / Python 口径一致。
- 沪深300净值：`last_price / 基期收盘价`（stock_code=000300）

接口可通过环境变量 `NAV_SOURCE_TABLE=mv_product_nav_compute` 指定从物化视图读取；默认 `product_nav_daily`。无数据时回退到实时计算。

## hsgt_price_deliver 字段说明

| 字段         | 说明                         |
|--------------|------------------------------|
| **biz_date** | 日期，格式 **yyyymmdd**（如 20260105） |
| **stock_code** | 股票代码                     |
| **stock_name** | 股票中文名称                 |
| **last_price** | 当日收盘价                   |

- 与 `config_stock_position` 联用时，按 `stock_code` 对齐；若配置表为 6 位代码，需与 `hsgt_price_deliver` 中代码格式一致（必要时做 SZ/SH 前缀映射）。
