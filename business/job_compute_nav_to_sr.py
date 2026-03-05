# -*- coding: utf-8 -*-
"""
按 temp3.py 口径，从 StarRocks:
  - config_stock_position   交易表
  - hsgt_price_deliver      收盘价表（含个股 + 沪深300=000300）

计算某个产品的每日净值 + 沪深300净值，并写入 product_nav_daily。
"""

import pymysql
import pandas as pd
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from decimal import Decimal, ROUND_DOWN, getcontext

# ===== 基本参数 =====
INIT_CAPITAL = 10_000_000      # 初始资金 1000 万
PRODUCT_NAME = "短线王"        # 要计算的产品名称
START_DATE = "2025-12-01"      # 基期，用于对齐 HS300
getcontext().prec = 28


def _d(v) -> Decimal:
    return Decimal(str(v))


def _q2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.00"), rounding=ROUND_DOWN)


def _q2_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# StarRocks 连接配置
SR_HOST = "10.8.93.40"
SR_PORT = 9030
SR_USER = "root"
SR_PASSWORD = "star@dt1988"
SR_DB = "portal_db"

# 表名
CONFIG_STOCK_POSITION_TABLE = "config_stock_position"
HSGT_PRICE_TABLE = "hsgt_price_deliver"
PRODUCT_NAV_DAILY_TABLE = "product_nav_daily"   # 目标净值表


def get_conn():
    return pymysql.connect(
        host=SR_HOST,
        port=SR_PORT,
        user=SR_USER,
        password=SR_PASSWORD,
        database=SR_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def _parse_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if hasattr(v, "strftime"):
        # StarRocks DATE/DATETIME
        return v.date() if hasattr(v, "date") else v
    s = str(v).strip()
    if len(s) >= 8 and s.isdigit():
        return datetime.strptime(s[:8], "%Y%m%d").date()
    if "-" in s:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    return None


def load_trades(cur) -> pd.DataFrame:
    """
    从 config_stock_position 读取指定产品的交易记录，仿 temp3 的 df_trade。
    假定 position_pct 存的是“百分比数值”：5 表示 5%。
    """
    sql = f"""
        SELECT trade_date, stock_code, stock_name, position_pct, side, price
        FROM {CONFIG_STOCK_POSITION_TABLE}
        WHERE product_name = %s
          AND trade_date IS NOT NULL
          AND stock_code IS NOT NULL
          AND side IS NOT NULL
        ORDER BY trade_date ASC, id ASC
    """
    cur.execute(sql, (PRODUCT_NAME,))
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"{PRODUCT_NAME} 在 {CONFIG_STOCK_POSITION_TABLE} 中无交易记录")

    df = pd.DataFrame(rows)
    df["日期"] = df["trade_date"].apply(_parse_date)
    df["股票代码"] = df["stock_code"]
    df["买入/卖出"] = df["side"]
    # 仓位：转成“比例”，5% -> 0.05，2.5% -> 0.025
    df["仓位"] = df["position_pct"].astype(float) / 100.0
    df["成交价"] = df["price"].astype(float)
    df = df.sort_values(["日期"]).reset_index(drop=True)
    return df[["日期", "股票代码", "stock_name", "仓位", "买入/卖出", "成交价"]]


def load_hs300_from_sr(cur) -> pd.DataFrame:
    """
    从 hsgt_price_deliver 读取沪深300(日线)，仿 temp3 的 df_hs300。
    要求 stock_code='000300'。
    """
    sql = f"""
        SELECT biz_date, last_price
        FROM {HSGT_PRICE_TABLE}
        WHERE stock_code = '000300'
          AND biz_date IS NOT NULL
          AND last_price IS NOT NULL
        ORDER BY biz_date ASC
    """
    cur.execute(sql)
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError(f"{HSGT_PRICE_TABLE} 中无 stock_code='000300' 的数据")

    df = pd.DataFrame(rows)
    df["date"] = df["biz_date"].apply(_parse_date)
    df = df.sort_values("date").drop_duplicates("date")
    df = df.rename(columns={"last_price": "close"})
    return df[["date", "close"]]


def load_stock_prices_from_sr(cur, codes: List[str], start_dt: date, end_dt: date) -> pd.DataFrame:
    """
    从 hsgt_price_deliver 读取所有个股在 [start_dt, end_dt] 的收盘价，仿 temp3 的 df_close。
    返回 DataFrame: index=date, columns=股票代码, 值=收盘价 (经过 ffill/bfill)。
    """
    if not codes:
        raise RuntimeError("无股票代码，无法读取个股价格")

    codes_tuple = tuple(sorted(set(codes)))
    placeholder = ",".join(["%s"] * len(codes_tuple))

    sql = f"""
        SELECT biz_date, stock_code, last_price
        FROM {HSGT_PRICE_TABLE}
        WHERE stock_code IN ({placeholder})
          AND biz_date IS NOT NULL
          AND last_price IS NOT NULL
    """
    cur.execute(sql, codes_tuple)
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("hsgt_price_deliver 中无对应个股价格数据")

    df = pd.DataFrame(rows)
    df["date"] = df["biz_date"].apply(_parse_date)
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
    df["last_price"] = df["last_price"].astype(float)

    # 透视成收盘价矩阵：index=date, columns=股票代码
    df_pivot = df.pivot(index="date", columns="stock_code", values="last_price")
    return df_pivot


def compute_nav_series_from_sr() -> List[Dict[str, Any]]:
    with get_conn() as conn, conn.cursor() as cur:
        # 1. 交易记录（仿 temp3 的 df_trade）
        df_trade = load_trades(cur)

        # 2. 沪深300日线（仿 temp3 的 df_hs300）
        df_hs300 = load_hs300_from_sr(cur)

        # 基期处理（与 temp3 口径一致）
        trade_dates_hs300 = df_hs300["date"].tolist()
        if not trade_dates_hs300:
            raise RuntimeError("沪深300日线无交易日")

        base_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        if base_date not in trade_dates_hs300:
            base_date = trade_dates_hs300[0]
            print(f"警告：基期 {START_DATE} 不是交易日，自动使用第一个交易日 {base_date} 作为基期")

        base_hs300 = df_hs300.loc[df_hs300["date"] == base_date, "close"].iloc[0]
        df_hs300["hs300_nav"] = df_hs300["close"] / base_hs300

        # 3. 个股日线价格矩阵（仿 temp3 的 df_close）
        conv_codes = df_trade["股票代码"].unique().tolist()
        start_dt = min(df_trade["日期"].min(), df_hs300["date"].min())
        end_dt = df_hs300["date"].max()

        df_close = load_stock_prices_from_sr(cur, conv_codes, start_dt, end_dt)
        # 对齐到 hs300 的交易日，并做前后填充
        trade_dates = df_hs300["date"].tolist()
        df_close = df_close.reindex(trade_dates).ffill().bfill()

    # 4. 按 temp3 逻辑逐日回测（份额保留两位小数，不取整；卖出支持按持仓比例减仓）
    cash = _d(INIT_CAPITAL)
    positions: Dict[str, Decimal] = {}
    last_ratio: Dict[str, Decimal] = {}  # 每只股票建仓/加仓时的仓位比例，用于按比例减仓
    nav_list: List[Dict[str, Any]] = []
    trade_by_date = {d: g for d, g in df_trade.groupby("日期")}

    print("开始逐日计算净值（使用 StarRocks 价格）...")
    for dt in trade_dates:
        # 执行当日所有交易
        if dt in trade_by_date:
            day_trades = trade_by_date[dt]
            for _, row in day_trades.iterrows():
                code = row["股票代码"]
                action = row["买入/卖出"]
                ratio = _d(row["仓位"])      # 比例，如 0.05、0.025
                price = _d(row["成交价"])
                target_amount = _d(INIT_CAPITAL) * ratio

                if code not in df_close.columns:
                    print(f"  警告：{dt} 股票 {code} 无价格数据，跳过交易")
                    continue

                if action == "买入":
                    if price <= 0:
                        continue
                    shares = _q2(target_amount / price)
                    if shares <= 0:
                        print(f"  警告：{dt} {code} 买入股数为0，跳过")
                        continue
                    cost = _q2_money(shares * price)
                    if cash < cost:
                        shares = _q2(cash / price)
                        if shares <= 0:
                            continue
                        cost = _q2_money(shares * price)
                    cash = cash - cost
                    positions[code] = positions.get(code, Decimal("0")) + shares
                    last_ratio[code] = ratio  # 记录建仓/加仓时的仓位比例（Decimal）

                elif action == "卖出":
                    if code not in positions or positions[code] == 0:
                        print(f"  警告：{dt} {code} 持仓为0，无法卖出")
                        continue
                    # 若存在上次建仓仓位且 本次仓位 < 建仓仓位，则按“持仓比例”减仓（如 5% 建仓后卖 2.5% = 卖一半）
                    base_ratio = last_ratio.get(code)
                    if base_ratio is not None and base_ratio > 0 and ratio >= base_ratio:
                        # 业务含义：卖出仓位 >= 建仓仓位，视为清仓，直接卖掉当前全部持仓
                        shares = positions[code]
                    elif base_ratio is not None and base_ratio > 0 and ratio < base_ratio:
                        sell_ratio = ratio / base_ratio
                        shares = _q2(positions[code] * sell_ratio)
                        if shares > positions[code]:
                            shares = positions[code]
                    else:
                        if price <= 0:
                            continue
                        shares = _q2(target_amount / price)
                        if shares > positions[code]:
                            shares = positions[code]
                    if shares <= 0:
                        print(f"  警告：{dt} {code} 卖出股数为0，跳过")
                        continue
                    revenue = _q2_money(shares * price)
                    cash = cash + revenue
                    positions[code] = positions[code] - shares
                    if positions[code] == 0:
                        del positions[code]
                        last_ratio.pop(code, None)
                    else:
                        last_ratio[code] = ratio  # 剩余仓位对应新比例
                else:
                    print(f"  未知操作：{action}")

        # 计算当日市值与净值
        market_value = Decimal("0")
        for code, shares in positions.items():
            if code not in df_close.columns:
                print(f"  警告：{dt} 股票 {code} 无价格数据，按0计算市值")
                continue
            close_price = _d(df_close.loc[dt, code])
            market_value = market_value + (shares * close_price)

        total_asset = cash + market_value
        nav = float(total_asset / _d(INIT_CAPITAL))

        hs300_nav = df_hs300.loc[df_hs300["date"] == dt, "hs300_nav"].iloc[0]
        nav_list.append(
            {
                "biz_date": dt,
                "nav": round(nav, 6),
                "hs300_nav": round(hs300_nav, 6),
            }
        )

    return nav_list


def write_nav_to_sr(series: List[Dict[str, Any]]):
    if not series:
        print("[INFO] 无净值数据可写入")
        return
    sql = f"""
        INSERT INTO {PRODUCT_NAV_DAILY_TABLE}
            (product_name, biz_date, nav, hs300_nav, updated_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    now_dt = datetime.now()
    vals = [
        (PRODUCT_NAME, row["biz_date"], row["nav"], row["hs300_nav"], now_dt)
        for row in series
    ]
    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, vals)
        conn.commit()
    print(f"[OK] 写入 {PRODUCT_NAV_DAILY_TABLE} 条数: {len(series)}")


def main():
    print(f"开始按 temp3 口径，从 StarRocks 计算产品 {PRODUCT_NAME} 净值...")
    series = compute_nav_series_from_sr()
    print(f"计算完成，交易日数: {len(series)}")
    write_nav_to_sr(series)
    print("任务完成。")


if __name__ == "__main__":
    main()