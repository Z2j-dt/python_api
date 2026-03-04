# -*- coding: utf-8 -*-
"""
按 temp3.py 口径，从 StarRocks:
  - config_stock_position  (交易)
  - hsgt_price_deliver     (收盘价，含个股+沪深300=000300)
计算指定产品每日持仓明细 + 总资产 + 净值，
写入 product_nav_daily_detail 表（含每只股票明细 + 当日总计行）。
"""

import pymysql
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, getcontext

# ===== StarRocks 连接配置 =====
SR_HOST = "10.8.93.40"
SR_PORT = 9030
SR_USER = "root"
SR_PASSWORD = "star@dt1988"
SR_DB = "test_db"

CONFIG_STOCK_POSITION_TABLE = "config_stock_position"
HSGT_PRICE_TABLE = "hsgt_price_deliver"
DETAIL_TABLE = "product_nav_daily_detail"

NAV_INIT_CAPITAL = 10_000_000  # 初始资金 1000 万
HS300_CODE_DB = "000300"       # 沪深300 在价格表里的代码
PRODUCT_NAME = "短线王"        # 要计算的产品名

getcontext().prec = 28


def _d(v) -> Decimal:
    return Decimal(str(v))


def _q2(x: Decimal) -> Decimal:
    # 份额/股数保留两位小数，向下取整（避免超买/超卖）
    return x.quantize(Decimal("0.00"), rounding=ROUND_DOWN)


def _q2_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

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
        # MySQL DATE/DATETIME
        return v.date() if hasattr(v, "date") else v
    s = str(v).strip()
    if len(s) >= 8 and s.isdigit():
        return datetime.strptime(s[:8], "%Y%m%d").date()
    if "-" in s:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    return None


def load_trades(cur, product_name: str) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT trade_date, stock_code, stock_name, side, position_pct, price
        FROM {CONFIG_STOCK_POSITION_TABLE}
        WHERE product_name = %s
          AND trade_date IS NOT NULL
          AND stock_code IS NOT NULL
          AND side IS NOT NULL
        ORDER BY trade_date ASC, id ASC
    """
    cur.execute(sql, (product_name,))
    return cur.fetchall()


def load_prices(cur, min_d: date, max_d: date) -> Dict[Tuple[date, str], float]:
    sql = f"""
        SELECT biz_date, stock_code, last_price
        FROM {HSGT_PRICE_TABLE}
        WHERE biz_date IS NOT NULL
          AND last_price IS NOT NULL
    """
    cur.execute(sql)
    rows = cur.fetchall()

    price_map: Dict[Tuple[date, str], float] = {}
    for r in rows:
        dt = _parse_date(r.get("biz_date"))
        if not dt or dt < min_d or dt > max_d:
            continue
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        try:
            pr = float(r.get("last_price"))
        except (TypeError, ValueError):
            continue
        price_map[(dt, code)] = pr
    return price_map


def compute_daily_detail(product_name: str, cur) -> List[Dict[str, Any]]:
    trades = load_trades(cur, product_name)
    if not trades:
        return []

    # 交易日期区间
    trade_dates = set()
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            trade_dates.add(d)
    if not trade_dates:
        return []
    min_d, max_d = min(trade_dates), max(trade_dates)

    price_map = load_prices(cur, min_d, max_d)
    price_dates = {dt for (dt, _) in price_map.keys()}
    all_dates = sorted(d for d in (trade_dates | price_dates) if min_d <= d <= max_d)
    if not all_dates:
        return []

    # 按日期分组交易
    trade_by_date: Dict[date, List[Dict[str, Any]]] = {}
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            trade_by_date.setdefault(d, []).append(r)

    def _get_price(dt: date, code: str) -> Decimal:
        v = price_map.get((dt, code))
        return _d(v) if v is not None else Decimal("0")

    # 先准备沪深300净值（基期 = 最早有 price 的日期）
    hs300_prices = {d: p for (d, c), p in price_map.items() if c == HS300_CODE_DB}
    hs300_dates = sorted(hs300_prices.keys())
    hs300_nav_map: Dict[date, float] = {}
    if hs300_dates:
        base_d = hs300_dates[0]
        base_price = hs300_prices[base_d]
        if base_price > 0:
            for d in hs300_dates:
                hs300_nav_map[d] = hs300_prices[d] / base_price

    cash = _d(NAV_INIT_CAPITAL)
    positions: Dict[str, Decimal] = {}
    # 每只股票“建仓/加仓”时的仓位%，用于卖出时按比例减仓（如 5% 建仓后卖 2.5% = 卖一半持仓）
    last_pct: Dict[str, float] = {}

    detail_rows: List[Dict[str, Any]] = []

    for dt in all_dates:
        # ===== 1. 执行当日每一笔交易，记录 row_type = 1 的过程明细 =====
        for row in trade_by_date.get(dt, []):
            code = (row.get("stock_code") or "").strip()
            side = (row.get("side") or "").strip()
            stock_name = row.get("stock_name")
            try:
                pct = float(row.get("position_pct") or 0)
                trade_price = float(row.get("price") or 0)
            except (TypeError, ValueError):
                continue
            if trade_price <= 0:
                continue

            pos_before = positions.get(code, Decimal("0"))
            cash_before = cash

            # position_pct 存的是“百分比数值”(5 表示 5%)
            target_amount = _d(NAV_INIT_CAPITAL) * _d(pct) / Decimal("100")
            trade_shares = Decimal("0")

            if side == "买入":
                price_d = _d(trade_price)
                if price_d <= 0:
                    continue
                shares = _q2(target_amount / price_d)
                if shares <= 0:
                    continue
                cost = _q2_money(shares * price_d)
                if cash < cost:
                    shares = _q2(cash / price_d)
                    if shares <= 0:
                        continue
                    cost = _q2_money(shares * price_d)
                cash = cash - cost
                positions[code] = positions.get(code, Decimal("0")) + shares
                trade_shares = shares  # 买入为正（两位小数）
                last_pct[code] = pct  # 记录建仓/加仓时的仓位%，供后续“按比例减仓”用

            elif side == "卖出":
                if code not in positions or positions[code] <= 0:
                    continue
                # 卖出：若存在上次建仓仓位% 且 本次仓位% < 建仓仓位%，则按“持仓比例”减仓（如 5% 建仓后卖 2.5% = 卖一半）
                base_pct = last_pct.get(code)
                if base_pct is not None and base_pct > 0 and pct >= base_pct:
                    # 业务含义：卖出仓位 >= 建仓仓位，视为清仓，直接卖掉当前全部持仓
                    shares = pos_before
                elif base_pct is not None and base_pct > 0 and pct < base_pct:
                    # 按持仓比例：卖出股数 = 当前持仓 * (本次仓位% / 建仓仓位%)，再按 100 股取整
                    ratio = _d(pct) / _d(base_pct)
                    shares = _q2(pos_before * ratio)
                    if shares > pos_before:
                        shares = pos_before
                else:
                    # 无历史仓位% 或 本次仓位>=建仓仓位：按“初始资金*仓位%”算目标股数，再 cap 于持仓
                    price_d = _d(trade_price)
                    if price_d <= 0:
                        continue
                    shares = _q2(target_amount / price_d)
                    if shares > positions[code]:
                        shares = positions[code]
                if shares <= 0:
                    continue
                price_d = _d(trade_price)
                revenue = _q2_money(shares * price_d)
                cash = cash + revenue
                positions[code] = positions[code] - shares
                if positions[code] == 0:
                    positions.pop(code, None)
                    last_pct.pop(code, None)
                else:
                    last_pct[code] = pct  # 剩余仓位对应新的%
                trade_shares = -shares  # 卖出为负

            else:
                continue

            pos_after = positions.get(code, Decimal("0"))
            cash_after = cash

            # row_type = 1：逐笔交易明细
            detail_rows.append(
                {
                    "product_name": product_name,
                    "biz_date": dt,
                    "row_type": 1,
                    "stock_code": code,
                    "stock_name": stock_name,
                    "side": side,
                    "position_pct": pct,
                    "trade_price": trade_price,
                    "trade_shares": trade_shares,
                    "position_before": pos_before,
                    "position_after": pos_after,
                    "cash_before": float(cash_before),
                    "cash_after": float(cash_after),
                    "close_price": None,
                    "market_value": None,
                    "total_position_mv": None,
                    "total_cash": None,
                    "total_asset": None,
                    "nav": None,
                    "hs300_nav": None,
                    "remark": None,
                }
            )

        # ===== 2. 计算当日市值、总资产、净值，记录 row_type = 2/3 =====
        total_mv = Decimal("0")
        per_stock_snapshot: List[Tuple[str, Decimal, Decimal, Decimal]] = []
        for code, sh in positions.items():
            px = _get_price(dt, code)
            mv = sh * px
            total_mv += mv
            per_stock_snapshot.append((code, sh, px, mv))

        total_asset = cash + total_mv
        nav = float(total_asset / _d(NAV_INIT_CAPITAL))
        hs300_nav = hs300_nav_map.get(dt)

        # row_type = 2：当日每只股票的持仓快照
        for code, sh, px, mv in per_stock_snapshot:
            detail_rows.append(
                {
                    "product_name": product_name,
                    "biz_date": dt,
                    "row_type": 2,
                    "stock_code": code,
                    "stock_name": None,
                    "side": None,
                    "position_pct": None,
                    "trade_price": None,
                    "trade_shares": None,
                    "position_before": None,
                    "position_after": sh,
                    "cash_before": None,
                    "cash_after": float(cash),
                    "close_price": float(px),
                    "market_value": float(mv),
                    "total_position_mv": None,
                    "total_cash": None,
                    "total_asset": None,
                    "nav": None,
                    "hs300_nav": None,
                    "remark": None,
                }
            )

        # row_type = 3：当日总资产汇总行
        detail_rows.append(
            {
                "product_name": product_name,
                "biz_date": dt,
                "row_type": 3,
                "stock_code": "__TOTAL__",
                "stock_name": "当日汇总",
                "side": None,
                "position_pct": None,
                "trade_price": None,
                "trade_shares": None,
                "position_before": None,
                "position_after": None,
                "cash_before": None,
                "cash_after": float(cash),
                "close_price": None,
                "market_value": None,
                "total_position_mv": float(total_mv),
                "total_cash": float(cash),
                "total_asset": float(total_asset),
                "nav": float(nav),
                "hs300_nav": hs300_nav,
                "remark": None,
            }
        )

    return detail_rows


def write_detail_to_sr(rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] 无明细数据可写入")
        return

    sql = f"""
        INSERT INTO {DETAIL_TABLE}
        (
          product_name, biz_date, row_type,
          stock_code, stock_name,
          side, position_pct, trade_price, trade_shares,
          position_before, position_after,
          cash_before, cash_after,
          close_price, market_value,
          total_position_mv, total_cash, total_asset,
          nav, hs300_nav,
          remark, updated_at
        )
        VALUES (
          %s, %s, %s,
          %s, %s,
          %s, %s, %s, %s,
          %s, %s,
          %s, %s,
          %s, %s,
          %s, %s, %s,
          %s, %s,
          %s, %s
        )
    """
    now_dt = datetime.now()
    vals = []
    for r in rows:
        vals.append(
            (
                r["product_name"],
                r["biz_date"],
                r["row_type"],
                r["stock_code"],
                r["stock_name"],
                r["side"],
                r["position_pct"],
                r["trade_price"],
                r["trade_shares"],
                r["position_before"],
                r["position_after"],
                r["cash_before"],
                r["cash_after"],
                r["close_price"],
                r["market_value"],
                r["total_position_mv"],
                r["total_cash"],
                r["total_asset"],
                r["nav"],
                r["hs300_nav"],
                r["remark"],
                now_dt,
            )
        )

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, vals)
        conn.commit()
    print(f"[OK] 写入 {DETAIL_TABLE} 行数: {len(rows)}")


def main():
    print(f"开始计算产品 {PRODUCT_NAME} 的每日明细（从 StarRocks 两张表读取）...")
    with get_conn() as conn, conn.cursor() as cur:
        rows = compute_daily_detail(PRODUCT_NAME, cur)
    print(f"计算得到明细行数: {len(rows)}")
    write_detail_to_sr(rows)
    print("任务完成。")


if __name__ == "__main__":
    main()