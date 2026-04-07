# -*- coding: utf-8 -*-
"""
同赢先锋专用增量任务（从快照续算）

用途：
1) 保留已人工写入的历史净值（含 2025-05-09 基期与 2026-03-30 快照）；
2) 从 2026-03-31（可通过 --from-date 覆盖）开始，按交易+行情增量续算；
3) 仅 DELETE+INSERT 增量区间，不触碰历史。
"""

import argparse
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from job_compute_nav_detail_to_sr import (
    CONFIG_STOCK_POSITION_TABLE,
    DETAIL_TABLE,
    HSGT_PRICE_TABLE,
    HS300_CODE_SQL_IN,
    PRICE_LOOKBACK_BEFORE_FIRST_TRADE_DAYS,
    _d,
    _hs300_priority,
    _match_hs300_code,
    _parse_date,
    _q2_money,
    _q4_generic,
    _q4_share,
    fetch_hs300_close_on_date,
    get_conn,
    get_latest_price_date,
    load_prices_and_names,
)

PRODUCT_NAME = "同赢先锋"
SNAPSHOT_DATE = date(2026, 3, 30)
DEFAULT_UPDATE_FROM = SNAPSHOT_DATE + timedelta(days=1)


def _parse_date_arg(raw: str) -> date:
    s = (raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    raise ValueError(f"无法解析日期: {raw!r}，请使用 YYYYMMDD 或 YYYY-MM-DD")


def _cli_parse_update_from() -> date:
    parser = argparse.ArgumentParser(description="同赢先锋从快照续算净值明细")
    parser.add_argument(
        "-f",
        "--from-date",
        dest="from_date",
        metavar="YYYYMMDD",
        help="从该日起重算并落库，格式 YYYYMMDD 或 YYYY-MM-DD（默认 2026-03-31）",
    )
    args = parser.parse_args()
    raw = (args.from_date or "").strip()
    if not raw:
        return DEFAULT_UPDATE_FROM
    return _parse_date_arg(raw)


def load_snapshot_state(cur, snapshot_date: date) -> Dict[str, Any]:
    # row_type=3 汇总
    cur.execute(
        f"""
        SELECT biz_date, total_asset, nav, hs300_nav, total_cash
        FROM {DETAIL_TABLE}
        WHERE product_name = %s
          AND row_type = 3
          AND biz_date = %s
        LIMIT 1
        """,
        (PRODUCT_NAME, snapshot_date),
    )
    total_row = cur.fetchone() or {}
    if not total_row:
        raise ValueError(f"{PRODUCT_NAME} 在 {snapshot_date} 未找到 row_type=3 快照")

    # row_type=1 成交（用于还原 lot 成本）
    cur.execute(
        f"""
        SELECT row_id, stock_code, stock_name, side, position_pct, trade_price, trade_shares
        FROM {DETAIL_TABLE}
        WHERE product_name = %s
          AND row_type = 1
          AND biz_date = %s
        ORDER BY row_id ASC
        """,
        (PRODUCT_NAME, snapshot_date),
    )
    trade_rows = cur.fetchall() or []
    if not trade_rows:
        raise ValueError(f"{PRODUCT_NAME} 在 {snapshot_date} 未找到 row_type=1 快照交易")

    # row_type=2 持仓（用于校验与补 stock_name）
    cur.execute(
        f"""
        SELECT stock_code, stock_name, position_after, open_pct
        FROM {DETAIL_TABLE}
        WHERE product_name = %s
          AND row_type = 2
          AND biz_date = %s
        """,
        (PRODUCT_NAME, snapshot_date),
    )
    pos_rows = cur.fetchall() or []
    if not pos_rows:
        raise ValueError(f"{PRODUCT_NAME} 在 {snapshot_date} 未找到 row_type=2 快照持仓")

    cash = _d(total_row.get("total_cash") or 0)
    total_asset = _d(total_row.get("total_asset") or 0)
    nav = _d(total_row.get("nav") or 0)
    if nav <= 0 or total_asset <= 0:
        raise ValueError(f"{PRODUCT_NAME} 在 {snapshot_date} 快照 total_asset/nav 非法")
    portfolio_nav_denom = total_asset / nav

    lots: Dict[str, List[Dict[str, Decimal]]] = {}
    stock_name_map: Dict[str, str] = {}
    for r in trade_rows:
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        nm = (r.get("stock_name") or "").strip()
        if nm:
            stock_name_map[code] = nm
        side = (r.get("side") or "").strip()
        if side != "买入":
            # 快照建仓日按“买入”恢复成本；若后续有卖出快照，可在此扩展 FIFO 还原。
            continue
        pct = _d(r.get("position_pct") or 0)
        price = _d(r.get("trade_price") or 0)
        shares = _d(r.get("trade_shares") or 0)
        if pct <= 0 or price <= 0 or shares <= 0:
            continue
        cost = _q2_money(shares * price)
        lots.setdefault(code, []).append(
            {
                "pct_remain": pct,
                "shares_remain": shares,
                "cost_remain": cost,
            }
        )

    positions: Dict[str, Decimal] = {}
    position_cost: Dict[str, Decimal] = {}
    for code, ls in lots.items():
        sh = Decimal("0")
        cs = Decimal("0")
        for lot in ls:
            sh += _d(lot.get("shares_remain") or 0)
            cs += _d(lot.get("cost_remain") or 0)
        if sh > 0:
            positions[code] = sh
            position_cost[code] = cs

    # 用 row_type=2 修正/补齐份额与名称
    for r in pos_rows:
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        nm = (r.get("stock_name") or "").strip()
        if nm:
            stock_name_map[code] = nm
        sh = _d(r.get("position_after") or 0)
        if sh > 0:
            positions[code] = sh
            if code not in position_cost:
                position_cost[code] = Decimal("0")
                lots.setdefault(
                    code,
                    [{"pct_remain": _d(r.get("open_pct") or 0), "shares_remain": sh, "cost_remain": Decimal("0")}],
                )

    return {
        "cash": cash,
        "positions": positions,
        "position_cost": position_cost,
        "lots": lots,
        "portfolio_nav_denom": portfolio_nav_denom,
        "stock_name_map": stock_name_map,
        "snapshot_hs300_nav": _d(total_row.get("hs300_nav") or 0),
    }


def load_trades_after_snapshot(cur, from_date: date) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT trade_date, row_id, stock_code, stock_name, side, position_pct, price
        FROM {CONFIG_STOCK_POSITION_TABLE}
        WHERE product_name = %s
          AND trade_date IS NOT NULL
          AND stock_code IS NOT NULL
          AND side IS NOT NULL
          AND trade_date >= %s
        ORDER BY trade_date ASC, row_id ASC, id ASC
    """
    cur.execute(sql, (PRODUCT_NAME, from_date))
    return cur.fetchall() or []


def build_hs300_nav_map(
    cur,
    price_map: Dict[Tuple[date, str], float],
    snapshot_date: date,
    snapshot_hs300_nav: Decimal,
) -> Dict[date, float]:
    hs300_prices: Dict[date, Decimal] = {}
    hs300_code_for_date: Dict[date, str] = {}
    for (d, c), p in price_map.items():
        if not _match_hs300_code(c):
            continue
        pr = _d(p)
        cc = (c or "").strip().upper()
        if d not in hs300_prices or _hs300_priority(cc) < _hs300_priority(hs300_code_for_date.get(d, "")):
            hs300_prices[d] = pr
            hs300_code_for_date[d] = cc

    # 用“快照日已知 hs300_nav”反推出基准价，保证续算序列无缝衔接
    snapshot_px = hs300_prices.get(snapshot_date) or fetch_hs300_close_on_date(cur, snapshot_date)
    if not snapshot_px or snapshot_px <= 0 or snapshot_hs300_nav <= 0:
        return {}
    base_price = snapshot_px / snapshot_hs300_nav
    out: Dict[date, float] = {}
    for d, px in hs300_prices.items():
        if px > 0:
            out[d] = float(px / base_price)
    return out


def compute_from_snapshot(cur, update_from: date) -> List[Dict[str, Any]]:
    state = load_snapshot_state(cur, SNAPSHOT_DATE)
    cash: Decimal = state["cash"]
    positions: Dict[str, Decimal] = state["positions"]
    position_cost: Dict[str, Decimal] = state["position_cost"]
    lots: Dict[str, List[Dict[str, Decimal]]] = state["lots"]
    portfolio_nav_denom: Decimal = state["portfolio_nav_denom"]
    stock_name_map: Dict[str, str] = state["stock_name_map"]
    snapshot_hs300_nav: Decimal = state["snapshot_hs300_nav"]

    trades = load_trades_after_snapshot(cur, update_from)
    trade_dates = set()
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            trade_dates.add(d)
            c = (r.get("stock_code") or "").strip()
            n = (r.get("stock_name") or "").strip()
            if c and n:
                stock_name_map[c] = n
    # 支持“从 3/31 起全量重算”：即便后续没有交易，也要按行情日生成 row_type=2/3。
    min_d = min(trade_dates) if trade_dates else update_from
    max_d_trade = max(trade_dates) if trade_dates else update_from
    price_load_min = min(SNAPSHOT_DATE, min_d) - timedelta(days=PRICE_LOOKBACK_BEFORE_FIRST_TRADE_DAYS)
    max_price_d = get_latest_price_date(cur, update_from)
    max_d = max(max_d_trade, max_price_d) if max_price_d else max_d_trade

    price_map, price_name_map = load_prices_and_names(cur, price_load_min, max_d)
    for c, n in price_name_map.items():
        if c and n:
            stock_name_map[c] = n

    hs300_nav_map = build_hs300_nav_map(cur, price_map, SNAPSHOT_DATE, snapshot_hs300_nav)

    prices_by_date: Dict[date, Dict[str, Decimal]] = {}
    for (d0, c0), p0 in price_map.items():
        if d0 not in prices_by_date:
            prices_by_date[d0] = {}
        prices_by_date[d0][c0] = _d(p0)
    last_close: Dict[str, Decimal] = {}

    # 先用快照日行情填充 last_close，保障后续 ffill 起点正确
    day0 = prices_by_date.get(SNAPSHOT_DATE) or {}
    for code, px in day0.items():
        last_close[code] = px

    def _get_price_ffill(dt: date, code: str) -> Decimal:
        day = prices_by_date.get(dt)
        if day and code in day:
            last_close[code] = day[code]
        return last_close.get(code, Decimal("0"))

    trade_by_date: Dict[date, List[Dict[str, Any]]] = {}
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            trade_by_date.setdefault(d, []).append(r)

    def _lots_open_pct(code: str) -> Decimal:
        s = Decimal("0")
        for l in lots.get(code) or []:
            s += _d(l.get("pct_remain") or 0)
        return s

    def _recalc_positions_cost(code: str) -> None:
        sh = Decimal("0")
        cs = Decimal("0")
        for l in lots.get(code) or []:
            sh += _d(l.get("shares_remain") or 0)
            cs += _d(l.get("cost_remain") or 0)
        if sh <= 0:
            positions.pop(code, None)
            position_cost.pop(code, None)
            lots.pop(code, None)
        else:
            positions[code] = sh
            position_cost[code] = cs

    all_dates = sorted(
        d for d in (set(trade_by_date.keys()) | {dt for (dt, _) in price_map.keys()}) if update_from <= d <= max_d
    )
    if not all_dates:
        return []

    detail_rows: List[Dict[str, Any]] = []
    for dt in all_dates:
        day_trade_seq: Dict[Tuple[str, str], int] = {}

        def _trade_row_stock_code(code: str, side: str) -> str:
            key = (code, side)
            day_trade_seq[key] = int(day_trade_seq.get(key) or 0) + 1
            return f"{code}#{side}#{day_trade_seq[key]}"

        day_trades = sorted(trade_by_date.get(dt, []), key=lambda r: int(r.get("row_id") or 0))
        for row in day_trades:
            code = (row.get("stock_code") or "").strip()
            side = (row.get("side") or "").strip()
            stock_name = (row.get("stock_name") or "").strip() or stock_name_map.get(code) or None
            row_id = row.get("row_id")
            pct = float(row.get("position_pct") or 0)
            trade_price = float(row.get("price") or 0)
            if not code or trade_price <= 0:
                continue

            pos_before = positions.get(code, Decimal("0"))
            cash_before = cash
            capital_base = cash + (sum(position_cost.values()) if position_cost else Decimal("0"))
            target_amount = capital_base * _d(pct) / Decimal("100")
            trade_shares = Decimal("0")

            if side == "买入":
                price_d = _d(trade_price)
                shares = _q4_share(target_amount / price_d) if price_d > 0 else Decimal("0")
                if shares <= 0:
                    continue
                cost = _q2_money(shares * price_d)
                if cash < cost:
                    shares = _q4_share(cash / price_d)
                    if shares <= 0:
                        continue
                    cost = _q2_money(shares * price_d)
                cash -= cost
                lots.setdefault(code, []).append(
                    {"pct_remain": _d(pct), "shares_remain": shares, "cost_remain": cost}
                )
                _recalc_positions_cost(code)
                trade_shares = shares
            elif side == "卖出":
                if code not in positions or positions[code] <= 0:
                    continue
                price_d = _d(trade_price)
                sell_pct_left = _d(pct)
                sell_shares_total = Decimal("0")
                ls = lots.get(code) or []
                i = 0
                while i < len(ls) and sell_pct_left > 0:
                    lot = ls[i]
                    lot_pct = _d(lot.get("pct_remain") or 0)
                    if lot_pct <= 0:
                        i += 1
                        continue
                    if sell_pct_left >= lot_pct:
                        sell_shares_total += _d(lot.get("shares_remain") or 0)
                        sell_pct_left -= lot_pct
                        lot["pct_remain"] = Decimal("0")
                        lot["shares_remain"] = Decimal("0")
                        lot["cost_remain"] = Decimal("0")
                        i += 1
                    else:
                        ratio = sell_pct_left / lot_pct
                        lot_sh = _d(lot.get("shares_remain") or 0)
                        sell_sh = _q4_share(lot_sh * ratio)
                        if sell_sh > lot_sh:
                            sell_sh = lot_sh
                        lot_cs = _d(lot.get("cost_remain") or 0)
                        sell_cs = _q2_money(lot_cs * (sell_sh / lot_sh)) if lot_sh > 0 else Decimal("0")
                        lot["shares_remain"] = lot_sh - sell_sh
                        lot["cost_remain"] = lot_cs - sell_cs
                        lot["pct_remain"] = lot_pct - sell_pct_left
                        sell_shares_total += sell_sh
                        sell_pct_left = Decimal("0")
                        break
                if sell_shares_total <= 0:
                    continue
                cash += _q2_money(sell_shares_total * price_d)
                lots[code] = [
                    l
                    for l in lots.get(code, [])
                    if _d(l.get("shares_remain") or 0) > 0 and _d(l.get("pct_remain") or 0) > 0
                ]
                _recalc_positions_cost(code)
                trade_shares = -sell_shares_total
            else:
                continue

            detail_rows.append(
                {
                    "product_name": PRODUCT_NAME,
                    "biz_date": dt,
                    "row_type": 1,
                    "row_id": row_id,
                    "stock_code": _trade_row_stock_code(code, side),
                    "stock_name": stock_name,
                    "side": side,
                    "position_pct": pct,
                    "trade_price": trade_price,
                    "trade_shares": trade_shares,
                    "position_before": pos_before,
                    "position_after": positions.get(code, Decimal("0")),
                    "cash_before": float(cash_before),
                    "cash_after": float(cash),
                    "asset_no_float": float(capital_base),
                    "open_pct": float(_lots_open_pct(code)),
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

        total_mv = Decimal("0")
        per_stock_snapshot: List[Tuple[str, Decimal, Decimal, Decimal]] = []
        for code, sh in positions.items():
            px = _get_price_ffill(dt, code)
            mv = sh * px
            total_mv += mv
            per_stock_snapshot.append((code, sh, px, mv))

        total_asset = cash + total_mv
        nav = float(total_asset / portfolio_nav_denom) if portfolio_nav_denom > 0 else 0.0
        hs300_nav = hs300_nav_map.get(dt)

        for code, sh, px, mv in per_stock_snapshot:
            detail_rows.append(
                {
                    "product_name": PRODUCT_NAME,
                    "biz_date": dt,
                    "row_type": 2,
                    "row_id": None,
                    "stock_code": code,
                    "stock_name": stock_name_map.get(code) or None,
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
                    "asset_no_float": None,
                    "open_pct": float(_lots_open_pct(code)),
                    "total_asset": None,
                    "nav": None,
                    "hs300_nav": None,
                    "remark": None,
                }
            )

        total_open_pct = Decimal("0")
        for code in positions.keys():
            total_open_pct += _lots_open_pct(code)

        detail_rows.append(
            {
                "product_name": PRODUCT_NAME,
                "biz_date": dt,
                "row_type": 3,
                "row_id": None,
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
                "asset_no_float": float(cash + (sum(position_cost.values()) if position_cost else Decimal("0"))),
                "open_pct": float(total_open_pct),
                "total_asset": float(total_asset),
                "nav": float(nav),
                "hs300_nav": hs300_nav,
                "remark": None,
            }
        )

    return detail_rows


def write_rows(rows: List[Dict[str, Any]], update_from: date) -> None:
    if not rows:
        print("[INFO] 无新增行可写入")
        return

    biz_dates = [r.get("biz_date") for r in rows if r.get("biz_date") is not None]
    max_d = max(biz_dates) if biz_dates else None
    if max_d is None:
        print("[INFO] 无有效日期可写入")
        return

    sql = f"""
        INSERT INTO {DETAIL_TABLE}
        (
          product_name, biz_date, row_id, row_type,
          stock_code, stock_name,
          side, position_pct, trade_price, trade_shares,
          position_before, position_after,
          cash_before, cash_after,
          close_price, market_value,
          total_position_mv, total_cash, asset_no_float, open_pct, total_asset,
          nav, hs300_nav,
          remark, updated_at
        )
        VALUES (
          %s, %s, %s, %s,
          %s, %s,
          %s, %s, %s, %s,
          %s, %s,
          %s, %s,
          %s, %s,
          %s, %s, %s, %s, %s,
          %s, %s,
          %s, %s
        )
    """
    now_dt = datetime.now()
    vals = []
    for r in rows:
        r_fmt = dict(r)
        for k, v in list(r_fmt.items()):
            if k in ("nav", "hs300_nav", "row_id"):
                continue
            if isinstance(v, (int, float, Decimal)):
                r_fmt[k] = _q4_generic(v)
        vals.append(
            (
                r_fmt["product_name"],
                r_fmt["biz_date"],
                r_fmt.get("row_id"),
                r_fmt["row_type"],
                r_fmt["stock_code"],
                r_fmt["stock_name"],
                r_fmt["side"],
                r_fmt["position_pct"],
                r_fmt["trade_price"],
                r_fmt["trade_shares"],
                r_fmt["position_before"],
                r_fmt["position_after"],
                r_fmt["cash_before"],
                r_fmt["cash_after"],
                r_fmt["close_price"],
                r_fmt["market_value"],
                r_fmt["total_position_mv"],
                r_fmt["total_cash"],
                r_fmt.get("asset_no_float"),
                r_fmt.get("open_pct"),
                r_fmt["total_asset"],
                r_fmt["nav"],
                r_fmt["hs300_nav"],
                r_fmt["remark"],
                now_dt,
            )
        )

    with get_conn() as conn, conn.cursor() as cur:
        del_sql = f"""
            DELETE FROM {DETAIL_TABLE}
            WHERE product_name = %s
              AND biz_date >= %s
              AND biz_date <= %s
        """
        cur.execute(del_sql, (PRODUCT_NAME, update_from, max_d))
        cur.executemany(sql, vals)
        conn.commit()
    print(f"[OK] {PRODUCT_NAME} 写入行数: {len(rows)} 区间: [{update_from}, {max_d}]")


def main() -> None:
    update_from = _cli_parse_update_from()
    if update_from <= SNAPSHOT_DATE:
        raise ValueError(f"from-date 必须大于快照日 {SNAPSHOT_DATE}")
    print(f"[RUN] {PRODUCT_NAME} 从快照 {SNAPSHOT_DATE} 续算，增量起点 {update_from}")
    with get_conn() as conn, conn.cursor() as cur:
        rows = compute_from_snapshot(cur, update_from)
    write_rows(rows, update_from)


if __name__ == "__main__":
    main()

