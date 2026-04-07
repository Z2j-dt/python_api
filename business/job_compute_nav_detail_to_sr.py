# -*- coding: utf-8 -*-
"""
按 temp3.py 口径，从 StarRocks:
  - config_stock_position  (交易)
  - hsgt_price_deliver     (收盘价，含个股+沪深300=000300)
计算指定产品每日持仓明细 + 总资产 + 净值，
写入 product_nav_daily_detail 表（含每只股票明细 + 当日总计行）。

净值基期（固定）：以「首笔操作交易日」的上一交易日为基日（如首操作为 2025-12-01 则基期为 2025-11-28），
该基日组合净值与沪深300净值均为 1；之后组合净值 = 当日总资产 / 基日收盘总资产，300 = 当日收盘 / 基日收盘。
基期只由最早一笔 config 交易日期决定，全序列不变。

增量写入：命令行传入 --from-date / -f，值为 YYYYMMDD（如 20250301）或 YYYY-MM-DD；
仍从基期起回放持仓状态，但只生成并 DELETE+INSERT 该日（含）至行情末日的明细行。
"""

import argparse
import pymysql
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN, getcontext

# ===== StarRocks 连接配置 =====
SR_HOST = "10.8.93.40"
SR_PORT = 9030
SR_USER = "root"
SR_PASSWORD = "star@dt1988"
SR_DB = "portal_db"

CONFIG_STOCK_POSITION_TABLE = "config_stock_position"
HSGT_PRICE_TABLE = "hsgt_price_deliver"
DETAIL_TABLE = "product_nav_daily_detail"
# 每个产品的初始资金配置表：product_name, init_capital
PRODUCT_CAPITAL_TABLE = "config_product_nav_capital"
MANUAL_NAV_PRODUCTS = {"同赢先锋"}

NAV_INIT_CAPITAL = 10_000_000  # 默认初始资金 1000 万（配置表查不到时回退）
HS300_CODE_DB = "000300"       # 沪深300 在价格表里的代码（标准写法）
# 库里历史数据可能混用多种写法；与字面量不等会导致「上一交易日」查不到、净值基期退化成首笔交易日=1
HS300_CODE_SQL_IN = ("000300", "000300.SH", "SH000300", "sh000300")
# 首笔交易日之前向下多取的自然日，保证 000300 在「上一交易日」的行进入 price_map
# （若仅依赖 SQL 求 anchor 一旦未命中，原先会把 price_load_min=首笔日，整段历史 300 被滤掉 → 基期=首日 → 净值恒为 1）
PRICE_LOOKBACK_BEFORE_FIRST_TRADE_DAYS = 400
PRODUCT_NAME = "短线王"        # 要计算的产品名（可按需改成其他产品）

getcontext().prec = 28


def _d(v) -> Decimal:
    return Decimal(str(v))


def _match_hs300_code(stock_code: Optional[str]) -> bool:
    """判定价格表里的 stock_code 是否沪深300（放宽匹配以适配不同编码格式）。"""
    if not stock_code:
        return False
    c = stock_code.strip().upper()
    return "000300" in c


def _hs300_priority(stock_code: str) -> int:
    """同一天多条 000300 口径时，优先选择更“标准”的编码。"""
    c = (stock_code or "").strip().upper()
    if c == "000300":
        return 0
    if c == "000300.SH":
        return 1
    if c == "SH000300":
        return 2
    # 兼容其它前缀/后缀写法：只要包含000300，就放在最后
    if "000300" in c:
        return 10
    return 1000


def _q2(x: Decimal) -> Decimal:
    # 份额/股数保留两位小数，向下取整（避免超买/超卖）
    return x.quantize(Decimal("0.00"), rounding=ROUND_DOWN)


def _q2_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def _q4_share(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


def _q4_generic(x: Any) -> Any:
    """
    将数值统一为最多 4 位小数（向下取整），
    用于除 nav / hs300_nav 以外的数值字段在结果表中的展示精度。
    """
    if x is None:
        return None
    try:
        d = Decimal(str(x))
        return d.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    except Exception:
        return x


def load_init_capital(cur, product_name: str) -> Decimal:
    """
    按产品名称从配置表获取初始资金；若查不到或配置异常则回退到 NAV_INIT_CAPITAL。
    """
    try:
        sql = f"""
            SELECT init_capital
            FROM {PRODUCT_CAPITAL_TABLE}
            WHERE product_name = %s
            LIMIT 1
        """
        cur.execute(sql, (product_name,))
        row = cur.fetchone() or {}
        val = row.get("init_capital")
        if val is None:
            raise ValueError("empty init_capital")
        d = Decimal(str(val))
        if d <= 0:
            raise ValueError("non-positive init_capital")
        return d
    except Exception:
        return Decimal(str(NAV_INIT_CAPITAL))


def load_products_capital(cur) -> List[Tuple[str, Decimal]]:
    """
    从配置表加载所有产品及其初始资金。
    返回: [(product_name, init_capital_decimal), ...]
    """
    sql = f"""
        SELECT product_name, init_capital
        FROM {PRODUCT_CAPITAL_TABLE}
        WHERE product_name IS NOT NULL
          AND product_name != ''
          AND init_capital IS NOT NULL
    """
    cur.execute(sql)
    rows = cur.fetchall() or []
    out: List[Tuple[str, Decimal]] = []
    for r in rows:
        name = (r.get("product_name") or "").strip()
        if not name:
            continue
        try:
            cap = Decimal(str(r.get("init_capital")))
        except Exception:
            continue
        if cap <= 0:
            continue
        out.append((name, cap))
    # 去重：同名取第一条
    seen = set()
    dedup: List[Tuple[str, Decimal]] = []
    for name, cap in out:
        if name in seen:
            continue
        seen.add(name)
        dedup.append((name, cap))
    return dedup

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
        SELECT trade_date, row_id, stock_code, stock_name, side, position_pct, price
        FROM {CONFIG_STOCK_POSITION_TABLE}
        WHERE product_name = %s
          AND trade_date IS NOT NULL
          AND stock_code IS NOT NULL
          AND side IS NOT NULL
        ORDER BY trade_date ASC, row_id ASC, id ASC
    """
    cur.execute(sql, (product_name,))
    return cur.fetchall()


def get_first_trade_date(cur, product_name: str) -> Optional[date]:
    """
    直接从交易配置表取该产品首个有效交易日（最小 trade_date）。
    这里与 load_trades 保持同口径过滤，避免基期来源不一致。
    """
    sql = f"""
        SELECT MIN(trade_date) AS first_trade_date
        FROM {CONFIG_STOCK_POSITION_TABLE}
        WHERE product_name = %s
          AND trade_date IS NOT NULL
          AND stock_code IS NOT NULL
          AND side IS NOT NULL
    """
    try:
        cur.execute(sql, (product_name,))
        row = cur.fetchone() or {}
        return _parse_date(row.get("first_trade_date"))
    except Exception:
        return None


def load_prices_and_names(
    cur, min_d: date, max_d: date
) -> Tuple[Dict[Tuple[date, str], float], Dict[str, str]]:
    """
    从价格表加载区间内收盘价，并尽可能补全 stock_name。

    - price_map: (biz_date, stock_code) -> last_price
    - name_map: stock_code -> stock_name（取区间内最新一条非空名称）
    """
    rows: List[Dict[str, Any]] = []
    try:
        sql = f"""
            SELECT biz_date, stock_code, stock_name, last_price
            FROM {HSGT_PRICE_TABLE}
            WHERE biz_date IS NOT NULL
              AND last_price IS NOT NULL
        """
        cur.execute(sql)
        rows = cur.fetchall()
    except Exception:
        # 兼容旧表结构：没有 stock_name 列
        sql = f"""
            SELECT biz_date, stock_code, last_price
            FROM {HSGT_PRICE_TABLE}
            WHERE biz_date IS NOT NULL
              AND last_price IS NOT NULL
        """
        cur.execute(sql)
        rows = cur.fetchall()

    price_map: Dict[Tuple[date, str], float] = {}
    name_map_latest: Dict[str, Tuple[date, str]] = {}

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

        nm = (r.get("stock_name") or "").strip() if isinstance(r, dict) else ""
        if nm:
            prev = name_map_latest.get(code)
            if prev is None or dt >= prev[0]:
                name_map_latest[code] = (dt, nm)

    name_map: Dict[str, str] = {code: nm for code, (_, nm) in name_map_latest.items()}
    return price_map, name_map


def get_latest_price_date(cur, min_d: date) -> Optional[date]:
    """
    从价格表取 >= min_d 的最新 biz_date。
    用于解决“当日无交易但有行情/净值”的场景：也要生成当日净值。
    """
    sql = f"""
        SELECT MAX(biz_date) AS max_d
        FROM {HSGT_PRICE_TABLE}
        WHERE biz_date IS NOT NULL
          AND biz_date >= %s
          AND last_price IS NOT NULL
    """
    try:
        cur.execute(sql, (min_d,))
        row = cur.fetchone() or {}
        return _parse_date(row.get("max_d"))
    except Exception:
        return None


def get_hs300_prev_trading_date(cur, first_trade_date: date) -> Optional[date]:
    """
    取「config 中首笔操作日」的前一个沪深300有收盘价的交易日。
    该日即本产品固定净值基期（不变）：组合与沪深300 均以此为分母，当日净值=1。
    """
    ph = ",".join(["%s"] * len(HS300_CODE_SQL_IN))
    sql = f"""
        SELECT MAX(biz_date) AS prev_d
        FROM {HSGT_PRICE_TABLE}
        WHERE biz_date IS NOT NULL
          AND biz_date < %s
          AND stock_code IN ({ph})
          AND last_price IS NOT NULL
    """
    try:
        cur.execute(sql, (first_trade_date,) + tuple(HS300_CODE_SQL_IN))
        row = cur.fetchone() or {}
        return _parse_date(row.get("prev_d"))
    except Exception:
        return None


def fetch_hs300_close_on_date(cur, biz_d: date) -> Optional[Decimal]:
    """指定日取沪深300收盘价（多代码兼容）；全表扫描 load 仍缺该日时补一条。"""
    ph = ",".join(["%s"] * len(HS300_CODE_SQL_IN))
    sql = f"""
        SELECT stock_code, last_price
        FROM {HSGT_PRICE_TABLE}
        WHERE biz_date = %s
          AND last_price IS NOT NULL
          AND stock_code LIKE %s
        LIMIT 50
    """
    try:
        cur.execute(sql, (biz_d, "%000300%"))
        rows = cur.fetchall() or []
        best = None
        best_pr = None
        for r in rows:
            sc = (r.get("stock_code") or "").strip().upper()
            val = r.get("last_price")
            if val is None:
                continue
            try:
                pr = _d(val)
            except Exception:
                continue
            if pr <= 0:
                continue
            if best_pr is None or _hs300_priority(sc) < _hs300_priority(best or ""):
                best_pr = pr
                best = sc
        return best_pr if best_pr is not None else None
    except Exception:
        return None


def compute_daily_detail(
    product_name: str, cur, update_from: Optional[date] = None
) -> List[Dict[str, Any]]:
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
    # 首个交易日优先从表中直接获取，基期锚点始终基于表内原始数据确定。
    first_trade_d = get_first_trade_date(cur, product_name)
    min_d = first_trade_d if first_trade_d is not None else min(trade_dates)
    max_d_trade = max(trade_dates)
    # 基期规则（按业务约定）：
    # 直接取产品首个交易日的自然日前一天，不再按沪深300交易日回溯。
    sql_anchor = min_d - timedelta(days=1)
    price_floor = min_d - timedelta(days=PRICE_LOOKBACK_BEFORE_FIRST_TRADE_DAYS)
    price_load_min = price_floor
    if sql_anchor is not None and sql_anchor < min_d:
        price_load_min = min(price_load_min, sql_anchor)
    # 关键修复：当日无交易但价格表已有当日数据时，也要把区间延伸到最新价格日（通常=今天）
    max_price_d = get_latest_price_date(cur, min_d)
    max_d = max_d_trade
    if max_price_d and max_price_d > max_d:
        max_d = max_price_d

    # 价格 + 股票名称（名称优先用价格表，若缺失再用交易配置表）
    price_map, price_name_map = load_prices_and_names(cur, price_load_min, max_d)

    # 基期固定为首个交易日的自然日前一天（不做交易日校正）
    anchor_d: Optional[date] = sql_anchor
    print(
        f"[INFO] product={product_name} first_trade_date={min_d} "
        f"sql_anchor={sql_anchor} anchor_d={anchor_d}"
    )
    trade_name_map: Dict[str, str] = {}
    for r in trades:
        code = (r.get("stock_code") or "").strip()
        nm = (r.get("stock_name") or "").strip()
        if code and nm:
            trade_name_map[code] = nm
    stock_name_map: Dict[str, str] = dict(trade_name_map)
    for c, n in price_name_map.items():
        if c and n:
            stock_name_map[c] = n

    price_dates = {dt for (dt, _) in price_map.keys()}
    all_dates = sorted(d for d in (trade_dates | price_dates) if min_d <= d <= max_d)
    # 基期日必须进序列：即便个股价在当日无行，仍有全现金 + 沪深300 基准可分母
    if anchor_d is not None and anchor_d < min_d:
        all_dates = [anchor_d] + all_dates
    if not all_dates:
        return []

    # 按日期分组交易
    trade_by_date: Dict[date, List[Dict[str, Any]]] = {}
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            trade_by_date.setdefault(d, []).append(r)

    # temp3.py 口径：收盘价按交易日序列前向填充（ffill）
    prices_by_date: Dict[date, Dict[str, Decimal]] = {}
    for (d0, c0), p0 in price_map.items():
        if d0 not in prices_by_date:
            prices_by_date[d0] = {}
        try:
            prices_by_date[d0][c0] = _d(p0)
        except Exception:
            pass
    last_close: Dict[str, Decimal] = {}

    def _get_price_ffill(dt: date, code: str) -> Decimal:
        day = prices_by_date.get(dt)
        if day and code in day:
            last_close[code] = day[code]
        return last_close.get(code, Decimal("0"))

    # 沪深300净值：基期=anchor_d当日（若该日有 000300 收盘价），否则取该日前最近一个有值的日期。
    hs300_prices: Dict[date, Decimal] = {}
    hs300_code_for_date: Dict[date, str] = {}
    for (d, c), p in price_map.items():
        if not _match_hs300_code(c):
            continue
        try:
            pr = _d(p)
        except Exception:
            continue
        cur_code = (c or "").strip().upper()
        if d not in hs300_prices:
            hs300_prices[d] = pr
            hs300_code_for_date[d] = cur_code
        else:
            # 同一天多条编码时，优先选择标准编码（如 '000300'）
            if _hs300_priority(cur_code) < _hs300_priority(hs300_code_for_date.get(d, "")):
                hs300_prices[d] = pr
                hs300_code_for_date[d] = cur_code
    if anchor_d is not None and anchor_d not in hs300_prices:
        px_anchor = fetch_hs300_close_on_date(cur, anchor_d)
        if px_anchor is not None:
            hs300_prices[anchor_d] = px_anchor
    hs300_dates = sorted(hs300_prices.keys())
    hs300_nav_map: Dict[date, float] = {}
    base_d: Optional[date] = None
    base_price: Optional[Decimal] = None
    anchor_d_before_hs300: Optional[date] = anchor_d
    if hs300_dates:
        if anchor_d is not None:
            # 优先用 anchor_d 对应的 hs300 收盘价；若取不到则用最近一条 <= anchor_d
            candidates = [d for d in hs300_dates if d <= anchor_d]
            if candidates:
                base_d = candidates[-1]
            else:
                base_d = hs300_dates[0]
        else:
            base_d = hs300_dates[0]
        base_price = hs300_prices.get(base_d)
    # 若 anchor_d 当天没有 000300 值，则把“组合基期起点”也回退到最近的可用基准日
    # （这样 row_type=3 的初始净值/hs300_nav 才会对齐你的口径）
    if (
        anchor_d_before_hs300 is not None
        and base_d is not None
        and base_d != anchor_d_before_hs300
    ):
        anchor_d = base_d
        all_dates = sorted(d for d in (trade_dates | price_dates) if anchor_d <= d <= max_d)
        if anchor_d not in all_dates:
            all_dates = [anchor_d] + all_dates

    print(
        f"[INFO] product={product_name} hs300_anchor_d={anchor_d_before_hs300} "
        f"hs300_base_d={base_d} hs300_base_price={base_price} "
        f"effective_anchor_d={anchor_d} hs300_dates_count={len(hs300_dates)}"
    )
    if base_d is not None and base_price is not None and base_price > 0:
        for d in hs300_dates:
            if d >= base_d:
                hs300_nav_map[d] = float(hs300_prices[d] / base_price)

    # 按产品获取初始资金（支持不同产品不同资金规模）
    init_capital = load_init_capital(cur, product_name)
    cash = init_capital
    # shares：不要求 100 股整数，保留 4 位小数
    positions: Dict[str, Decimal] = {}
    # 成本口径持仓成本（用于“可用总资产”= cash + Σ成本，不含浮动盈亏；卖出实现盈亏体现在 cash 变化）
    position_cost: Dict[str, Decimal] = {}
    # 分批持仓（FIFO）：每次买入生成一个 lot；卖出按最早 lot 优先扣减。
    # lot: {pct_remain, shares_remain, cost_remain}
    lots: Dict[str, List[Dict[str, Decimal]]] = {}
    # 组合净值分母 = 基期日收盘总资产（固定）；仅当无基期插入逻辑时退回 init_capital
    portfolio_nav_denom: Optional[Decimal] = None

    def _lots_open_pct(code: str) -> Decimal:
        ls = lots.get(code) or []
        s = Decimal("0")
        for l in ls:
            try:
                s += _d(l.get("pct_remain") or 0)
            except Exception:
                pass
        return s

    def _recalc_positions_cost(code: str) -> None:
        ls = lots.get(code) or []
        sh = Decimal("0")
        cs = Decimal("0")
        for l in ls:
            sh += _d(l.get("shares_remain") or 0)
            cs += _d(l.get("cost_remain") or 0)
        if sh <= 0:
            positions.pop(code, None)
            position_cost.pop(code, None)
            lots.pop(code, None)
        else:
            positions[code] = sh
            position_cost[code] = cs
    detail_rows: List[Dict[str, Any]] = []

    for dt in all_dates:
        # 增量重算也要保留“基期日”：
        # 基期按“该产品首个交易日的上一个交易日”确定（anchor_d），
        # 这样每个产品都能在净值序列中有自己的起点（通常为 1 / 1）。
        emit = (
            update_from is None
            or dt >= update_from
            or (anchor_d is not None and dt == anchor_d)
        )
        # row_type=1 需要逐笔保留（同一天同一股票可多次买/卖，且成交价/金额不同不可聚合）。
        # 但 SR 侧常见唯一键为 (product_name, biz_date, row_type, stock_code)，因此这里为
        # 每笔交易生成一个“不会重复”的 stock_code："{原代码}#{买入/卖出}#{序号}"。
        day_trade_seq: Dict[Tuple[str, str], int] = {}

        def _trade_row_stock_code(code: str, side: str) -> str:
            key = (code, side)
            day_trade_seq[key] = int(day_trade_seq.get(key) or 0) + 1
            return f"{code}#{side}#{day_trade_seq[key]}" if side else f"{code}#{day_trade_seq[key]}"

        # ===== 1. 执行当日每一笔交易（按 trade_date,row_id 顺序）=====
        day_trades = trade_by_date.get(dt, [])
        day_trades = sorted(day_trades, key=lambda r: int(r.get("row_id") or 0))
        for row in day_trades:
            code = (row.get("stock_code") or "").strip()
            side = (row.get("side") or "").strip()
            stock_name = (row.get("stock_name") or "").strip() or stock_name_map.get(code) or None
            row_id = row.get("row_id")
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
            # 下单资金基数：初始资金 + 已实现盈亏（不含未卖出的浮动盈亏）
            capital_base = cash + (sum(position_cost.values()) if position_cost else Decimal("0"))
            target_amount = capital_base * _d(pct) / Decimal("100")
            trade_shares = Decimal("0")

            if side == "买入":
                price_d = _d(trade_price)
                if price_d <= 0:
                    continue
                shares = _q4_share(target_amount / price_d)
                if shares <= 0:
                    continue
                cost = _q2_money(shares * price_d)
                if cash < cost:
                    shares = _q4_share(cash / price_d)
                    if shares <= 0:
                        continue
                    cost = _q2_money(shares * price_d)
                cash = cash - cost
                lots.setdefault(code, []).append(
                    {
                        "pct_remain": _d(pct),
                        "shares_remain": shares,
                        "cost_remain": cost,
                    }
                )
                _recalc_positions_cost(code)
                trade_shares = shares  # 买入为正

            elif side == "卖出":
                if code not in positions or positions[code] <= 0:
                    continue
                price_d = _d(trade_price)
                if price_d <= 0:
                    continue
                # FIFO 按“最早买入的仓位%”优先扣减：
                # 例如先买10%再买5%，卖5% => 只从第一笔(10%)扣一半份额。
                sell_pct_left = _d(pct)
                sell_shares_total = Decimal("0")
                realized_cost_total = Decimal("0")
                ls = lots.get(code) or []
                if not ls:
                    continue
                i = 0
                while i < len(ls) and sell_pct_left > 0:
                    lot = ls[i]
                    lot_pct = _d(lot.get("pct_remain") or 0)
                    if lot_pct <= 0:
                        i += 1
                        continue
                    if sell_pct_left >= lot_pct:
                        # 卖光该 lot
                        sell_sh = _d(lot.get("shares_remain") or 0)
                        sell_cs = _d(lot.get("cost_remain") or 0)
                        sell_shares_total += sell_sh
                        realized_cost_total += sell_cs
                        sell_pct_left -= lot_pct
                        lot["pct_remain"] = Decimal("0")
                        lot["shares_remain"] = Decimal("0")
                        lot["cost_remain"] = Decimal("0")
                        i += 1
                    else:
                        # 在该 lot 内按比例卖出
                        ratio = sell_pct_left / lot_pct
                        lot_sh = _d(lot.get("shares_remain") or 0)
                        lot_cs = _d(lot.get("cost_remain") or 0)
                        sell_sh = _q4_share(lot_sh * ratio)
                        if sell_sh > lot_sh:
                            sell_sh = lot_sh
                        # 成本按份额比例扣（保持与该 lot 份额一致）
                        sell_cs = _q2_money(lot_cs * (sell_sh / lot_sh)) if lot_sh > 0 else Decimal("0")
                        lot["shares_remain"] = lot_sh - sell_sh
                        lot["cost_remain"] = lot_cs - sell_cs
                        lot["pct_remain"] = lot_pct - sell_pct_left
                        sell_shares_total += sell_sh
                        realized_cost_total += sell_cs
                        sell_pct_left = Decimal("0")
                        break

                if sell_shares_total <= 0:
                    continue
                # 卖出收入
                revenue = _q2_money(sell_shares_total * price_d)
                cash = cash + revenue
                # 刷新汇总持仓与成本
                # 清理空 lot
                lots[code] = [l for l in lots.get(code, []) if _d(l.get("shares_remain") or 0) > 0 and _d(l.get("pct_remain") or 0) > 0]
                _recalc_positions_cost(code)
                trade_shares = -sell_shares_total

            else:
                continue

            pos_after = positions.get(code, Decimal("0"))
            cash_after = cash
            open_pct_after = _lots_open_pct(code) if code in lots else None

            # row_type = 1：逐笔交易明细（保留每笔，不做聚合）
            if emit:
                detail_rows.append(
                    {
                        "product_name": product_name,
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
                        "position_after": pos_after,
                        "cash_before": float(cash_before),
                        "cash_after": float(cash_after),
                        # temp3.py 不计算该指标；这里用“下单前现金”作参考
                        "asset_no_float": float(capital_base),
                        "open_pct": float(open_pct_after) if open_pct_after is not None else None,
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
            px = _get_price_ffill(dt, code)
            mv = sh * px
            total_mv += mv
            per_stock_snapshot.append((code, sh, px, mv))

        # 资产口径：总资产 = 现金 + 持仓市值（净值需要包含浮动盈亏）
        total_asset = cash + total_mv
        if anchor_d is not None and dt == anchor_d:
            portfolio_nav_denom = total_asset
        denom = (
            portfolio_nav_denom
            if portfolio_nav_denom is not None and portfolio_nav_denom > 0
            else init_capital
        )
        nav = float(total_asset / denom) if denom > 0 else 0.0
        hs300_nav = hs300_nav_map.get(dt)

        # row_type = 2：当日每只股票的持仓快照
        if emit:
            for code, sh, px, mv in per_stock_snapshot:
                detail_rows.append(
                    {
                        "product_name": product_name,
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
                        # row_type=2 需要展示“仓位(按股票)”：这里用当日结束时该股票仍持有的 open_pct。
                        "open_pct": float(_lots_open_pct(code)),
                        "total_asset": None,
                        "nav": None,
                        "hs300_nav": None,
                        "remark": None,
                    }
                )

            # row_type = 3：当日总资产汇总行
            # 汇总当日组合总仓位：对所有仍持有的股票求 open_pct 之和
            total_open_pct = Decimal("0")
            for code in positions.keys():
                try:
                    total_open_pct += _lots_open_pct(code)
                except Exception:
                    pass

            detail_rows.append(
                {
                    "product_name": product_name,
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
                    "asset_no_float": float(
                        cash + (sum(position_cost.values()) if position_cost else Decimal("0"))
                    ),
                    "open_pct": float(total_open_pct) if total_open_pct is not None else None,
                    "total_asset": float(total_asset),
                    "nav": float(nav),
                    "hs300_nav": hs300_nav,
                    "remark": None,
                }
            )

    return detail_rows


def write_detail_to_sr(rows: List[Dict[str, Any]], update_from: Optional[date] = None):
    if not rows:
        print("[INFO] 无明细数据可写入")
        return

    # 为了保证任务可重复执行：先删除本次计算日期区间内的旧数据，避免多跑叠加导致“多出股票/重复行”
    product_name = rows[0].get("product_name")
    if str(product_name or "").strip() in MANUAL_NAV_PRODUCTS:
        print(f"[SKIP] 产品={product_name} 为手工净值维护产品，禁止批量任务写入/删除。")
        return
    biz_dates = [r.get("biz_date") for r in rows if r.get("biz_date") is not None]
    min_d = min(biz_dates) if biz_dates else None
    max_d = max(biz_dates) if biz_dates else None

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
        # 除 nav / hs300_nav / row_id 这些字段外，其余数值统一为 4 位小数精度
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
        if product_name:
            if update_from is None:
                # 全量：先清空该产品历史，避免旧基期/旧口径残留
                del_sql = f"""
                    DELETE FROM {DETAIL_TABLE}
                    WHERE product_name = %s
                """
                cur.execute(del_sql, (product_name,))
            elif max_d:
                # 增量：删除区间起点必须严格使用 update_from。
                # 说明：rows 里可能额外包含“锚点日”（早于 update_from），若用 min(rows.biz_date)
                # 会误删 update_from 之前的历史数据。
                del_start = update_from
                if del_start is None and min_d is not None:
                    # 理论上不会进入（上面已分支 update_from is None），仅保底
                    del_start = min_d
                if del_start is None:
                    del_start = max_d
                del_sql = f"""
                    DELETE FROM {DETAIL_TABLE}
                    WHERE product_name = %s
                      AND biz_date >= %s
                      AND biz_date <= %s
                """
                cur.execute(del_sql, (product_name, del_start, max_d))
        cur.executemany(sql, vals)
        conn.commit()
    print(f"[OK] 写入 {DETAIL_TABLE} 行数: {len(rows)}")


def main(update_from: Optional[date] = None):
    print(f"开始批量计算产品净值明细（从 {PRODUCT_CAPITAL_TABLE} 读取产品与初始资金）...")
    if update_from is not None:
        print(f"[INFO] 增量写入：仅 DELETE+INSERT biz_date >= {update_from}（此前仍完整回放持仓状态）")
    total_written = 0
    with get_conn() as conn, conn.cursor() as cur:
        products = load_products_capital(cur)
        if not products:
            print(f"[WARN] {PRODUCT_CAPITAL_TABLE} 中未找到有效产品配置，任务结束。")
            return
        for name, cap in products:
            # 同赢先锋的历史净值已通过人工方式写入 product_nav_daily_detail，
            # 后续将通过单独脚本从 2026-03-31 起增量续算，这里先跳过，避免被全量/增量任务覆盖。
            if name.strip() in MANUAL_NAV_PRODUCTS:
                print(f"[SKIP] 产品={name} 为手工净值维护产品，跳过本次批量任务。")
                continue
            # compute_daily_detail 内部仍会按 name 再查一次资金；这里先确保表里有配置即可
            print(f"[RUN] 产品={name} 初始资金={cap}")
            rows = compute_daily_detail(name, cur, update_from=update_from)
            if not rows:
                print(f"[SKIP] 产品={name} 无交易数据或增量区间内无可写日期")
                continue
            print(f"[INFO] 产品={name} 计算得到明细行数: {len(rows)}")
            write_detail_to_sr(rows, update_from=update_from)
            total_written += len(rows)
    print(f"[OK] 批量任务完成，总写入行数: {total_written}")


def _parse_date_arg(raw: str) -> date:
    """CLI 日期：优先 YYYYMMDD，兼容 YYYY-MM-DD。"""
    s = raw.strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    raise ValueError(f"无法解析日期: {raw!r}，请使用 YYYYMMDD 或 YYYY-MM-DD")


def _cli_parse_update_from() -> Optional[date]:
    parser = argparse.ArgumentParser(description="产品净值明细写入 product_nav_daily_detail")
    parser.add_argument(
        "-f",
        "--from-date",
        dest="from_date",
        metavar="YYYYMMDD",
        help="从该日起重算并落库至行情末日，格式 YYYYMMDD（如 20250301）或 YYYY-MM-DD；不传则全量",
    )
    args = parser.parse_args()
    raw = (args.from_date or "").strip()
    if not raw:
        return None
    return _parse_date_arg(raw)


if __name__ == "__main__":
    main(update_from=_cli_parse_update_from())