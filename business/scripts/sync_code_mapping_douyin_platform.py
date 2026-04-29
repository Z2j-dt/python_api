# -*- coding: utf-8 -*-
"""
同步最新一天的投流主体ID到配置表 code_mapping_adid_platform

规则：
1) platform='腾讯'：取 portal_db.txads_full_daily 表 day 最新一天数据，将 account_id 写入新表 id
2) platform='抖音'：取 portal_db.advertiser_data 表 biz_date 最新一天数据，将 advertiser_id 写入新表 id
3) platform='快手'：取 portal_db.ksads_daily 表 day 最新一天数据，将 advertiser_id 写入新表 id

默认模式：replace（按 platform 覆盖写入：先删该平台旧数据，再插入最新列表）
也支持：append（只插入不存在的 id，不删旧数据）
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Iterable, List, Optional, Sequence

import pymysql


TARGET_TABLE = "code_mapping_adid_platform"

SR_HOST = os.environ.get("STARROCKS_HOST", "127.0.0.1")
SR_PORT = int(os.environ.get("STARROCKS_PORT", "9030"))
SR_USER = os.environ.get("STARROCKS_USER", "root")
SR_PASSWORD = os.environ.get("STARROCKS_PASSWORD", "")
SR_DB = os.environ.get("STARROCKS_DATABASE", "portal_db")
PORTAL_DB = os.environ.get("PORTAL_DB", "portal_db")


def get_conn():
    return pymysql.connect(
        host=SR_HOST,
        port=SR_PORT,
        user=SR_USER,
        password=SR_PASSWORD,
        database=SR_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def _max_scalar(cur, sql: str, params: Sequence[object]) -> Optional[object]:
    cur.execute(sql, tuple(params))
    row = cur.fetchone() or {}
    # PyMySQL DictCursor 下，取第一个 value（列名不确定时用）
    for _, v in row.items():
        return v
    return None


def _to_int_list(values: Iterable[object]) -> List[int]:
    out: List[int] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except Exception:
            # 无法转换的 id 直接跳过
            continue
    # 去重保持稳定
    seen = set()
    uniq: List[int] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def fetch_latest_ids_for_platform(cur) -> dict:
    now_sql = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _ = now_sql  # 仅占位，便于调试打印

    out: dict = {}

    # 腾讯
    latest_tx_day = _max_scalar(
        cur,
        f"SELECT MAX(day) AS latest_day FROM `{PORTAL_DB}`.`txads_full_daily`",
        [],
    )
    if latest_tx_day is not None:
        cur.execute(
            """
            SELECT DISTINCT account_id AS id
            FROM `{PORTAL_DB}`.`txads_full_daily`
            WHERE day = %s AND account_id IS NOT NULL
            """,
            (latest_tx_day,),
        )
        ids = _to_int_list((r or {}).get("id") for r in cur.fetchall())
        out["腾讯"] = {"latest_day": latest_tx_day, "ids": ids}

    # 抖音
    latest_dy_biz_date = _max_scalar(
        cur,
        f"SELECT MAX(biz_date) AS latest_biz_date FROM `{PORTAL_DB}`.`advertiser_data`",
        [],
    )
    if latest_dy_biz_date is not None:
        cur.execute(
            """
            SELECT DISTINCT advertiser_id AS id
            FROM `{PORTAL_DB}`.`advertiser_data`
            WHERE biz_date = %s AND advertiser_id IS NOT NULL
            """,
            (latest_dy_biz_date,),
        )
        ids = _to_int_list((r or {}).get("id") for r in cur.fetchall())
        out["抖音"] = {"latest_biz_date": latest_dy_biz_date, "ids": ids}

    # 快手
    latest_ks_day = _max_scalar(
        cur,
        f"SELECT MAX(day) AS latest_day FROM `{PORTAL_DB}`.`ksads_daily`",
        [],
    )
    if latest_ks_day is not None:
        cur.execute(
            """
            SELECT DISTINCT advertiser_id AS id
            FROM `{PORTAL_DB}`.`ksads_daily`
            WHERE day = %s AND advertiser_id IS NOT NULL
            """,
            (latest_ks_day,),
        )
        ids = _to_int_list((r or {}).get("id") for r in cur.fetchall())
        out["快手"] = {"latest_day": latest_ks_day, "ids": ids}

    return out


def replace_platform(cur, platform: str, ids: List[int], now_str: str, dry_run: bool = False) -> int:
    if not ids:
        return 0

    if not dry_run:
        cur.execute(f"DELETE FROM `{TARGET_TABLE}` WHERE platform = %s", (platform,))

    # code_value 在表定义里 NOT NULL；这里用空字符串表示“来自自动同步”
    has_ut = False
    try:
        cur.execute(f"DESCRIBE `{TARGET_TABLE}`")
        cols = cur.fetchall() or []
        col_names = {str((c or {}).get("Field") or (c or {}).get("field") or (c or {}).get("name") or "").strip().lower() for c in cols}
        has_ut = "updated_time" in col_names
    except Exception:
        has_ut = False

    if has_ut:
        insert_sql = f"""
            INSERT INTO `{TARGET_TABLE}` (platform, id, code_value, description, stat_cost, channel_name, created_time, updated_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [(platform, x, "", None, None, "自动导入", now_str, now_str) for x in ids]
    else:
        insert_sql = f"""
            INSERT INTO `{TARGET_TABLE}` (platform, id, code_value, description, stat_cost, channel_name, created_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        rows = [(platform, x, "", None, None, "自动导入", now_str) for x in ids]
    if not dry_run:
        cur.executemany(insert_sql, rows)
    return len(ids)


def append_platform(cur, platform: str, ids: List[int], now_str: str, dry_run: bool = False) -> int:
    if not ids:
        return 0

    # 找出不存在的 (platform,id)
    cur.execute(f"SELECT id FROM `{TARGET_TABLE}` WHERE platform = %s", (platform,))
    exist = {int((r or {}).get("id")) for r in cur.fetchall() if (r or {}).get("id") is not None}
    to_insert = [x for x in ids if x not in exist]
    if not to_insert:
        return 0

    # 兼容旧表：如果没有 updated_time 列则不写
    has_ut = False
    try:
        cur.execute(f"DESCRIBE `{TARGET_TABLE}`")
        cols = cur.fetchall() or []
        col_names = {str((c or {}).get("Field") or (c or {}).get("field") or (c or {}).get("name") or "").strip().lower() for c in cols}
        has_ut = "updated_time" in col_names
    except Exception:
        has_ut = False

    if has_ut:
        insert_sql = f"""
            INSERT INTO `{TARGET_TABLE}` (platform, id, code_value, description, stat_cost, channel_name, created_time, updated_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [(platform, x, "", None, None, "自动导入", now_str, now_str) for x in to_insert]
    else:
        insert_sql = f"""
            INSERT INTO `{TARGET_TABLE}` (platform, id, code_value, description, stat_cost, channel_name, created_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        rows = [(platform, x, "", None, None, "自动导入", now_str) for x in to_insert]
    if not dry_run:
        cur.executemany(insert_sql, rows)
    return len(to_insert)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["replace", "append"], default="replace", help="replace=覆盖写入；append=只插入不存在的")
    parser.add_argument("--dry-run", action="store_true", help="仅打印，不执行写入")
    args = parser.parse_args()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_conn() as conn:
        with conn.cursor() as cur:
            data = fetch_latest_ids_for_platform(cur)

            total_inserted = 0
            for platform, info in data.items():
                ids = info.get("ids") or []
                latest_tag = info.get("latest_day") or info.get("latest_biz_date")
                print(f"[{platform}] latest={latest_tag} ids={len(ids)}")

                if args.mode == "replace":
                    inserted = replace_platform(cur, platform, ids, now_str, dry_run=args.dry_run)
                else:
                    inserted = append_platform(cur, platform, ids, now_str, dry_run=args.dry_run)
                total_inserted += inserted

            if not args.dry_run:
                conn.commit()

    print(f"完成：mode={args.mode}, total_inserted={total_inserted}")


if __name__ == "__main__":
    main()

