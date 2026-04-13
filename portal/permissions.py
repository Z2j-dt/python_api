# -*- coding: utf-8 -*-
"""门户子模块权限：从 StarRocks/MySQL 表 portal_user_resource 加载，与 module_permissions.md 中 resource_id 一致。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql

# 与 portal/docs/module_permissions.md 一致；超级用户未落表时视为拥有全部 write
KNOWN_RESOURCE_IDS: frozenset = frozenset(
    {
        "hive_metadata",
        "sql_lineage",
        "excel_to_hive",
        "dolphin_failed",
        "sql_to_excel",
        "sr_api:realtime",
        "sr_api:config_code_mapping",
        "sr_api:open_channel_daily",
        "sr_api:config_open",
        "sr_api:config_staff",
        "sr_api:config_stock_position",
        "sr_api:config_sales_order",
        "sr_api:config_sign_customer_group",
        "sr_api:config_activity_channel",
        "sr_api:sales_daily_leads",
        "sr_api:config_opportunity_lead",
        "sr_api:config_morning_hot_stock_track",
    }
)


def full_superuser_resources() -> Dict[str, str]:
    return {rid: "write" for rid in KNOWN_RESOURCE_IDS}


def is_superuser(username: str, superusers: Set[str]) -> bool:
    u = (username or "").strip()
    return bool(u and u in superusers)


def resources_to_modules(resources: Dict[str, str]) -> list:
    """由 resource_id 推导门户旧版 modules 列表（含 sr_api 至多一次，且放在末尾）。"""
    if not resources:
        return []
    non_sr = sorted(k for k in resources if not k.startswith("sr_api:"))
    if any(k.startswith("sr_api:") for k in resources):
        return non_sr + ["sr_api"]
    return non_sr


def fetch_user_resources(
    username: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
) -> Optional[Dict[str, str]]:
    """查询账号权限。返回 None 表示数据库异常（调用方可回退旧逻辑）；{} 表示无授权行。"""
    u = (username or "").strip()
    if not u:
        return {}
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=3,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT resource_id, access_level FROM `{table}` WHERE username = %s",
                    (u,),
                )
                rows: list = cur.fetchall() or []
        finally:
            conn.close()
    except Exception:
        return None

    out: Dict[str, str] = {}
    for row in rows:
        rid = (row.get("resource_id") or "").strip()
        if not rid:
            continue
        level = (row.get("access_level") or "").strip().lower()
        if level not in ("read", "write"):
            continue
        out[rid] = level
    return out


# 管理页分组展示：与侧栏一致
ADMIN_RESOURCE_GROUPS: List[Tuple[str, List[Tuple[str, str]]]] = [
    (
        "数据中心",
        [
            ("hive_metadata", "数据字典"),
            ("sql_lineage", "数据血缘"),
            ("excel_to_hive", "离线上传"),
            ("dolphin_failed", "任务管理"),
            ("sql_to_excel", "数据导出"),
        ],
    ),
    (
        "市场中心",
        [
            ("sr_api:realtime", "实时加微名单"),
            ("sr_api:config_code_mapping", "抖音投流账号配置"),
        ],
    ),
    (
        "直销中心",
        [
            ("sr_api:open_channel_daily", "自营渠道加微统计"),
            ("sr_api:config_open", "渠道字典配置"),
            ("sr_api:config_staff", "承接人员配置"),
        ],
    ),
    (
        "投顾&销售中心",
        [
            ("sr_api:config_stock_position", "产品净值"),
            ("sr_api:config_sales_order", "销售订单配置"),
            ("sr_api:config_sign_customer_group", "签约客户群管理配置"),
            ("sr_api:config_activity_channel", "活动渠道字典配置"),
            ("sr_api:sales_daily_leads", "销售每日进线数据表"),
        ],
    ),
    (
        "客户中心",
        [
            ("sr_api:config_opportunity_lead", "商机线索配置"),
            ("sr_api:config_morning_hot_stock_track", "早盘人气股战绩追踪配置"),
        ],
    ),
]


def save_user_resources(
    username: str,
    resources: Dict[str, str],
    host: str,
    port: int,
    db_user: str,
    password: str,
    database: str,
    table: str,
    updated_by: str,
) -> None:
    """全量替换某用户在表中的权限行（先删后插）。resources 仅保留 read/write，且 resource_id 须在 KNOWN_RESOURCE_IDS 内。"""
    u = (username or "").strip()
    if not u:
        raise ValueError("username 为空")
    cleaned: Dict[str, str] = {}
    for rid, lvl in (resources or {}).items():
        rid = (rid or "").strip()
        lvl = (lvl or "").strip().lower()
        if not rid or lvl not in ("read", "write"):
            continue
        if rid not in KNOWN_RESOURCE_IDS:
            raise ValueError(f"非法 resource_id: {rid}")
        cleaned[rid] = lvl
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=db_user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{table}` WHERE username = %s", (u,))
            for rid, lvl in cleaned.items():
                cur.execute(
                    f"INSERT INTO `{table}` "
                    f"(username, resource_id, access_level, updated_at, updated_by) "
                    f"VALUES (%s, %s, %s, %s, %s)",
                    (u, rid, lvl, now_str, (updated_by or "").strip() or None),
                )
        conn.commit()
    finally:
        conn.close()


def nav_show_flags(resources: Optional[Dict[str, Any]]) -> tuple:
    """返回 (show_data_center, show_business)。resources 非 dict 表示走旧版 modules 逻辑，返回 (None, None)。"""
    if not isinstance(resources, dict):
        return None, None
    dc = {"hive_metadata", "sql_lineage", "excel_to_hive", "dolphin_failed", "sql_to_excel"}
    show_dc = any(rid in resources for rid in dc)
    show_biz = any(
        isinstance(k, str) and k.startswith("sr_api:") for k in resources.keys()
    )
    return show_dc, show_biz
