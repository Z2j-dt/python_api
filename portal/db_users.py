# -*- coding: utf-8 -*-
"""门户账号表 portal_user：登录校验、列表、创建（与 users.json 二选一）。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pymysql
from werkzeug.security import check_password_hash, generate_password_hash


def _connect(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
):
    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )


def verify_user_login(
    username: str,
    password: str,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> Optional[Dict[str, Any]]:
    """校验用户名密码，成功返回 {username, is_superuser}，失败 None。"""
    u = (username or "").strip()
    if not u or password is None:
        return None
    try:
        conn = _connect(host, port, db_user, db_password, database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT username, password_hash, is_superuser FROM `{table}` WHERE username = %s",
                    (u,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception:
        return None
    if not row:
        return None
    ph = (row.get("password_hash") or "").strip()
    if not ph or not check_password_hash(ph, password):
        return None
    is_su = row.get("is_superuser")
    try:
        is_su = bool(int(is_su)) if is_su is not None else False
    except (TypeError, ValueError):
        is_su = bool(is_su)
    return {"username": u, "is_superuser": is_su}


def get_user_row(
    username: str,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> Optional[Dict[str, Any]]:
    u = (username or "").strip()
    if not u:
        return None
    try:
        conn = _connect(host, port, db_user, db_password, database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT username, is_superuser FROM `{table}` WHERE username = %s",
                    (u,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception:
        return None
    if not row:
        return None
    is_su = row.get("is_superuser")
    try:
        is_su = bool(int(is_su)) if is_su is not None else False
    except (TypeError, ValueError):
        is_su = bool(is_su)
    return {"username": row.get("username"), "is_superuser": is_su}


def list_users(
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> List[Dict[str, Any]]:
    try:
        conn = _connect(host, port, db_user, db_password, database)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT username, is_superuser FROM `{table}` ORDER BY username ASC"
                )
                rows = cur.fetchall() or []
        finally:
            conn.close()
    except Exception:
        return []
    out = []
    for row in rows:
        is_su = row.get("is_superuser")
        try:
            is_su = bool(int(is_su)) if is_su is not None else False
        except (TypeError, ValueError):
            is_su = bool(is_su)
        out.append({"username": (row.get("username") or "").strip(), "is_superuser": is_su})
    return out


def user_exists(
    username: str,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> bool:
    return get_user_row(username, host, port, db_user, db_password, database, table) is not None


def insert_user(
    username: str,
    password_plain: str,
    is_superuser: bool,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> None:
    u = (username or "").strip()
    if not u:
        raise ValueError("username 为空")
    if not password_plain:
        raise ValueError("密码不能为空")
    ph = generate_password_hash(password_plain)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    su = 1 if is_superuser else 0
    conn = _connect(host, port, db_user, db_password, database)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{table}` (username, password_hash, is_superuser, created_at, updated_at) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (u, ph, su, now, now),
            )
        conn.commit()
    finally:
        conn.close()


def update_password(
    username: str,
    password_plain: str,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> None:
    u = (username or "").strip()
    if not u or not password_plain:
        raise ValueError("用户名或密码无效")
    ph = generate_password_hash(password_plain)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    conn = _connect(host, port, db_user, db_password, database)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{table}` SET password_hash = %s, updated_at = %s WHERE username = %s",
                (ph, now, u),
            )
            if cur.rowcount == 0:
                raise ValueError("用户不存在")
        conn.commit()
    finally:
        conn.close()


def delete_user(
    username: str,
    host: str,
    port: int,
    db_user: str,
    db_password: str,
    database: str,
    table: str,
) -> None:
    u = (username or "").strip()
    if not u:
        raise ValueError("username 为空")
    conn = _connect(host, port, db_user, db_password, database)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM `{table}` WHERE username = %s", (u,))
            if cur.rowcount == 0:
                raise ValueError("用户不存在")
        conn.commit()
    finally:
        conn.close()
