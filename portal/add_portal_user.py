# -*- coding: utf-8 -*-
"""
命令行创建 portal_user 表账号（用于首次启用 PORTAL_AUTH_FROM_DB 时插入第一个管理员）。

用法（在项目根执行；连接信息来自 business/.env、环境变量 STARROCKS_*，或命令行）:
  python -m portal.add_portal_user admin 你的密码 --superuser
  python -m portal.add_portal_user admin 你的密码 --superuser --host 10.8.1.2 --port 9030 -u root -p 密码 -d your_db

依赖: pymysql, werkzeug；可选 python-dotenv（用于加载 business/.env）
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from portal.db_users import insert_user


def _load_dotenv_files():
    """与 business 一致：从 business/.env 或项目根 .env 加载（需 pip install python-dotenv）。"""
    root = Path(__file__).resolve().parent.parent
    for name in ("business/.env", ".env"):
        p = root / name.replace("/", os.sep)
        if not p.is_file():
            continue
        try:
            from dotenv import load_dotenv  # type: ignore

            load_dotenv(p)
        except ImportError:
            break


def _sr_from_env():
    _load_dotenv_files()
    try:
        from business.app import _settings as biz  # type: ignore

        return (
            biz.starrocks_host,
            int(getattr(biz, "starrocks_port", 9030)),
            biz.starrocks_user,
            biz.starrocks_password,
            biz.starrocks_database,
        )
    except Exception:
        return (
            os.environ.get("STARROCKS_HOST", "127.0.0.1"),
            int(os.environ.get("STARROCKS_PORT", "9030")),
            os.environ.get("STARROCKS_USER", "root"),
            os.environ.get("STARROCKS_PASSWORD", ""),
            os.environ.get("STARROCKS_DATABASE", "your_database"),
        )


def main() -> int:
    p = argparse.ArgumentParser(description="向 portal_user 表插入账号")
    p.add_argument("username")
    p.add_argument("password")
    p.add_argument("--superuser", action="store_true", help="标记为超级用户")
    p.add_argument(
        "--table",
        default=os.environ.get("PORTAL_USER_TABLE", "portal_user"),
        help="表名，默认 portal_user",
    )
    p.add_argument("--host", default=None, help="StarRocks/MySQL 地址（覆盖 STARROCKS_HOST）")
    p.add_argument("--port", type=int, default=None, help="端口，默认 9030")
    p.add_argument("--user", "-u", default=None, dest="dbuser", help="数据库用户")
    p.add_argument("--password", "-p", default=None, dest="dbpassword", help="数据库密码")
    p.add_argument("--database", "-d", default=None, help="库名")
    args = p.parse_args()
    host, port, user, password, database = _sr_from_env()
    if args.host is not None:
        host = args.host
    if args.port is not None:
        port = args.port
    if args.dbuser is not None:
        user = args.dbuser
    if args.dbpassword is not None:
        password = args.dbpassword
    if args.database is not None:
        database = args.database
    try:
        insert_user(
            args.username,
            args.password,
            args.superuser,
            host,
            port,
            user,
            password,
            database,
            args.table,
        )
        print("OK:", args.username, "->", host, port, database)
        return 0
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
