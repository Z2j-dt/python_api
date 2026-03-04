# -*- coding: utf-8 -*-
"""统一门户 - 各微服务地址与登录权限配置"""

import os

# 默认账号：若未配置 auth_config，则使用 admin + 下方哈希或环境变量 PORTAL_ADMIN_PASSWORD（明文，仅开发）
# 生成密码哈希（在项目根 venv 下）: python -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('你的密码'))"
_DEFAULT_ADMIN_HASH = ""


def _get_auth_users():
    import json
    from pathlib import Path
    # 1) auth_config.py（哈希配置）
    try:
        from portal.auth_config import AUTH_USERS
        return AUTH_USERS
    except ImportError:
        pass
    # 2) users.json（单独一份，明文用户名密码，方便改）
    portal_dir = Path(__file__).resolve().parent
    users_file = portal_dir / "users.json"
    if users_file.exists():
        try:
            raw = json.loads(users_file.read_text(encoding="utf-8"))
            out = {}
            for u in raw:
                name = (u.get("username") or u.get("name") or "").strip()
                if not name:
                    continue
                out[name] = {
                    "password": u.get("password"),
                    "modules": u.get("modules") or ["hive_metadata", "sql_lineage", "sr_api"],
                    "readonly": bool(u.get("readonly")) if "readonly" in u else False,
                }
            if out:
                return out
        except Exception:
            pass
    # 3) 环境变量
    raw = os.environ.get("PORTAL_AUTH_USERS")
    if raw:
        return json.loads(raw)
    # 4) 默认
    return {
        "admin": {
            "password_hash": os.environ.get("PORTAL_ADMIN_PASSWORD_HASH", _DEFAULT_ADMIN_HASH),
            "modules": ["hive_metadata", "sql_lineage", "sr_api"],
        },
    }


class Config:
    """门户配置：一个页面，多模块（数据字典、数据血缘、业务场景）+ 登录权限"""
    # 门户自身端口
    PORT = int(os.environ.get("PORTAL_PORT", "5000"))

    # 各子服务地址（可改为同机不同端口，或部署后的实际域名）
    HIVE_METADATA_URL = os.environ.get("HIVE_METADATA_URL", "http://127.0.0.1:5001")
    SQL_LINEAGE_URL = os.environ.get("SQL_LINEAGE_URL", "http://127.0.0.1:5002")
    SR_API_URL = os.environ.get("SR_API_URL", "http://127.0.0.1:5003")

    # 三大模块 base URL（用于「每模块一端口」部署时门户 iframe/链接）
    # 不设或为空时表示与门户同源（单端口 run_all 模式）；设为 http://host:5001 等则走多端口
    DATA_MAP_BASE = (os.environ.get("DATA_MAP_URL", "") or "").rstrip("/")
    SUPPORT_BASE = (os.environ.get("SUPPORT_URL", "") or "").rstrip("/")
    BUSINESS_BASE = (os.environ.get("BUSINESS_URL", "") or "").rstrip("/")

    # 门户入口 URL（多端口时，子模块 /login、/logout 会重定向到此）
    PORTAL_URL = os.environ.get("PORTAL_URL", "http://127.0.0.1:5000").rstrip("/")

    SECRET_KEY = os.environ.get("PORTAL_SECRET_KEY", "portal-dev-secret")
    DEBUG = os.environ.get("PORTAL_DEBUG", "1").lower() in ("1", "true", "yes")

    # 登录与权限
    AUTH_COOKIE_NAME = "portal_auth"
    AUTH_COOKIE_MAX_AGE = int(os.environ.get("PORTAL_AUTH_COOKIE_MAX_AGE", "86400"))  # 24h
    AUTH_USERS = _get_auth_users()
