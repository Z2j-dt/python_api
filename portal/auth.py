# -*- coding: utf-8 -*-
"""门户登录与权限：校验密码、签发/校验 Cookie"""

import os
from werkzeug.security import check_password_hash

try:
    from itsdangerous import URLSafeTimedSerializer
except ImportError:
    URLSafeTimedSerializer = None


def _serializer(secret_key, salt="portal-auth"):
    if URLSafeTimedSerializer is None:
        raise RuntimeError("itsdangerous 未安装，请 pip install itsdangerous")
    return URLSafeTimedSerializer(secret_key, salt=salt)


def verify_password(username, password, auth_users):
    """校验用户名密码，成功返回该用户的 modules 列表，否则返回 None。
    支持两种配置：password_hash（哈希）或 password（明文，来自 users.json）。"""
    user = auth_users.get(username)
    if not user:
        return None
    # 明文密码（如 users.json）
    plain = user.get("password")
    if plain is not None:
        if password == plain:
            return user.get("modules", [])
        return None
    ph = user.get("password_hash") or ""
    if not ph:
        if username == "admin" and os.environ.get("PORTAL_ADMIN_PASSWORD") and password == os.environ.get("PORTAL_ADMIN_PASSWORD"):
            return user.get("modules", [])
        return None
    if check_password_hash(ph, password):
        return user.get("modules", [])
    return None


def create_auth_cookie(username, modules, secret_key, max_age, cookie_name, resources=None):
    """生成登录态 Cookie 值（已签名）。
    resources为 dict 时写入 r，表示走库表细粒度权限；为 None 时不写 r，保持旧版仅 modules 行为。"""
    s = _serializer(secret_key)
    payload = {"u": username, "m": modules or []}
    if resources is not None:
        payload["r"] = resources if isinstance(resources, dict) else {}
    return s.dumps(payload)


def verify_auth_cookie(cookie_value, secret_key, max_age, cookie_name):
    """校验并解析 Cookie，成功返回 (username, modules, resources)。
    resources：dict 表示细粒度；None 表示旧 Cookie（仅按 modules 控制菜单）。"""
    if not cookie_value:
        return None
    s = _serializer(secret_key)
    try:
        data = s.loads(cookie_value, max_age=max_age)
        u = data.get("u")
        m = data.get("m") or []
        if "r" in data:
            r = data.get("r")
            if not isinstance(r, dict):
                r = {}
            return (u, m, r)
        return (u, m, None)
    except Exception:
        return None


def get_cookie_from_environ(environ, cookie_name):
    """从 WSGI environ 的 HTTP_COOKIE 中读取指定 cookie 的值。"""
    raw = environ.get("HTTP_COOKIE") or ""
    for part in raw.split(";"):
        part = part.strip()
        if part.startswith(cookie_name + "="):
            val = part[len(cookie_name) + 1 :].strip().strip('"')
            return val
    return None


# 多端口无 Nginx 时：用 URL 里的 portal_token 做跨端口鉴权（短期有效）
PORTAL_TOKEN_MAX_AGE = 600  # 10 分钟


def create_portal_token(username, modules, secret_key, max_age_seconds=PORTAL_TOKEN_MAX_AGE, resources=None):
    """生成短期 portal_token，用于 iframe 加载其他端口时带在 URL 上。"""
    s = _serializer(secret_key, salt="portal-token")
    payload = {"u": username, "m": modules or []}
    if resources is not None:
        payload["r"] = resources if isinstance(resources, dict) else {}
    return s.dumps(payload)


def verify_portal_token(token, secret_key, max_age_seconds=PORTAL_TOKEN_MAX_AGE):
    """校验 portal_token，成功返回 (username, modules, resources)；resources 无则为 None（旧 token）。"""
    if not token or not secret_key:
        return None
    try:
        s = _serializer(secret_key, salt="portal-token")
        data = s.loads(token, max_age=max_age_seconds)
        u = data.get("u")
        m = data.get("m") or []
        if "r" in data:
            r = data.get("r")
            if not isinstance(r, dict):
                r = {}
            return (u, m, r)
        return (u, m, None)
    except Exception:
        return None
