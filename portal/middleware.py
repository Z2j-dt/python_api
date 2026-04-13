# -*- coding: utf-8 -*-
"""门户鉴权中间件：未登录访问除 /login、/logout、/static 外均重定向到登录页。
支持 URL 参数 portal_token（多端口无 Nginx 时一次登录跨端口访问）。"""
from urllib.parse import quote, parse_qs


def _get_query_param(environ, name):
    """从 QUERY_STRING 中读取单个参数值。"""
    qs = environ.get("QUERY_STRING") or ""
    params = parse_qs(qs, keep_blank_values=True)
    vals = params.get(name) or []
    return (vals[0] or "").strip() if vals else ""


def auth_middleware(app, config):
    """返回包装后的 WSGI app：先校验 Cookie，若无则校验 URL 参数 portal_token，未通过则重定向到 /login。"""
    from portal.auth import (
        verify_auth_cookie,
        get_cookie_from_environ,
        verify_portal_token,
        PORTAL_TOKEN_MAX_AGE,
    )

    cookie_name = config["AUTH_COOKIE_NAME"]
    secret_key = config["SECRET_KEY"]
    max_age = config["AUTH_COOKIE_MAX_AGE"]

    def allowed_without_auth(path):
        path = (path or "").split("?")[0].rstrip("/") or "/"
        if path == "/login" or path.startswith("/login?"):
            return True
        if path == "/logout" or path.startswith("/logout?"):
            return True
        if path.startswith("/static"):
            return True
        # 技术支持对外开放的接口（例如 /dolphin/api/hsgt-price-deliver）允许免登录访问，
        # 避免从外部环境调用时被重定向到本机 127.0.0.1:5000/login。
        if path.startswith("/dolphin/api/hsgt-price-deliver"):
            return True
        return False

    def application(environ, start_response):
        path = environ.get("PATH_INFO", "")
        if allowed_without_auth(path):
            return app(environ, start_response)

        cookie_value = get_cookie_from_environ(environ, cookie_name)
        user_info = verify_auth_cookie(cookie_value, secret_key, max_age, cookie_name)
        if not user_info:
            token = _get_query_param(environ, "portal_token")
            user_info = verify_portal_token(token, secret_key, PORTAL_TOKEN_MAX_AGE) if token else None
        if user_info:
            username, modules, resources = user_info
            environ["portal.user"] = {
                "username": username,
                "modules": modules or [],
                "resources": resources,
            }
            return app(environ, start_response)

        # 未登录：重定向到 /login，可带 next（规范化为纯路径，避免 /? 等）
        clean_path = (path or "/").split("?")[0].rstrip("/") or "/"
        location = "/login?next=" + quote(clean_path)
        start_response("302 Found", [("Location", location), ("Content-Type", "text/html; charset=utf-8")])
        return [b"Redirecting to <a href=\"" + location.encode("utf-8") + b"\">login</a>..."]

    return application
