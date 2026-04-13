# -*- coding: utf-8 -*-
"""
统一门户 - 一个页面，分模块展示各微服务 + 登录与模块权限
"""
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

import pymysql

# 保证直接运行 python portal/app.py 时项目根在 path 里，能 import portal
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from urllib.parse import quote, urlparse

from flask import Flask, abort, jsonify, render_template, request, Response, redirect, url_for
from portal.config import Config
from portal.auth import (
    verify_password,
    create_auth_cookie,
    verify_auth_cookie,
    get_cookie_from_environ,
    create_portal_token,
)
from portal.middleware import auth_middleware
from portal.db_users import (
    get_user_row,
    insert_user,
    list_users as db_list_portal_users,
    delete_user as db_delete_user,
    update_password as db_update_password,
    user_exists as db_user_exists,
    verify_user_login,
)
from portal.permissions import (
    ADMIN_RESOURCE_GROUPS,
    fetch_user_resources,
    full_superuser_resources,
    is_superuser,
    resources_to_modules,
    nav_show_flags,
    save_user_resources,
)

app = Flask(__name__)
app.config.from_object(Config)

# 用于前端静态资源 cache bust（避免浏览器/代理缓存导致“改了没生效”）
PORTAL_BUILD_VERSION = os.environ.get("PORTAL_BUILD_VERSION") or datetime.now().strftime("%Y%m%d%H%M%S")

# 独立运行 portal/app.py 时也要走鉴权中间件，否则 GET / 时拿不到 Cookie 会再次跳到登录
_auth_config = {
    "AUTH_COOKIE_NAME": app.config["AUTH_COOKIE_NAME"],
    "SECRET_KEY": app.config["SECRET_KEY"],
    "AUTH_COOKIE_MAX_AGE": app.config["AUTH_COOKIE_MAX_AGE"],
}
app.wsgi_app = auth_middleware(app.wsgi_app, _auth_config)


def _current_user():
    """从鉴权中间件注入的 environ 中取当前用户，未登录为 None。"""
    return request.environ.get("portal.user") if request.environ else None


try:
    # 优先复用 business/app.py 里的 StarRocks 配置，避免重复配置一套环境变量
    from business.app import _settings as _biz_settings  # type: ignore
except Exception:
    _biz_settings = None

if _biz_settings is not None:
    _SR_HOST = _biz_settings.starrocks_host
    _SR_PORT = int(getattr(_biz_settings, "starrocks_port", 9030))
    _SR_USER = _biz_settings.starrocks_user
    _SR_PASSWORD = _biz_settings.starrocks_password
    _SR_DB = _biz_settings.starrocks_database
else:
    _SR_HOST = os.environ.get("STARROCKS_HOST", "127.0.0.1")
    _SR_PORT = int(os.environ.get("STARROCKS_PORT", "9030"))
    _SR_USER = os.environ.get("STARROCKS_USER", "root")
    _SR_PASSWORD = os.environ.get("STARROCKS_PASSWORD", "")
    _SR_DB = os.environ.get("STARROCKS_DATABASE", "your_database")

_SR_EVENT_TABLE = os.environ.get("PORTAL_EVENT_TABLE", "portal_event_log")
_SR_EVENT_ENABLE = (os.environ.get("PORTAL_EVENT_TO_DB", "1").lower() in ("1", "true", "yes"))


def _event_log_path() -> str:
    """埋点日志文件路径，可通过环境变量 PORTAL_EVENT_LOG 覆盖。"""
    cfg_path = app.config.get("EVENT_LOG_PATH") or os.environ.get("PORTAL_EVENT_LOG") or ""
    if cfg_path:
        return cfg_path
    portal_dir = Path(__file__).resolve().parent
    return str(portal_dir / "portal_events.log")


def _log_event(event, username=None, **extra):
    """简单埋点：按行写 JSON + 可选落库 StarRocks，避免影响主流程，所有异常静默忽略。"""
    # 统一使用中国北京时间（UTC+8）
    now = datetime.utcnow() + timedelta(hours=8)
    ts_iso = now.isoformat(timespec="seconds")
    try:
        log_path = _event_log_path()
        rec = {
            "ts": ts_iso,
            "event": event,
            "username": username,
            "ip": (request.headers.get("X-Real-IP") or
                   (request.headers.get("X-Forwarded-For", "").split(",")[0].strip() if request.headers.get("X-Forwarded-For") else "") or
                   request.remote_addr),
            "ua": request.headers.get("User-Agent"),
            "path": request.path,
            "extra": extra or {},
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        # 埋点失败不影响正常业务
        return

    # 落库 StarRocks：低频埋点，允许同步插入；异常忽略
    if not _SR_EVENT_ENABLE or not _SR_EVENT_TABLE:
        return
    try:
        conn = pymysql.connect(
            host=_SR_HOST,
            port=_SR_PORT,
            user=_SR_USER,
            password=_SR_PASSWORD,
            database=_SR_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=2,
        )
        try:
            with conn.cursor() as cur:
                event_date = now.date().isoformat()
                ts_dt = now.strftime("%Y-%m-%d %H:%M:%S")
                extra_json = extra or {}
                cur.execute(
                    f"INSERT INTO `{_SR_EVENT_TABLE}` "
                    "(event_date, ts, event, username, module, view, label, ip, ua, path, extra_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        event_date,
                        ts_dt,
                        str(event),
                        username,
                        (extra_json.get("module") if isinstance(extra_json, dict) else None),
                        (extra_json.get("view") if isinstance(extra_json, dict) else None),
                        (extra_json.get("label") if isinstance(extra_json, dict) else None),
                        rec.get("ip"),
                        rec.get("ua"),
                        rec.get("path"),
                        json.dumps(extra_json, ensure_ascii=False),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        # 不影响主流程
        return


def _proxy_sr_api(path=""):
    """将 /sr_api 请求转发到 SR_API_URL 后端（业务场景单独进程时用）。
    代理时强制使用 127.0.0.1，避免内网下 SR_API_URL 配成外网 IP 时本机连自己挂起导致无响应。
    """
    import requests
    raw_base = app.config["SR_API_URL"].rstrip("/")
    parsed = urlparse(raw_base)
    # 代理请求一律走本机，避免内网环境连外网 IP 无响应
    proxy_base = "http://127.0.0.1:%s" % (parsed.port or 5003)
    url = f"{proxy_base}/{path}".rstrip("/") or proxy_base + "/"
    if request.query_string:
        url += "?" + request.query_string.decode("utf-8")
    proxy_headers = {"X-Forwarded-Prefix": "/sr_api"}
    if request.content_type:
        proxy_headers["Content-Type"] = request.content_type
    # 透传门户登录凭证给 business（cookie + token 双通道）
    raw_cookie = request.headers.get("Cookie")
    if raw_cookie:
        proxy_headers["Cookie"] = raw_cookie
    token_header = (request.headers.get("X-Portal-Token") or "").strip()
    if token_header:
        proxy_headers["X-Portal-Token"] = token_header
    # 内网代理兜底：直接透传已登录用户与细粒度权限，避免多进程/跨端口下 token 解析差异
    try:
        cu = _current_user()
        if isinstance(cu, dict):
            uname = (cu.get("username") or "").strip()
            if uname:
                proxy_headers["X-Portal-User"] = uname
            r = cu.get("resources")
            if isinstance(r, dict):
                proxy_headers["X-Portal-Resources"] = json.dumps(r, ensure_ascii=False)
    except Exception:
        pass
    try:
        if request.method == "GET":
            r = requests.get(url, headers=proxy_headers, timeout=30)
        elif request.method == "POST":
            r = requests.post(url, data=request.get_data(), headers=proxy_headers, timeout=30)
        elif request.method == "HEAD":
            r = requests.head(url, headers=proxy_headers, timeout=10)
        else:
            r = requests.request(request.method, url, data=request.get_data(), headers=proxy_headers, timeout=30)
    except Exception as e:
        return Response("Proxy error: %s" % e, status=502)
    excluded = {"Content-Encoding", "Transfer-Encoding", "Connection"}
    headers = [(k, v) for k, v in r.headers.items() if k not in excluded]
    return Response(r.content, status=r.status_code, headers=headers)


@app.route("/login", methods=["GET", "POST"])
def login():
    """登录页：GET 展示表单，POST 校验并写 Cookie 后重定向到 / 或 next。"""
    if request.method == "GET":
        return render_template("login.html", next_url=request.args.get("next") or "/")

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    raw_next = (request.form.get("next") or request.args.get("next") or "/").strip()
    next_url = (raw_next.split("?")[0] or "/").rstrip("/") or "/"
    if not next_url.startswith("/"):
        next_url = "/"

    row = None
    if app.config.get("PORTAL_AUTH_FROM_DB"):
        row = verify_user_login(
            username,
            password,
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_USER_TABLE"],
        )
        if row is None:
            return render_template(
                "login.html",
                next_url=next_url,
                error="用户名或密码错误",
            ), 401
        modules = []
    else:
        modules = verify_password(username, password, app.config["AUTH_USERS"])
        if modules is None:
            return render_template(
                "login.html",
                next_url=next_url,
                error="用户名或密码错误",
            ), 401

    resources_for_cookie = None
    modules_for_cookie = modules
    if app.config.get("PORTAL_PERMISSION_FROM_DB"):
        su = set(app.config.get("PORTAL_SUPERUSERS") or frozenset())
        if app.config.get("PORTAL_AUTH_FROM_DB"):
            super_u = (bool(row.get("is_superuser")) if row else False) or is_superuser(username, su)
        else:
            super_u = is_superuser(username, su)
        if super_u:
            r = full_superuser_resources()
            resources_for_cookie = r
            modules_for_cookie = resources_to_modules(r)
        else:
            r = fetch_user_resources(
                username,
                _SR_HOST,
                _SR_PORT,
                _SR_USER,
                _SR_PASSWORD,
                _SR_DB,
                app.config["PORTAL_PERMISSION_TABLE"],
            )
            if r is None:
                resources_for_cookie = None
                modules_for_cookie = modules
            else:
                resources_for_cookie = r
                modules_for_cookie = resources_to_modules(r)

    # 登录成功埋点：记录账号、模块权限等
    try:
        _log_event(
            "login_success",
            username=username,
            modules=modules_for_cookie,
        )
    except Exception:
        pass

    cookie_val = create_auth_cookie(
        username,
        modules_for_cookie,
        app.config["SECRET_KEY"],
        app.config["AUTH_COOKIE_MAX_AGE"],
        app.config["AUTH_COOKIE_NAME"],
        resources=resources_for_cookie,
    )
    resp = redirect(next_url)
    resp.set_cookie(
        app.config["AUTH_COOKIE_NAME"],
        cookie_val,
        max_age=app.config["AUTH_COOKIE_MAX_AGE"],
        path="/",
        httponly=True,
        samesite="Lax",
    )
    return resp


@app.route("/logout")
def logout():
    """登出：清除 Cookie 并重定向到登录页。"""
    resp = redirect(url_for("login"))
    resp.delete_cookie(app.config["AUTH_COOKIE_NAME"], path="/")
    return resp


@app.route("/account/password", methods=["GET", "POST"])
def account_change_password():
    """库表登录用户自助修改密码（portal_user）。"""
    u = _current_user()
    if not u:
        return redirect(url_for("login", next="/account/password"))
    if not app.config.get("PORTAL_AUTH_FROM_DB"):
        return Response(
            "当前未启用 PORTAL_AUTH_FROM_DB，账号来自 users.json，请由管理员直接改文件或使用 auth_config。",
            status=403,
            mimetype="text/plain; charset=utf-8",
        )
    name = (u.get("username") or "").strip()
    if request.method == "GET":
        return render_template(
            "change_password.html",
            username=name,
            build_version=PORTAL_BUILD_VERSION,
            error=None,
        )
    current = (request.form.get("current_password") or "").strip()
    new_pwd = request.form.get("new_password") or ""
    confirm = request.form.get("confirm_password") or ""
    err = None
    if not current:
        err = "请输入当前密码"
    elif new_pwd != confirm:
        err = "两次输入的新密码不一致"
    elif len(new_pwd) < 6:
        err = "新密码至少 6 位"
    if err:
        return render_template(
            "change_password.html",
            username=name,
            build_version=PORTAL_BUILD_VERSION,
            error=err,
        ), 400
    row = verify_user_login(
        name,
        current,
        _SR_HOST,
        _SR_PORT,
        _SR_USER,
        _SR_PASSWORD,
        _SR_DB,
        app.config["PORTAL_USER_TABLE"],
    )
    if row is None:
        return render_template(
            "change_password.html",
            username=name,
            build_version=PORTAL_BUILD_VERSION,
            error="当前密码错误",
        ), 401
    try:
        db_update_password(
            name,
            new_pwd,
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_USER_TABLE"],
        )
    except Exception as e:
        return render_template(
            "change_password.html",
            username=name,
            build_version=PORTAL_BUILD_VERSION,
            error=str(e) or "修改失败",
        ), 500
    return redirect(url_for("index") + "?pwd_changed=1")


# 地址栏路径 <-> 模块 ID 映射。business 下用子目录，如 /business/realtime_mv，便于后续扩展
_PATH_TO_MODULE = {
    "/data_map/hive_metadata": "hive_metadata",
    "/data_map/sql_lineage_web": "sql_lineage",
    "/support/excel_to_hive": "excel_to_hive",
    "/support/dolphin": "dolphin_failed",
    "/support/sql_to_excel": "sql_to_excel",
    "/business/realtime_mv": "sr_api",
}
_MODULE_TO_PATH = {v: k for k, v in _PATH_TO_MODULE.items()}


def _module_from_path(path):
    """从路径解析模块 ID，如 /data_map/hive_metadata -> hive_metadata"""
    p = ((path or "").split("?")[0] or "/").rstrip("/") or "/"
    if _PATH_TO_MODULE.get(p):
        return _PATH_TO_MODULE[p]
    if p.startswith("/business/realtime_mv"):
        return "sr_api"
    if p.startswith("/business"):
        return "sr_api"  # 兼容 /business、/business/xxx
    if p.startswith("/data_map/hive_metadata"):
        return "hive_metadata"
    if p.startswith("/data_map/sql_lineage"):
        return "sql_lineage"
    if p.startswith("/support/dolphin"):
        return "dolphin_failed"
    if p.startswith("/support/excel_to_hive"):
        return "excel_to_hive"
    if p.startswith("/support/sql_to_excel"):
        return "sql_to_excel"
    return None


def _module_url(module_id, data_map_base, support_base, business_base):
    """与前端 config 一致：单端口用路径，多端口用 base + 子路径。"""
    if module_id == "hive_metadata":
        return (data_map_base + "/hive_metadata/") if data_map_base else "/data_map/hive_metadata/"
    if module_id == "sql_lineage":
        return (data_map_base + "/sql_lineage_web/") if data_map_base else "/data_map/sql_lineage_web/"
    if module_id == "excel_to_hive":
        return (support_base + "/excel_to_hive/") if support_base else "/support/excel_to_hive/"
    if module_id == "dolphin_failed":
        return (support_base + "/dolphin/") if support_base else "/support/dolphin/"
    if module_id == "sql_to_excel":
        return (support_base + "/sql_to_excel/") if support_base else "/support/sql_to_excel/"
    if module_id == "sr_api":
        return (business_base.rstrip("/") + "/realtime_mv/") if business_base else "/business/realtime_mv/"
    return "/data_map/hive_metadata/"


def _is_portal_superuser(username: str) -> bool:
    """环境变量超级用户名单，或 portal_user.is_superuser=1。"""
    name = (username or "").strip()
    if not name:
        return False
    if is_superuser(name, set(app.config.get("PORTAL_SUPERUSERS") or frozenset())):
        return True
    if app.config.get("PORTAL_AUTH_FROM_DB"):
        r = get_user_row(
            name,
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_USER_TABLE"],
        )
        return bool(r and r.get("is_superuser"))
    return False


def _require_superuser():
    """已登录且为超级用户，否则返回 None。"""
    u = _current_user()
    if not u:
        return None
    name = (u.get("username") or "").strip()
    if _is_portal_superuser(name):
        return u
    return None


def _render_index(user, path_hint=None):
    """渲染门户首页，path_hint 为地址栏路径如 /data_map/hive_metadata，用于确定初始模块。"""
    try:
        pwd_changed = (request.args.get("pwd_changed") or "").strip() == "1"
    except Exception:
        pwd_changed = False
    portal_token = ""
    data_map_base = app.config.get("DATA_MAP_BASE", "") or ""
    support_base = app.config.get("SUPPORT_BASE", "") or ""
    business_base = app.config.get("BUSINESS_BASE", "") or ""

    # 支持双 IP 访问：用当前请求的 Host 生成 iframe 地址，使外网(192.168.168.219)与内网(10.8.93.34)均可访问
    if data_map_base or support_base or business_base:
        try:
            request_host = (request.host or "").split(":")[0] or (request.host or "")
            if request_host:
                scheme = request.environ.get("HTTP_X_FORWARDED_PROTO", request.scheme) or "http"
                def _port_from(url, default):
                    p = urlparse(url or "")
                    return p.port if p.port is not None else default
                data_map_base = f"{scheme}://{request_host}:{_port_from(data_map_base, 5001)}" if data_map_base else ""
                support_base = f"{scheme}://{request_host}:{_port_from(support_base, 5002)}" if support_base else ""
                business_base = f"{scheme}://{request_host}:{_port_from(business_base, 5003)}" if business_base else ""
        except Exception:
            pass

    raw_res = user.get("resources")
    token_resources = raw_res if isinstance(raw_res, dict) else None
    if data_map_base or support_base or business_base:
        portal_token = create_portal_token(
            user.get("username"),
            user.get("modules") or [],
            app.config["SECRET_KEY"],
            resources=token_resources,
        )
    allowed = user.get("modules") or []
    show_dc_flag, show_biz_flag = nav_show_flags(raw_res)
    show_market_center_nav = False
    show_direct_center_nav = False
    show_advisor_center_nav = False
    show_customer_center_nav = False
    if show_dc_flag is None:
        show_data_center_nav = (
            ("hive_metadata" in allowed)
            or ("sql_lineage" in allowed)
            or ("excel_to_hive" in allowed)
            or ("dolphin_failed" in allowed)
            or ("sql_to_excel" in allowed)
        )
        show_business_nav = "sr_api" in allowed
        show_market_center_nav = show_business_nav
        show_direct_center_nav = show_business_nav
        show_advisor_center_nav = show_business_nav
        show_customer_center_nav = show_business_nav
        resource_access = None
    else:
        show_data_center_nav = show_dc_flag
        show_business_nav = show_biz_flag
        resource_access = raw_res if isinstance(raw_res, dict) else {}
        show_market_center_nav = any(
            rid in resource_access
            for rid in ("sr_api:realtime", "sr_api:config_code_mapping")
        )
        show_direct_center_nav = any(
            rid in resource_access
            for rid in ("sr_api:open_channel_daily", "sr_api:config_open", "sr_api:config_staff")
        )
        show_advisor_center_nav = any(
            rid in resource_access
            for rid in (
                "sr_api:config_stock_position",
                "sr_api:config_sales_order",
                "sr_api:config_sign_customer_group",
                "sr_api:config_activity_channel",
                "sr_api:sales_daily_leads",
            )
        )
        show_customer_center_nav = any(
            rid in resource_access
            for rid in ("sr_api:config_opportunity_lead", "sr_api:config_morning_hot_stock_track")
        )
    # users.json 里可配置 readonly=true（库表登录时无此项）
    if app.config.get("PORTAL_AUTH_FROM_DB"):
        is_readonly = False
    else:
        try:
            uconf = (app.config.get("AUTH_USERS") or {}).get(user.get("username") or "", {}) or {}
            is_readonly = bool(uconf.get("readonly"))
        except Exception:
            is_readonly = False
    # 优先用 path_hint 解析模块；否则用第一个有权限的，且默认不选业务应用避免首屏卡死
    first_id = None
    first_module_url = ""
    initial_path = "/"
    business_iframe_base = ""
    if allowed:
        first_id = _module_from_path(path_hint) if path_hint else None
        if not first_id or first_id not in allowed:
            first_id = allowed[0]
        if first_id == "sr_api" and (not path_hint or path_hint.strip("/") in ("", "business")):
            non_sr = [m for m in allowed if m != "sr_api"]
            if non_sr:
                first_id = non_sr[0]
        first_module_url = _module_url(first_id, data_map_base, support_base, business_base)
        if portal_token and first_module_url:
            sep = "&" if "?" in first_module_url else "?"
            first_module_url = first_module_url + sep + "portal_token=" + quote(portal_token, safe="")
        initial_path = _MODULE_TO_PATH.get(first_id) or "/data_map/hive_metadata"
    return render_template(
        "index.html",
        username=user.get("username"),
        allowed_modules=allowed,
        show_data_center_nav=show_data_center_nav,
        show_business_nav=show_business_nav,
        show_market_center_nav=show_market_center_nav,
        show_direct_center_nav=show_direct_center_nav,
        show_advisor_center_nav=show_advisor_center_nav,
        show_customer_center_nav=show_customer_center_nav,
        resource_access=resource_access,
        data_map_base=data_map_base,
        support_base=support_base,
        business_base=business_base,
        business_iframe_base=business_iframe_base,
        portal_token=portal_token,
        is_readonly=is_readonly,
        first_module_url=first_module_url,
        initial_path=initial_path,
        build_version=PORTAL_BUILD_VERSION,
        show_admin_link=_is_portal_superuser((user.get("username") or "").strip()),
        show_password_link=bool(app.config.get("PORTAL_AUTH_FROM_DB")),
        pwd_changed=pwd_changed,
    )


@app.route("/admin/permissions")
def admin_permissions_page():
    """超级用户：子模块权限配置页（读写 portal_user_resource）。"""
    u = _current_user()
    if not u:
        return redirect(url_for("login", next="/admin/permissions"))
    if not _require_superuser():
        return Response("无权访问（需超级用户）", status=403, mimetype="text/plain; charset=utf-8")
    return render_template(
        "admin_permissions.html",
        build_version=PORTAL_BUILD_VERSION,
        from_db=bool(app.config.get("PORTAL_PERMISSION_FROM_DB")),
        auth_from_db=bool(app.config.get("PORTAL_AUTH_FROM_DB")),
        resource_groups=ADMIN_RESOURCE_GROUPS,
    )


def _admin_list_account_users():
    """权限页下拉里展示的账号：库表模式用 portal_user，否则用 AUTH_USERS。"""
    su = set(app.config.get("PORTAL_SUPERUSERS") or frozenset())
    if app.config.get("PORTAL_AUTH_FROM_DB"):
        rows = db_list_portal_users(
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_USER_TABLE"],
        )
        out = []
        for row in rows:
            name = row.get("username") or ""
            db_su = bool(row.get("is_superuser"))
            out.append(
                {
                    "username": name,
                    "is_superuser": db_su or is_superuser(name, su),
                }
            )
        return out
    out = []
    for name in sorted((app.config.get("AUTH_USERS") or {}).keys()):
        out.append(
            {
                "username": name,
                "is_superuser": is_superuser(name, su),
            }
        )
    return out


@app.route("/admin/api/permissions/users", methods=["GET"])
def admin_api_list_users():
    if not _require_superuser():
        abort(403)
    return jsonify({"users": _admin_list_account_users()})


@app.route("/admin/api/permissions/users/<username>", methods=["GET"])
def admin_api_get_user_resources(username):
    if not _require_superuser():
        abort(403)
    un = (username or "").strip()
    if not un:
        abort(400)
    r = fetch_user_resources(
        un,
        _SR_HOST,
        _SR_PORT,
        _SR_USER,
        _SR_PASSWORD,
        _SR_DB,
        app.config["PORTAL_PERMISSION_TABLE"],
    )
    if r is None:
        return jsonify({"error": "数据库查询失败"}), 500
    return jsonify({"username": un, "resources": r})


@app.route("/admin/api/permissions/users/<username>", methods=["PUT"])
def admin_api_put_user_resources(username):
    su = _current_user()
    if not _require_superuser():
        abort(403)
    if not app.config.get("PORTAL_PERMISSION_FROM_DB"):
        return jsonify({"error": "未启用 PORTAL_PERMISSION_FROM_DB"}), 400
    un = (username or "").strip()
    if app.config.get("PORTAL_AUTH_FROM_DB"):
        if not db_user_exists(un, _SR_HOST, _SR_PORT, _SR_USER, _SR_PASSWORD, _SR_DB, app.config["PORTAL_USER_TABLE"]):
            return jsonify({"error": "用户不存在于 portal_user 表"}), 400
    elif un not in (app.config.get("AUTH_USERS") or {}):
        return jsonify({"error": "用户不存在于账号配置中"}), 400
    if _is_portal_superuser(un):
        return jsonify({"error": "超级用户无需配置表权限"}), 400
    body = request.get_json(silent=True) or {}
    resources = body.get("resources") if isinstance(body.get("resources"), dict) else {}
    editor = (su.get("username") or "").strip()
    try:
        save_user_resources(
            un,
            resources,
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_PERMISSION_TABLE"],
            editor,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/admin/api/users", methods=["POST"])
def admin_api_create_portal_user():
    """仅在 PORTAL_AUTH_FROM_DB=1 时向 portal_user 插入账号（超级用户）。"""
    if not _require_superuser():
        abort(403)
    if not app.config.get("PORTAL_AUTH_FROM_DB"):
        return jsonify({"error": "未启用 PORTAL_AUTH_FROM_DB，请使用 users.json 维护账号"}), 400
    body = request.get_json(silent=True) or {}
    un = (body.get("username") or "").strip()
    pw = body.get("password") or ""
    is_su = bool(body.get("is_superuser"))
    if not un or not pw:
        return jsonify({"error": "用户名与密码必填"}), 400
    try:
        insert_user(
            un,
            pw,
            is_su,
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_USER_TABLE"],
        )
    except pymysql.err.IntegrityError:
        return jsonify({"error": "用户名已存在"}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        err = str(e)
        if "Duplicate" in err or "1062" in err or "duplicate" in err.lower():
            return jsonify({"error": "用户名已存在"}), 400
        return jsonify({"error": err}), 500
    return jsonify({"ok": True})


@app.route("/admin/api/users/<username>", methods=["DELETE"])
def admin_api_delete_portal_user(username):
    """仅超级用户：删除 portal_user 中的普通用户（并清理 portal_user_resource）。"""
    su = _current_user()
    if not _require_superuser():
        abort(403)
    if not app.config.get("PORTAL_AUTH_FROM_DB"):
        return jsonify({"error": "未启用 PORTAL_AUTH_FROM_DB，请使用 users.json 维护账号"}), 400
    un = (username or "").strip()
    if not un:
        abort(400)
    actor = (su.get("username") or "").strip() if isinstance(su, dict) else ""
    if actor and un == actor:
        return jsonify({"error": "不能删除当前登录账号"}), 400
    # 超级用户禁止删除（含 env 超级用户）
    if _is_portal_superuser(un):
        return jsonify({"error": "不能删除超级用户账号"}), 400
    row = get_user_row(
        un,
        _SR_HOST,
        _SR_PORT,
        _SR_USER,
        _SR_PASSWORD,
        _SR_DB,
        app.config["PORTAL_USER_TABLE"],
    )
    if row and row.get("is_superuser"):
        return jsonify({"error": "不能删除超级用户账号"}), 400
    try:
        # 先删权限记录（可选）
        if app.config.get("PORTAL_PERMISSION_FROM_DB"):
            save_user_resources(
                un,
                {},
                _SR_HOST,
                _SR_PORT,
                _SR_USER,
                _SR_PASSWORD,
                _SR_DB,
                app.config["PORTAL_PERMISSION_TABLE"],
                actor,
            )
        db_delete_user(
            un,
            _SR_HOST,
            _SR_PORT,
            _SR_USER,
            _SR_PASSWORD,
            _SR_DB,
            app.config["PORTAL_USER_TABLE"],
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/")
def index():
    """单页门户。"""
    user = _current_user()
    if not user:
        next_path = (request.path or "/").rstrip("/") or "/"
        return redirect(url_for("login", next=next_path))
    return _render_index(user, request.path)


@app.route("/data_map/<path:subpath>")
def index_data_map(subpath):
    """门户壳：地址栏展示 /data_map/xxx。"""
    user = _current_user()
    if not user:
        return redirect(url_for("login", next=request.path))
    path = "/data_map/" + (subpath or "").rstrip("/")
    return _render_index(user, path)


@app.route("/support/<path:subpath>")
def index_support(subpath):
    """门户壳：地址栏展示 /support/xxx。"""
    user = _current_user()
    if not user:
        return redirect(url_for("login", next=request.path))
    path = "/support/" + (subpath or "").rstrip("/")
    return _render_index(user, path)


@app.route("/business")
def index_business_root():
    """/business 重定向到默认子模块 /business/realtime_mv。"""
    user = _current_user()
    if not user:
        return redirect(url_for("login", next="/business/realtime_mv"))
    return redirect("/business/realtime_mv", code=302)


@app.route("/business/<path:subpath>")
def index_business(subpath):
    """门户壳：地址栏展示 /business/realtime_mv 等子目录。"""
    user = _current_user()
    if not user:
        return redirect(url_for("login", next=request.path))
    path = "/business/" + (subpath or "").rstrip("/")
    return _render_index(user, path)


@app.route("/health")
def health():
    return {"status": "ok", "service": "portal"}


@app.route("/sr_api/", defaults={"path": ""})
@app.route("/sr_api/<path:path>")
def proxy_sr_api(path):
    return _proxy_sr_api(path)


@app.route("/track", methods=["POST"])
def track():
    """前端埋点上报接口：记录模块点击、页面加载等事件。"""
    user = _current_user()
    username = user.get("username") if isinstance(user, dict) else None
    # 兼容 sendBeacon：其 Content-Type 通常不是 application/json，get_json 拿不到，需要手动解析原始 body
    data = {}
    try:
        parsed = request.get_json(silent=True)
        if isinstance(parsed, dict):
            data = parsed
        else:
            raise ValueError
    except Exception:
        try:
            raw = request.get_data(cache=False, as_text=True) or ""
            if raw.strip():
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    data = loaded
        except Exception:
            data = {}
    event = (data.get("event") or "").strip() or "unknown"
    module_id = (data.get("module") or "").strip() or None
    view = (data.get("view") or "").strip() or None
    label = (data.get("label") or "").strip() or None
    try:
        _log_event(
            event,
            username=username,
            module=module_id,
            view=view,
            label=label,
        )
    except Exception:
        pass
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=app.config["PORT"],
        debug=app.config["DEBUG"],
    )
