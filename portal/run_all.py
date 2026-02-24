# -*- coding: utf-8 -*-
"""
单进程、单端口 5000：门户 + 数据字典 + 数据血缘 + 业务场景(sr_api) 全部在一个进程里。
只起一个门户进程即可，无需单独起 sr_api 或做反向代理。
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

from portal.app import app as portal_app
from portal.middleware import auth_middleware
from data_map.app import app as data_map_app
from support.app import app as support_app

try:
    from business.app import app as business_app
except ImportError:
    business_app = None

# Flask 侧：门户 + 数据地图 + 技术支持
dispatcher = DispatcherMiddleware(portal_app, {
    "/data_map": data_map_app,
    "/support": support_app,
})

# 地址栏路径：document 请求(直接访问/刷新)走门户壳，iframe 请求走实际模块
_SHELL_PREFIXES = ("/data_map", "/support", "/business")

def _route_by_dest(environ, start_response):
    path = environ.get("PATH_INFO", "")
    dest = (environ.get("HTTP_SEC_FETCH_DEST") or "").strip().lower()
    is_iframe = dest == "iframe"
    wants_shell = any(path == p or path.startswith(p + "/") for p in _SHELL_PREFIXES)
    if wants_shell and not is_iframe:
        return portal_app(environ, start_response)
    return dispatcher(environ, start_response)

flask_app = _route_by_dest
auth_config = {
    "AUTH_COOKIE_NAME": portal_app.config["AUTH_COOKIE_NAME"],
    "SECRET_KEY": portal_app.config["SECRET_KEY"],
    "AUTH_COOKIE_MAX_AGE": portal_app.config["AUTH_COOKIE_MAX_AGE"],
}
flask_app = auth_middleware(flask_app, auth_config)

import os
os.environ.setdefault("SR_API_MOUNT_PATH", "/business/realtime_mv")
if business_app is not None:
    asgi_app = FastAPI(title="数据平台门户")
    asgi_app.mount("/business/realtime_mv", business_app)
    asgi_app.mount("/", WSGIMiddleware(flask_app))
else:
    asgi_app = None
    flask_app_only = flask_app

if __name__ == "__main__":
    port = portal_app.config["PORT"]
    print("统一门户（单进程单端口 %s）: http://127.0.0.1:%s" % (port, port))
    print("  - 数据地图 - 数据字典: http://127.0.0.1:%s/data_map/hive_metadata/" % port)
    print("  - 数据地图 - 数据血缘: http://127.0.0.1:%s/data_map/sql_lineage_web/" % port)
    print("  - 技术支持 - Excel 入库 Hive: http://127.0.0.1:%s/support/excel_to_hive/" % port)
    print("  - 业务应用 - 实时加微监测: http://127.0.0.1:%s/business/realtime_mv/" % port)
    if asgi_app is not None:
        import uvicorn
        uvicorn.run(asgi_app, host="0.0.0.0", port=port)
    else:
        from werkzeug.serving import run_simple
        run_simple("0.0.0.0", port, flask_app_only, use_debugger=portal_app.config["DEBUG"], use_reloader=portal_app.config["DEBUG"], threaded=True)
