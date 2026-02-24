# -*- coding: utf-8 -*-
"""
数据地图 - 单入口 app.py，合并数据字典(hive_metadata) + 数据血缘(sql_lineage_web)。
data_map 下仅此一个 app.py，templates 和 static 在 data_map/templates/、data_map/static/。
"""
import sys
import os
import re
import json
import logging
from pathlib import Path
from datetime import datetime

from flask import Flask, render_template, request, jsonify
import pymysql
import mysql.connector
from mysql.connector import Error
from werkzeug.middleware.dispatcher import DispatcherMiddleware

# 路径
_DATA_MAP = Path(__file__).resolve().parent
ROOT = _DATA_MAP.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from portal.config import Config as PortalConfig
from portal.middleware import auth_middleware

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== 数据字典 (hive_metadata) ==========
HIVE_DB = {
    'host': os.environ.get('HIVE_DB_HOST', '10.8.93.31'),
    'port': int(os.environ.get('HIVE_DB_PORT', 3306)),
    'user': os.environ.get('HIVE_DB_USER', 'root'),
    'password': os.environ.get('HIVE_DB_PASSWORD', 'dt@MysqlYGV001'),
    'database': os.environ.get('HIVE_DB_NAME', 'hive'),
    'charset': 'utf8mb4'
}
HIVE_TARGET_SCHEMAS = ['dwd', 'dws', 'ads']

def _get_hive_conn():
    return pymysql.connect(**HIVE_DB, cursorclass=pymysql.cursors.DictCursor)

def _is_chinese(text):
    return text and any('\u4e00' <= c <= '\u9fff' for c in text)

# ========== 数据血缘 (sql_lineage_web) ==========
LINEAGE_DB = {
    'host': os.environ.get('LINEAGE_DB_HOST', '10.8.93.32'),
    'port': int(os.environ.get('LINEAGE_DB_PORT', 3306)),
    'user': os.environ.get('LINEAGE_DB_USER', 'root'),
    'password': os.environ.get('LINEAGE_DB_PASSWORD', 'dolphinscheduler@dt'),
    'database': os.environ.get('LINEAGE_DB_NAME', 'dolphinscheduler'),
}

class _LineageDb:
    def __init__(self):
        self.conn = None
    def get_conn(self):
        if not self.conn or not self.conn.is_connected():
            self.conn = mysql.connector.connect(**LINEAGE_DB)
        return self.conn
    def execute(self, query, params=None):
        conn = self.get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute(query, params or ())
        rows = cur.fetchall()
        for r in rows:
            for k, v in r.items():
                if isinstance(v, bytes):
                    try: r[k] = v.decode('utf-8')
                    except: r[k] = str(v)
        return rows

class _LineageAnalyzer:
    def __init__(self):
        self.db = _LineageDb()
    def search_tables(self, keyword):
        if not keyword or len(keyword) < 1:
            return []
        rows = self.db.execute("SELECT DISTINCT target_table_name as name FROM sql_lineage_results WHERE target_table_name LIKE %s ORDER BY target_table_name", (f"%{keyword}%",))
        return [r['name'] for r in rows if r.get('name')]
    def get_table_lineage(self, table_name):
        if not table_name:
            return None
        try:
            up = self.db.execute("SELECT source_table_name FROM sql_lineage_results WHERE target_table_name = %s AND error_message IS NULL AND source_table_name IS NOT NULL", (table_name,))
            down = self.db.execute("SELECT DISTINCT target_table_name FROM sql_lineage_results WHERE source_table_name LIKE %s AND error_message IS NULL", (f"%{table_name}%",))
            upstream = set()
            if up and up[0].get('source_table_name'):
                upstream = set(t.strip() for t in up[0]['source_table_name'].split(',') if t.strip())
            downstream = {r['target_table_name'] for r in down if r.get('target_table_name')}
            if not upstream and not downstream:
                return self._demo(table_name)
            return self._build(table_name, upstream, downstream)
        except Exception as e:
            return self._demo(table_name)
    def _demo(self, t):
        if t.startswith('ads.'):
            u, d = [f"dwd.{t.split('.')[1]}_detail","dim.product_info","dwd.user_behavior"],[f"report.{t.split('.')[1]}_summary","dashboard.sales_overview"]
        elif t.startswith('dwd.'):
            u, d = [f"ods.{t.split('.')[1]}_raw","ods.customer_info"],[f"ads.{t.split('.')[1]}_summary","dws.user_profile"]
        elif t.startswith('ods.'):
            u, d = ["source_system.sales_db","external_api.data_feed"],[f"dwd.{t.split('.')[1]}_processed"]
        else:
            u, d = [f"source.{t}","raw.{t}"],[f"target.{t}","report.{t}"]
        return self._build(t, set(u), set(d))
    def _build(self, center, up, down):
        layer = lambda x: 'ODS' if x.startswith('ods.') else 'DWD' if x.startswith('dwd.') else 'DWS' if x.startswith('dws.') else 'ADS' if x.startswith('ads.') else 'DIM' if x.startswith('dim.') else 'OTHER'
        nodes = [{'id':center,'name':center.split('.')[-1] if '.' in center else center,'type':'center','layer':layer(center)}]
        links = []
        for t in up:
            nodes.append({'id':t,'name':t.split('.')[-1] if '.' in t else t,'type':'upstream','layer':layer(t)})
            links.append({'source':t,'target':center,'type':'upstream'})
        for t in down:
            nodes.append({'id':t,'name':t.split('.')[-1] if '.' in t else t,'type':'downstream','layer':layer(t)})
            links.append({'source':center,'target':t,'type':'downstream'})
        return {'nodes':nodes,'links':links,'center_table':center,'upstream_count':len(up),'downstream_count':len(down)}

_lineage = _LineageAnalyzer()

# ========== 数据字典 Flask App ==========
_hive_app = Flask('hive_metadata',
    template_folder=str(_DATA_MAP / 'templates' / 'hive_metadata'),
    static_folder=str(_DATA_MAP / 'static' / 'hive_metadata'),
    static_url_path='/static')

EXCLUDE_KW = ['tmp','temp','bak','backup','test','demo','sample','实验','练习','old','废弃','deprecated','mid','middle','intermediate','中间','dev','develop','debug','开发']

@_hive_app.route('/')
def hive_index():
    base = (request.environ.get('SCRIPT_NAME') or '').rstrip('/')
    return render_template('index.html', schemas=HIVE_TARGET_SCHEMAS, base_path=base)

@_hive_app.route('/api/schemas')
def hive_schemas():
    return jsonify({'success':True,'schemas':HIVE_TARGET_SCHEMAS,'timestamp':datetime.now().isoformat()})

@_hive_app.route('/api/search-tables', methods=['POST'])
def hive_search():
    try:
        data = request.get_json() if request.is_json else request.form.to_dict()
        pattern = (data.get('table_pattern') or '').strip()
        search_type = data.get('search_type', 'table')
        if not pattern:
            return jsonify({'success':False,'error':'请输入关键词','timestamp':datetime.now().isoformat()}), 400
        conn = _get_hive_conn()
        try:
            with conn.cursor() as cur:
                if search_type == 'field':
                    sql = """SELECT DISTINCT t.tbl_name,tp.param_value as table_comment,t.tbl_id,d.name as schema_name,c.column_name,c.type_name as field_type,c.comment as field_comment
                        FROM tbls t LEFT JOIN table_params tp ON t.tbl_id=tp.tbl_id AND tp.param_key='comment' LEFT JOIN dbs d ON t.db_id=d.db_id
                        LEFT JOIN sds s ON t.sd_id=s.sd_id LEFT JOIN columns_v2 c ON s.cd_id=c.cd_id
                        WHERE d.name IN ('dwd','dws','ads') AND (CONVERT(c.comment USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s OR CONVERT(c.column_name USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s) ORDER BY CASE d.name WHEN 'dwd' THEN 1 WHEN 'dws' THEN 2 WHEN 'ads' THEN 3 END,t.tbl_name"""
                    params = [f'%{pattern}%', f'%{pattern}%']
                else:
                    sql = """SELECT t.tbl_name,tp.param_value as table_comment,t.tbl_id,d.name as schema_name FROM tbls t
                        LEFT JOIN table_params tp ON t.tbl_id=tp.tbl_id AND tp.param_key='comment' LEFT JOIN dbs d ON t.db_id=d.db_id
                        WHERE d.name IN ('dwd','dws','ads') AND (CONVERT(tp.param_value USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s OR CONVERT(t.tbl_name USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s)
                        ORDER BY CASE d.name WHEN 'dwd' THEN 1 WHEN 'dws' THEN 2 WHEN 'ads' THEN 3 END,t.tbl_name"""
                    params = [f'%{pattern}%', f'%{pattern}%']
                cur.execute(sql, params)
                rows = cur.fetchall()
                result = []
                for r in rows:
                    tn = r['tbl_name'].lower()
                    if any(kw in tn for kw in EXCLUDE_KW) or re.search(r'_20\d{2,}', r['tbl_name']):
                        continue
                    result.append({'table_name':r['tbl_name'],'table_comment':r.get('table_comment') or '暂无注释','schema':r['schema_name'],'table_id':r['tbl_id'],'search_type':search_type})
                return jsonify({'success':True,'results':result,'count':len(result),'search_pattern':pattern,'search_type':search_type,'timestamp':datetime.now().isoformat()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({'success':False,'error':str(e),'timestamp':datetime.now().isoformat()}), 500

@_hive_app.route('/api/table/<path:schema>/<path:table_name>')
def hive_table_api(schema, table_name):
    if schema not in HIVE_TARGET_SCHEMAS:
        return jsonify({'success':False,'error':f'只能查询 {", ".join(HIVE_TARGET_SCHEMAS)} 层','timestamp':datetime.now().isoformat()}), 403
    conn = _get_hive_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT a.tbl_name,c.param_value as table_comment,d.column_name,d.type_name,d.comment as column_comment,d.integer_idx
                FROM (SELECT sd_id,tbl_name,tbl_id FROM tbls WHERE tbl_name=%s AND db_id=(SELECT db_id FROM dbs WHERE name=%s)) a
                LEFT JOIN (SELECT cd_id,sd_id FROM sds) b ON a.sd_id=b.sd_id
                LEFT JOIN (SELECT param_value,tbl_id FROM table_params WHERE param_key='comment') c ON a.tbl_id=c.tbl_id
                LEFT JOIN (SELECT cd_id,column_name,type_name,comment,integer_idx FROM columns_v2) d ON b.cd_id=d.cd_id ORDER BY d.integer_idx""", (table_name, schema))
            rows = cur.fetchall()
            if not rows:
                return jsonify({'success':False,'error':'表不存在','timestamp':datetime.now().isoformat()}), 404
            cols = [{'name':r['column_name'],'type':r['type_name'],'comment':r['column_comment'] or '暂无注释','index':r['integer_idx']} for r in rows if r['column_name']]
            return jsonify({'success':True,'table_name':rows[0]['tbl_name'],'table_comment':rows[0]['table_comment'] or '暂无注释','schema':schema,'columns':cols})
    except Exception as e:
        return jsonify({'success':False,'error':str(e),'timestamp':datetime.now().isoformat()}), 500
    finally:
        conn.close()

@_hive_app.route('/table/<path:schema>/<path:table_name>')
def hive_table_page(schema, table_name):
    base = (request.environ.get('SCRIPT_NAME') or '').rstrip('/')
    return render_template('table_detail.html', schema=schema, table_name=table_name, base_path=base)

@_hive_app.route('/api/layer-stats')
def hive_layer_stats():
    conn = _get_hive_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT d.name as schema_name,COUNT(*) as table_count FROM tbls t JOIN dbs d ON t.db_id=d.db_id WHERE d.name IN ('dwd','dws','ads') GROUP BY d.name ORDER BY CASE d.name WHEN 'dwd' THEN 1 WHEN 'dws' THEN 2 WHEN 'ads' THEN 3 END")
            return jsonify({'success':True,'stats':cur.fetchall(),'timestamp':datetime.now().isoformat()})
    except Exception as e:
        return jsonify({'success':False,'error':str(e),'timestamp':datetime.now().isoformat()}), 500
    finally:
        conn.close()

# ========== 数据血缘 Flask App ==========
_lineage_app = Flask('sql_lineage_web',
    template_folder=str(_DATA_MAP / 'templates' / 'sql_lineage_web'),
    static_folder=str(_DATA_MAP / 'static' / 'sql_lineage_web') if (_DATA_MAP / 'static' / 'sql_lineage_web').exists() else None,
    static_url_path='/static')

@_lineage_app.route('/')
def lineage_index():
    base = (request.environ.get('SCRIPT_NAME') or '').rstrip('/')
    return render_template('index.html', base_path=base)

@_lineage_app.route('/api/search')
def lineage_search():
    kw = (request.args.get('keyword') or '').strip()
    if len(kw) < 1:
        return jsonify({'tables': []})
    try:
        return jsonify({'tables': _lineage.search_tables(kw)})
    except:
        return jsonify({'tables': [t for t in ["ads.sales_summary","dwd.sales_detail","ods.sales_order"] if kw.lower() in t.lower()]})

@_lineage_app.route('/api/lineage/<path:table_name>')
def lineage_api(table_name):
    try:
        g = _lineage.get_table_lineage(table_name)
        return jsonify(g) if g else jsonify({'error':'未找到','nodes':[],'links':[],'center_table':table_name,'upstream_count':0,'downstream_count':0})
    except:
        return jsonify({'error':'服务器错误','nodes':[],'links':[],'center_table':table_name,'upstream_count':0,'downstream_count':0})

# ========== 数据地图主入口 ==========
_main = Flask('data_map')
_main.config.from_object(PortalConfig)
auth_config = {"AUTH_COOKIE_NAME": _main.config["AUTH_COOKIE_NAME"], "SECRET_KEY": _main.config["SECRET_KEY"], "AUTH_COOKIE_MAX_AGE": _main.config["AUTH_COOKIE_MAX_AGE"]}

@_main.route("/login")
@_main.route("/logout")
def _redirect_login():
    from flask import redirect
    base = _main.config.get("PORTAL_URL", "http://127.0.0.1:5000").rstrip("/")
    p = "logout" if request.path.strip("/") == "logout" else "login"
    return redirect(f"{base}/{p}?next={request.url}")

@_main.route("/")
def _nav():
    base = (request.environ.get("SCRIPT_NAME") or "").rstrip("/")
    return f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><title>数据地图</title>
<style>body{{font-family:sans-serif;margin:3rem auto;max-width:600px;}}a{{display:block;padding:0.75rem;margin:0.5rem 0;background:#f0f0f0;border-radius:6px;color:#333;text-decoration:none;}}a:hover{{background:#e0e0e0;}}</style>
<body><h1>数据地图</h1><p>请选择：</p><a href="{base}/hive_metadata/">数据字典</a><a href="{base}/sql_lineage_web/">数据血缘</a></body></html>'''

app = DispatcherMiddleware(_main, {
    "/hive_metadata": _hive_app,
    "/sql_lineage_web": _lineage_app,
})
app = auth_middleware(app, auth_config)

if __name__ == "__main__":
    port = int(os.environ.get("DATA_MAP_PORT", "5001"))
    base = os.environ.get("DATA_MAP_BASE", "").rstrip("/") or ""
    print("数据地图（端口 %s）: http://127.0.0.1:%s%s" % (port, port, base or "/"))
    print("  - 数据字典: http://127.0.0.1:%s%s/hive_metadata/" % (port, base))
    print("  - 数据血缘: http://127.0.0.1:%s%s/sql_lineage_web/" % (port, base))
    from werkzeug.serving import run_simple
    # 关闭 reloader：通过 run_data_map.py(runpy) 启动时 reloader 子进程会报 No module named app
    use_debug = os.environ.get("DATA_MAP_DEBUG", os.environ.get("PORTAL_DEBUG", "0")).lower() in ("1", "true", "yes")
    run_simple("0.0.0.0", port, app, use_debugger=use_debug, use_reloader=False, threaded=True)
