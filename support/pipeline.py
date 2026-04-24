# -*- coding: utf-8 -*-
"""Excel → tmp 库 Hive 表流水线（support 模块内部使用）"""
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd

CHINESE_TO_ENGLISH = {
    "备注": "remark", "用户ID": "user_id", "用户名": "user_name", "添加时间": "add_time",
    "标签名称": "tag_name", "标签组名称": "tag_group_name", "姓名": "name", "名称": "name",
    "时间": "time", "日期": "date", "金额": "amount", "数量": "quantity", "编号": "code",
    "类型": "type", "状态": "status", "描述": "description", "电话": "phone", "手机": "mobile",
    "邮箱": "email", "地址": "address", "客户名称": "customer_name", "客户": "customer",
    "订单号": "order_no", "订单": "order", "产品名称": "product_name", "产品": "product",
    "支付时间": "pay_time", "取消时间": "cancel_time", "支付方式": "pay_type", "服务天数": "service_days",
    "账号": "account", "资金账号": "fund_account", "唯一编码": "sole_code",
}

_SUPPORT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SUPPORT_DIR.parent
try:
    _map_file = _SUPPORT_DIR / "column_map.json"
    if _map_file.exists():
        import json
        for k, v in json.loads(_map_file.read_text(encoding="utf-8")).items():
            if isinstance(k, str) and isinstance(v, str) and k.strip():
                CHINESE_TO_ENGLISH[k.strip()] = v.strip()
    _csv = _PROJECT_ROOT / "c2e.csv"
    if _csv.exists():
        import csv
        with _csv.open("r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 2:
                    continue
                c0, c1 = (row[0] or "").strip(), (row[1] or "").strip()
                if not c0 and not c1:
                    continue
                _hc = lambda s: any('\u4e00' <= ch <= '\u9fff' for ch in s)
                if _hc(c0) and not _hc(c1):
                    zh, en = c0, c1
                elif _hc(c1) and not _hc(c0):
                    zh, en = c1, c0
                else:
                    zh, en = c0, c1
                if zh and en:
                    CHINESE_TO_ENGLISH[zh] = en
except Exception:
    pass

def _fuzzy_chinese_to_english(header: str) -> Optional[str]:
    s = (header or "").strip()
    if not s:
        return None
    candidates = [(len(zh), en) for zh, en in CHINESE_TO_ENGLISH.items() if zh and (zh in s or s in zh)]
    return max(candidates, key=lambda x: x[0])[1] if candidates else None

def _split_zh_terms(header: str) -> List[str]:
    s = (header or "").strip()
    return [p.strip() for p in re.split(r"[()\（\）\[\]【】,，、\s]+", s) if p and p.strip()]

def _allowed_ext(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[-1].lower() in ("xlsx", "xls")

def read_excel(path: str, sheet: int = 0, header: Optional[int] = None) -> pd.DataFrame:
    ext = path.rsplit(".", 1)[-1].lower()
    engine = "openpyxl" if ext == "xlsx" else "xlrd"
    return pd.read_excel(path, sheet_name=sheet, header=header, engine=engine)

def detect_has_header(df_raw: pd.DataFrame) -> bool:
    if df_raw is None or df_raw.empty or len(df_raw) < 2:
        return True
    first, second = df_raw.iloc[0], df_raw.iloc[1]
    fsc = sum(1 for v in first if isinstance(v, str) or (pd.notna(v) and not isinstance(v, (int, float))))
    ssc = sum(1 for v in second if isinstance(v, str) or (pd.notna(v) and not isinstance(v, (int, float))))
    return fsc >= ssc and fsc >= len(first) // 2

def _header_to_english(header: str, fallback_idx: int) -> str:
    if header is None or (isinstance(header, float) and pd.isna(header)):
        return f"col_{fallback_idx}"
    s = str(header).strip()
    if not s:
        return f"col_{fallback_idx}"
    if s in CHINESE_TO_ENGLISH:
        return CHINESE_TO_ENGLISH[s]
    f = _fuzzy_chinese_to_english(s)
    if f:
        return f
    for term in _split_zh_terms(s):
        t = _fuzzy_chinese_to_english(term)
        if t:
            return t
    clean = re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", s)).strip("_")
    return clean.lower() if clean and len(clean) <= 32 else f"col_{fallback_idx}"

def infer_schema(df: pd.DataFrame) -> List[Tuple[str, str, str]]:
    seen = {}
    cols = []
    for i, c in enumerate(df.columns):
        orig = str(c).strip() if c is not None and not (isinstance(c, float) and pd.isna(c)) else ""
        base = _header_to_english(orig, i)
        name, idx = base, 0
        while name in seen:
            idx += 1
            name = f"{base}_{idx}"
        seen[name] = True
        cols.append((name, "STRING", orig or f"col_{i}"))
    return cols

def sanitize_column_names(column_names: List[str], fallback_count: int) -> List[str]:
    names = list(column_names or [])
    seen = {}
    out: List[str] = []
    total = max(fallback_count, len(names))
    for i in range(total):
        raw = names[i] if i < len(names) else ""
        clean = re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", str(raw or "").strip())).strip("_").lower()
        if not clean:
            clean = f"col_{i}"
        name, idx = clean, 0
        while name in seen:
            idx += 1
            name = f"{clean}_{idx}"
        seen[name] = True
        out.append(name)
    return out

def build_create_external_ddl(database: str, table_name: str, columns: List[Tuple[str, str, str]], has_header: bool = True, encoding: str = "UTF-8", table_comment: Optional[str] = None) -> str:
    def esc(s): return (s or "").replace("'", "''")
    col_defs = ", ".join(f"`{c[0]}` {c[1]} COMMENT '{esc(c[2])}'" for c in columns)
    tbl_c = table_comment or table_name
    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table_name}` (\n  " + col_defs.replace(", ", "\n  , ") + f"\n)\nCOMMENT '{esc(tbl_c)}'\nROW FORMAT DELIMITED FIELDS TERMINATED BY ','\nWITH SERDEPROPERTIES ('field.delim'=',', 'serialization.encoding'='{encoding}', 'serialization.format'=',')\nSTORED AS TEXTFILE\n"
    if has_header:
        ddl += "TBLPROPERTIES ('skip.header.line.count'='1')\n"
    return ddl

def upload_to_hdfs(local_path: str, hdfs_path: str, run_as_user: Optional[str] = None) -> None:
    env = os.environ.copy()
    try:
        import pwd
        current_user = pwd.getpwuid(os.getuid()).pw_name
    except Exception:
        current_user = os.environ.get("USER", "")
    use_sudo = run_as_user and run_as_user != current_user
    def _hdfs(cmd):
        full = ["hdfs", "dfs"] + cmd
        if use_sudo:
            full = ["sudo", "-n", "-u", run_as_user] + full
        subprocess.run(full, check=True, capture_output=True, text=True, env=env)
    _hdfs(["-put", "-f", local_path, hdfs_path])

def _impala_shell_env():
    env = os.environ.copy()
    egg_cache = os.path.join(tempfile.gettempdir(), "python-eggs")
    try:
        os.makedirs(egg_cache, mode=0o777, exist_ok=True)
    except OSError:
        pass
    env["PYTHON_EGG_CACHE"] = egg_cache
    return env

def _run_impala_shell(host: str, port: int, tmp_sql: str, user: Optional[str] = None, password: Optional[str] = None, run_as_os_user: Optional[str] = None) -> None:
    connect_user = user or run_as_os_user
    cmd = ["impala-shell", "-i", f"{host}:{port}", "-f", tmp_sql]
    if connect_user:
        cmd.extend(["-u", connect_user])
    if password:
        cmd.extend(["--password", password])
    env = _impala_shell_env()
    if run_as_os_user:
        for wrapper in (["runuser", "-u", run_as_os_user, "--"], ["sudo", "-n", "-u", run_as_os_user]):
            try:
                subprocess.run(wrapper + cmd, check=True, capture_output=True, text=True, env=env)
                return
            except (FileNotFoundError, subprocess.CalledProcessError):
                continue
    subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)

def run_impala_sql(host: str, port: int, sql: str, user: Optional[str] = None, password: Optional[str] = None, run_as_os_user: Optional[str] = None) -> None:
    if run_as_os_user:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql)
            tmp_sql = f.name
        try:
            _run_impala_shell(host, port, tmp_sql, user, password, run_as_os_user)
        finally:
            os.unlink(tmp_sql)
        return
    try:
        from impala.dbapi import connect
        conn = connect(host=host, port=port, user=user or run_as_os_user or "root", password=password or "")
        cur = conn.cursor()
        for stmt in sql.split(";"):
            if stmt.strip():
                cur.execute(stmt.strip())
        cur.close()
        conn.close()
    except ImportError:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(sql)
            tmp_sql = f.name
        try:
            _run_impala_shell(host, port, tmp_sql, user, password, run_as_os_user)
        finally:
            os.unlink(tmp_sql)

def run_pipeline(excel_path: str, table_name: str, tmp_database: str, hdfs_base_path: str, impala_host: str, impala_port: int, impala_user: Optional[str] = None, impala_password: Optional[str] = None, impala_run_as_os_user: Optional[str] = None, hdfs_upload_as_user: Optional[str] = None, sheet: int = 0, csv_encoding: str = "GBK", table_comment: Optional[str] = None, custom_columns: Optional[List[str]] = None, replace_table: bool = True) -> dict:
    result = {"ok": False, "message": "", "has_header": True, "rows": 0, "table": table_name, "hdfs_path": "", "ddl": ""}
    if not _allowed_ext(excel_path):
        result["message"] = "仅支持 .xlsx / .xls 文件"
        return result
    df_raw = read_excel(excel_path, sheet=sheet, header=None)
    if df_raw.empty:
        result["message"] = "Excel 为空或无法读取"
        return result
    has_header = detect_has_header(df_raw)
    result["has_header"] = has_header
    header = 0 if has_header else None
    df = read_excel(excel_path, sheet=sheet, header=header)
    if not has_header:
        df.columns = [f"col_{i}" for i in range(len(df.columns))]
    result["rows"] = len(df)
    schema = infer_schema(df)
    if custom_columns:
        fixed = sanitize_column_names(custom_columns, len(schema))
        schema = [(fixed[i], schema[i][1], schema[i][2]) for i in range(len(schema))]
    safe_table = re.sub(r"[^\w]", "_", table_name).strip("_") or "excel_table"
    hdfs_dir = f"{hdfs_base_path.rstrip('/')}/{safe_table}"
    result["hdfs_path"] = hdfs_dir
    ddl = build_create_external_ddl(tmp_database, safe_table, schema, has_header=has_header, encoding=csv_encoding, table_comment=table_comment)
    result["ddl"] = ddl
    result["table"] = f"{tmp_database}.{safe_table}"
    if replace_table:
        run_impala_sql(
            impala_host,
            impala_port,
            f"DROP TABLE IF EXISTS `{tmp_database}`.`{safe_table}`;",
            user=impala_user,
            password=impala_password,
            run_as_os_user=impala_run_as_os_user,
        )
    run_impala_sql(impala_host, impala_port, ddl, user=impala_user, password=impala_password, run_as_os_user=impala_run_as_os_user)
    fd, csv_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    try:
        with open(csv_path, "w", encoding=csv_encoding, errors="replace", newline="") as f:
            df.to_csv(f, index=False, sep=",")
        if hdfs_upload_as_user:
            os.chmod(csv_path, 0o644)
        csv_file_hdfs = f"{hdfs_dir}/data.csv"
        upload_to_hdfs(csv_path, csv_file_hdfs, run_as_user=hdfs_upload_as_user)
        run_impala_sql(impala_host, impala_port, f"REFRESH `{tmp_database}`.`{safe_table}`;", user=impala_user, password=impala_password, run_as_os_user=impala_run_as_os_user)
        result["ok"] = True
        result["message"] = f"已建表 {result['table']}，数据行数 {result['rows']}，已刷新。"
    except subprocess.CalledProcessError as e:
        result["message"] = f"HDFS 或 Impala 执行失败: {e.stderr or str(e)}"
    except Exception as e:
        result["message"] = str(e)
    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)
    return result
