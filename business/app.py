# -*- coding: utf-8 -*-
"""
业务应用 - 单入口 app.py，合并 sr_api（实时加微监测等）。
business 下仅 app.py，frontend、sql 为资产目录。
"""
import os
import io
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from typing import List, Dict, Any, Optional
import pymysql
from contextlib import contextmanager
from datetime import datetime

from pydantic import BaseModel

_BUSINESS = Path(__file__).resolve().parent
FRONTEND_DIST = _BUSINESS / "frontend" / "dist"
# 独立运行时 API 在根路径 MOUNT_PATH=""；挂到 run_all 时为 /business/realtime_mv
MOUNT_PATH = (
    (os.environ.get("SR_API_MOUNT_PATH") or "").rstrip("/")
    if os.environ.get("BUSINESS_PORT")
    else (os.environ.get("SR_API_MOUNT_PATH") or "/business/realtime_mv").rstrip("/")
)

# 配置（从 .env 或环境变量）
try:
    from pydantic import BaseSettings
    class _Settings(BaseSettings):
        starrocks_host: str = "127.0.0.1"
        starrocks_port: int = 9030
        starrocks_user: str = "root"
        starrocks_password: str = ""
        starrocks_database: str = "your_database"
        mv_tables: str = "mv_table1,mv_table2"
        class Config:
            env_file = str(_BUSINESS / ".env")
            extra = "ignore"
    _settings = _Settings()
except Exception:
    class _Settings:
        starrocks_host = os.environ.get("STARROCKS_HOST", "127.0.0.1")
        starrocks_port = int(os.environ.get("STARROCKS_PORT", "9030"))
        starrocks_user = os.environ.get("STARROCKS_USER", "root")
        starrocks_password = os.environ.get("STARROCKS_PASSWORD", "")
        starrocks_database = os.environ.get("STARROCKS_DATABASE", "your_database")
        mv_tables = os.environ.get("MV_TABLES", "mv_table1,mv_table2")
    _settings = _Settings()

def _mv_list():
    return [t.strip() for t in _settings.mv_tables.split(",") if t.strip()]

def _get_conn(connect_timeout: int = 10):
    """DB 连接，避免内网不可达时长时间挂死。"""
    return pymysql.connect(
        host=_settings.starrocks_host, port=_settings.starrocks_port,
        user=_settings.starrocks_user, password=_settings.starrocks_password,
        database=_settings.starrocks_database, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=connect_timeout,
    )

@contextmanager
def _db_cursor():
    conn = _get_conn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# -------------------- 业务配置：只读账号（可选） --------------------
#
# 默认不启用任何限制；如需只读账号，请在服务启动时设置环境变量：
# - BUSINESS_READONLY_USERS: 逗号分隔用户名列表，例如 "yewu_ro,readonly"
#
# 网关/门户侧需要将登录用户名透传到请求头（任一即可）：
# - X-User / X-Username / X-Forwarded-User / X-Portal-User
_READONLY_USERS = {u.strip() for u in (os.environ.get("BUSINESS_READONLY_USERS") or "").split(",") if u.strip()}
_USER_HEADER_KEYS = ("x-user", "x-username", "x-forwarded-user", "x-portal-user")


def _get_request_user(request: Request) -> str:
    # 1) 反向代理/网关注入的用户头
    for k in _USER_HEADER_KEYS:
        v = request.headers.get(k)
        if v:
            return v.strip()
    # 2) 同源门户模式：浏览器会带 portal_auth Cookie（itsdangerous 签名）
    try:
        cookie_val = request.cookies.get("portal_auth") or ""
        secret = os.environ.get("PORTAL_SECRET_KEY") or ""
        if cookie_val and secret:
            try:
                from itsdangerous import URLSafeTimedSerializer
                s = URLSafeTimedSerializer(secret, salt="portal-auth")
                data = s.loads(cookie_val, max_age=int(os.environ.get("PORTAL_AUTH_COOKIE_MAX_AGE", "86400")))
                u = (data.get("u") or "").strip()
                if u:
                    return u
            except Exception:
                pass
    except Exception:
        pass
    # 3) 多端口 iframe：portal_token（短期有效）
    try:
        token = (request.query_params.get("portal_token") or "").strip()
        secret = os.environ.get("PORTAL_SECRET_KEY") or ""
        if token and secret:
            from itsdangerous import URLSafeTimedSerializer
            s = URLSafeTimedSerializer(secret, salt="portal-token")
            data = s.loads(token, max_age=600)
            u = (data.get("u") or "").strip()
            if u:
                return u
    except Exception:
        pass
    return ""


def _reject_if_readonly(request: Request) -> None:
    if not _READONLY_USERS:
        return
    user = _get_request_user(request)
    if user and user in _READONLY_USERS:
        raise HTTPException(status_code=403, detail="只读账号不允许修改配置数据")

# 自营渠道每日汇总表：支持按 dt 日期筛选，默认今天+昨天
DAILY_STATS_TABLE = "mv_scrm_open_channel_tag_stats"

def _query_mv_data(
    table_name: str,
    limit: int = 1000,
    tag_name: Optional[str] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
    dt_all: bool = False,
) -> List[Dict]:
    if table_name not in _mv_list():
        raise ValueError(f"表 {table_name} 未在配置中")
    with _db_cursor() as cur:
        conditions = []
        args: List[Any] = []
        if tag_name:
            conditions.append("tag_name = %s")
            args.append(tag_name)
        # 自营渠道每日表：按 dt 筛选；未传且非“全部”时默认今天和昨天
        if table_name == DAILY_STATS_TABLE:
            if dt_all:
                pass  # 全部日期，不限制 dt
            elif dt_from and dt_to:
                conditions.append("dt BETWEEN %s AND %s")
                args.extend([dt_from, dt_to])
            else:
                from datetime import date, timedelta
                today = date.today()
                yesterday = today - timedelta(days=1)
                conditions.append("dt BETWEEN %s AND %s")
                args.extend([yesterday.isoformat(), today.isoformat()])
        if conditions:
            where_sql = " AND ".join(conditions)
            args.append(limit)
            order = " ORDER BY dt DESC, open_channel, wechat_customer_tag" if table_name == DAILY_STATS_TABLE else ""
            cur.execute(f"SELECT * FROM `{table_name}` WHERE {where_sql}{order} LIMIT %s", tuple(args))
        else:
            order = " ORDER BY dt DESC" if table_name == DAILY_STATS_TABLE else ""
            cur.execute(f"SELECT * FROM `{table_name}`{order} LIMIT %s", (limit,))
        return cur.fetchall()

def _query_distinct_tags(table_name: str) -> List[str]:
    if table_name not in _mv_list():
        raise ValueError(f"表 {table_name} 未在配置中")
    with _db_cursor() as cur:
        cur.execute(f"SELECT DISTINCT tag_name FROM `{table_name}` WHERE tag_name IS NOT NULL AND tag_name != '' ORDER BY tag_name")
        return [r["tag_name"] for r in cur.fetchall()]

def _get_table_columns(table_name: str) -> List[Dict]:
    with _db_cursor() as cur:
        cur.execute(f"DESCRIBE `{table_name}`")
        return cur.fetchall()


# -------------------- 配置表：开户渠道 & 企微客户标签 --------------------

CONFIG_OPEN_CHANNEL_TABLE = "config_open_channel_tag"
CONFIG_CHANNEL_STAFF_TABLE = "config_channel_staff"
CONFIG_CODE_MAPPING_TABLE = "code_mapping"
CONFIG_STOCK_POSITION_TABLE = "config_stock_position"


class OpenChannelTagBase(BaseModel):
    open_channel: str
    wechat_customer_tag: str


class OpenChannelTagCreate(OpenChannelTagBase):
    pass


class OpenChannelTagUpdate(BaseModel):
    open_channel: Optional[str] = None
    wechat_customer_tag: Optional[str] = None


class OpenChannelTagOut(OpenChannelTagBase):
    id: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ChannelStaffBase(BaseModel):
    branch_name: str  # 营业部
    staff_name: str   # 姓名


class ChannelStaffCreate(ChannelStaffBase):
    pass


class ChannelStaffUpdate(BaseModel):
    branch_name: Optional[str] = None
    staff_name: Optional[str] = None


class ChannelStaffOut(ChannelStaffBase):
    id: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


_DT_FMT = "%Y-%m-%d %H:%M:%S"


def _fmt_dt(v: Any) -> Optional[str]:
    """把数据库的时间字段格式化为 'YYYY-MM-DD HH:MM:SS'。"""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime(_DT_FMT)
    # 兼容某些驱动/查询返回 str
    try:
        s = str(v)
        if "T" in s and len(s) >= 19:
            # 2026-02-06T13:13:13 -> 2026-02-06 13:13:13
            s = s.replace("T", " ", 1)
        return s
    except Exception:
        return None


def _row_config_time_fix(row: Dict[str, Any]) -> Dict[str, Any]:
    if not row:
        return row
    row = dict(row)
    if "created_at" in row:
        row["created_at"] = _fmt_dt(row.get("created_at"))
    if "updated_at" in row:
        row["updated_at"] = _fmt_dt(row.get("updated_at"))
    return row


def _row_code_mapping_fix(row: Dict[str, Any]) -> Dict[str, Any]:
    """格式化 code_mapping 的时间/数值字段，便于前端展示。"""
    if not row:
        return row
    row = dict(row)
    if "created_time" in row:
        row["created_time"] = _fmt_dt(row.get("created_time"))
    # DECIMAL 可能返回 Decimal，这里转成 float（前端只做展示/编辑）
    if "stat_cost" in row and row.get("stat_cost") is not None:
        try:
            row["stat_cost"] = float(row["stat_cost"])
        except Exception:
            pass
    return row


class CodeMappingBase(BaseModel):
    code_value: str
    description: Optional[str] = None
    stat_cost: Optional[float] = None
    channel_name: Optional[str] = None


class CodeMappingCreate(CodeMappingBase):
    id: int


class CodeMappingUpdate(BaseModel):
    code_value: Optional[str] = None
    description: Optional[str] = None
    stat_cost: Optional[float] = None
    channel_name: Optional[str] = None


class CodeMappingOut(CodeMappingBase):
    id: int
    created_time: Optional[str] = None


# -------------------- 配置表：股票仓位/买卖 --------------------

class StockPositionBase(BaseModel):
    product_name: Optional[str] = None
    trade_date: Optional[str] = None   # YYYY-MM-DD
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    position_pct: Optional[float] = None
    side: Optional[str] = None        # 买入 / 卖出
    price: Optional[float] = None


class StockPositionCreate(StockPositionBase):
    stock_code: str
    position_pct: float
    side: str
    price: float


class StockPositionUpdate(BaseModel):
    product_name: Optional[str] = None
    trade_date: Optional[str] = None
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    position_pct: Optional[float] = None
    side: Optional[str] = None
    price: Optional[float] = None


class StockPositionOut(StockPositionBase):
    id: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


def _row_stock_position_fix(row: Dict[str, Any]) -> Dict[str, Any]:
    if not row:
        return row
    row = dict(row)
    if "trade_date" in row and row.get("trade_date") is not None:
        v = row["trade_date"]
        if hasattr(v, "strftime"):
            row["trade_date"] = v.strftime("%Y-%m-%d")
        else:
            row["trade_date"] = str(v)[:10]
    if "created_at" in row:
        row["created_at"] = _fmt_dt(row.get("created_at"))
    if "updated_at" in row:
        row["updated_at"] = _fmt_dt(row.get("updated_at"))
    for key in ("position_pct", "price"):
        if key in row and row.get(key) is not None:
            try:
                row[key] = float(row[key])
            except Exception:
                pass
    return row


def _next_config_id(cur, table_name: str) -> int:
    """
    返回最小可用正整数 id（从 1 开始，删除后会补空缺）。
    适用于配置表这种小表。
    """
    cur.execute(f"SELECT 1 FROM `{table_name}` WHERE id = 1 LIMIT 1")
    if not cur.fetchone():
        return 1
    # 找到第一个断点：存在 t1.id，但不存在 t1.id+1
    cur.execute(
        f"SELECT t1.id + 1 AS next_id "
        f"FROM `{table_name}` t1 "
        f"LEFT JOIN `{table_name}` t2 ON t2.id = t1.id + 1 "
        f"WHERE t2.id IS NULL "
        f"ORDER BY t1.id "
        f"LIMIT 1"
    )
    r = cur.fetchone() or {}
    nid = r.get("next_id")
    try:
        nid = int(nid)
    except Exception:
        nid = 1
    return nid if nid >= 1 else 1

app = FastAPI(title="StarRocks MV 数据服务", description="查询 StarRocks 物化视图", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def allow_frame_embedding(request, call_next):
    """允许被门户 iframe 嵌入（门户与业务不同端口，需允许跨端口嵌入）"""
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    return response


@app.get("/health")
async def health():
    return {"status": "ok", "message": "StarRocks MV 数据服务运行中"}

@app.get("/api/tables")
async def get_tables() -> List[str]:
    return _mv_list()

@app.get("/api/data/{table_name}")
async def get_table_data(
    table_name: str,
    limit: int = 1000,
    tag_name: Optional[str] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
    dt_all: Optional[bool] = None,
) -> Dict[str, Any]:
    if table_name not in _mv_list():
        raise HTTPException(status_code=404, detail=f"表 {table_name} 未配置或不存在")
    try:
        data = _query_mv_data(
            table_name,
            limit=limit,
            tag_name=tag_name or None,
            dt_from=dt_from,
            dt_to=dt_to,
            dt_all=bool(dt_all) if dt_all is not None else False,
        )
        return {"table": table_name, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tags/{table_name}")
async def get_tags(table_name: str) -> List[str]:
    if table_name not in _mv_list():
        raise HTTPException(status_code=404, detail=f"表 {table_name} 未配置或不存在")
    try:
        return _query_distinct_tags(table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schema/{table_name}")
async def get_table_schema(table_name: str) -> Dict[str, Any]:
    if table_name not in _mv_list():
        raise HTTPException(status_code=404, detail=f"表 {table_name} 未配置或不存在")
    try:
        return {"table": table_name, "columns": _get_table_columns(table_name)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------- API：开户渠道 & 企微客户标签配置 --------------------

@app.get("/api/config/open-channel-tags", response_model=List[OpenChannelTagOut])
async def list_open_channel_tags() -> List[OpenChannelTagOut]:
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, open_channel, wechat_customer_tag, created_at, updated_at "
                f"FROM `{CONFIG_OPEN_CHANNEL_TABLE}` ORDER BY id ASC"
            )
            rows = cur.fetchall()
        return [OpenChannelTagOut(**_row_config_time_fix(row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config/open-channel-tags", response_model=OpenChannelTagOut)
async def create_open_channel_tag(body: OpenChannelTagCreate, request: Request) -> OpenChannelTagOut:
    _reject_if_readonly(request)
    try:
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            next_id = _next_config_id(cur, CONFIG_OPEN_CHANNEL_TABLE)
            cur.execute(
                f"INSERT INTO `{CONFIG_OPEN_CHANNEL_TABLE}` "
                f"(id, open_channel, wechat_customer_tag, created_at, updated_at) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (next_id, body.open_channel, body.wechat_customer_tag, now_str, now_str),
            )
            # 重新查询最新一条记录返回
            cur.execute(
                f"SELECT id, open_channel, wechat_customer_tag, created_at, updated_at "
                f"FROM `{CONFIG_OPEN_CHANNEL_TABLE}` WHERE id = %s",
                (next_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="创建失败")
        return OpenChannelTagOut(**_row_config_time_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/config/open-channel-tags/{item_id}", response_model=OpenChannelTagOut)
async def update_open_channel_tag(item_id: int, body: OpenChannelTagUpdate, request: Request) -> OpenChannelTagOut:
    _reject_if_readonly(request)
    if body.open_channel is None and body.wechat_customer_tag is None:
        raise HTTPException(status_code=400, detail="至少提供一个需要更新的字段")
    try:
        # 兼容 StarRocks：部分表不支持 UPDATE（会报 does not support update）
        # 用“查旧值 -> DELETE -> INSERT”实现修改，保留 created_at，仅更新 updated_at。
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, open_channel, wechat_customer_tag, created_at, updated_at "
                f"FROM `{CONFIG_OPEN_CHANNEL_TABLE}` WHERE id = %s",
                (item_id,),
            )
            old = cur.fetchone()
            if not old:
                raise HTTPException(status_code=404, detail="记录不存在")

            new_open = body.open_channel if body.open_channel is not None else old.get("open_channel")
            new_tag = body.wechat_customer_tag if body.wechat_customer_tag is not None else old.get("wechat_customer_tag")
            created_at = _fmt_dt(old.get("created_at")) or now_str

            cur.execute(
                f"DELETE FROM `{CONFIG_OPEN_CHANNEL_TABLE}` WHERE id = %s",
                (item_id,),
            )
            cur.execute(
                f"INSERT INTO `{CONFIG_OPEN_CHANNEL_TABLE}` "
                f"(id, open_channel, wechat_customer_tag, created_at, updated_at) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (item_id, new_open, new_tag, created_at, now_str),
            )
            cur.execute(
                f"SELECT id, open_channel, wechat_customer_tag, created_at, updated_at "
                f"FROM `{CONFIG_OPEN_CHANNEL_TABLE}` WHERE id = %s",
                (item_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="记录不存在")
        return OpenChannelTagOut(**_row_config_time_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/config/open-channel-tags/{item_id}")
async def delete_open_channel_tag(item_id: int, request: Request) -> Dict[str, Any]:
    _reject_if_readonly(request)
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"DELETE FROM `{CONFIG_OPEN_CHANNEL_TABLE}` WHERE id = %s",
                (item_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="记录不存在")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------- API：投流渠道承接员工配置表 --------------------

@app.get("/api/config/channel-staff", response_model=List[ChannelStaffOut])
async def list_channel_staff() -> List[ChannelStaffOut]:
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, branch_name, staff_name, created_at, updated_at "
                f"FROM `{CONFIG_CHANNEL_STAFF_TABLE}` ORDER BY id ASC"
            )
            rows = cur.fetchall()
        return [ChannelStaffOut(**_row_config_time_fix(row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config/channel-staff", response_model=ChannelStaffOut)
async def create_channel_staff(body: ChannelStaffCreate, request: Request) -> ChannelStaffOut:
    _reject_if_readonly(request)
    try:
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            next_id = _next_config_id(cur, CONFIG_CHANNEL_STAFF_TABLE)
            cur.execute(
                f"INSERT INTO `{CONFIG_CHANNEL_STAFF_TABLE}` "
                f"(id, branch_name, staff_name, created_at, updated_at) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (next_id, body.branch_name, body.staff_name, now_str, now_str),
            )
            cur.execute(
                f"SELECT id, branch_name, staff_name, created_at, updated_at "
                f"FROM `{CONFIG_CHANNEL_STAFF_TABLE}` WHERE id = %s",
                (next_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="创建失败")
        return ChannelStaffOut(**_row_config_time_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/config/channel-staff/{item_id}", response_model=ChannelStaffOut)
async def update_channel_staff(item_id: int, body: ChannelStaffUpdate, request: Request) -> ChannelStaffOut:
    _reject_if_readonly(request)
    if body.branch_name is None and body.staff_name is None:
        raise HTTPException(status_code=400, detail="至少提供一个需要更新的字段")
    try:
        # StarRocks 部分表不支持 UPDATE（会报 1064 does not support update）
        # 这里用“查旧值 -> DELETE -> INSERT”实现修改。
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, branch_name, staff_name, created_at, updated_at "
                f"FROM `{CONFIG_CHANNEL_STAFF_TABLE}` WHERE id = %s",
                (item_id,),
            )
            old = cur.fetchone()
            if not old:
                raise HTTPException(status_code=404, detail="记录不存在")

            new_branch = body.branch_name if body.branch_name is not None else old.get("branch_name")
            new_staff = body.staff_name if body.staff_name is not None else old.get("staff_name")
            created_at = _fmt_dt(old.get("created_at")) or now_str

            cur.execute(
                f"DELETE FROM `{CONFIG_CHANNEL_STAFF_TABLE}` WHERE id = %s",
                (item_id,),
            )
            cur.execute(
                f"INSERT INTO `{CONFIG_CHANNEL_STAFF_TABLE}` "
                f"(id, branch_name, staff_name, created_at, updated_at) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (item_id, new_branch, new_staff, created_at, now_str),
            )
            cur.execute(
                f"SELECT id, branch_name, staff_name, created_at, updated_at "
                f"FROM `{CONFIG_CHANNEL_STAFF_TABLE}` WHERE id = %s",
                (item_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="记录不存在")
        return ChannelStaffOut(**_row_config_time_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/config/channel-staff/{item_id}")
async def delete_channel_staff(item_id: int, request: Request) -> Dict[str, Any]:
    _reject_if_readonly(request)
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"DELETE FROM `{CONFIG_CHANNEL_STAFF_TABLE}` WHERE id = %s",
                (item_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="记录不存在")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------- API：渠道映射/消耗（code_mapping） --------------------

@app.get("/api/config/code-mapping", response_model=List[CodeMappingOut])
async def list_code_mapping() -> List[CodeMappingOut]:
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, code_value, description, stat_cost, channel_name, created_time "
                f"FROM `{CONFIG_CODE_MAPPING_TABLE}` ORDER BY id ASC"
            )
            rows = cur.fetchall()
        return [CodeMappingOut(**_row_code_mapping_fix(row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config/code-mapping", response_model=CodeMappingOut)
async def create_code_mapping(body: CodeMappingCreate, request: Request) -> CodeMappingOut:
    _reject_if_readonly(request)
    try:
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            cur.execute(f"SELECT 1 FROM `{CONFIG_CODE_MAPPING_TABLE}` WHERE id = %s LIMIT 1", (body.id,))
            if cur.fetchone():
                raise HTTPException(status_code=400, detail="ID 已存在")
            cur.execute(
                f"INSERT INTO `{CONFIG_CODE_MAPPING_TABLE}` "
                f"(id, code_value, description, stat_cost, channel_name, created_time) "
                f"VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    int(body.id),
                    body.code_value,
                    body.description,
                    body.stat_cost,
                    body.channel_name,
                    now_str,
                ),
            )
            cur.execute(
                f"SELECT id, code_value, description, stat_cost, channel_name, created_time "
                f"FROM `{CONFIG_CODE_MAPPING_TABLE}` WHERE id = %s",
                (int(body.id),),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="创建失败")
        return CodeMappingOut(**_row_code_mapping_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/config/code-mapping/{item_id}", response_model=CodeMappingOut)
async def update_code_mapping(item_id: int, body: CodeMappingUpdate, request: Request) -> CodeMappingOut:
    _reject_if_readonly(request)
    if body.code_value is None and body.description is None and body.stat_cost is None and body.channel_name is None:
        raise HTTPException(status_code=400, detail="至少提供一个需要更新的字段")
    try:
        fields = []
        params: List[Any] = []
        if body.code_value is not None:
            fields.append("code_value = %s")
            params.append(body.code_value)
        if body.description is not None:
            fields.append("description = %s")
            params.append(body.description)
        if body.stat_cost is not None:
            fields.append("stat_cost = %s")
            params.append(body.stat_cost)
        if body.channel_name is not None:
            fields.append("channel_name = %s")
            params.append(body.channel_name)

        with _db_cursor() as cur:
            sql = f"UPDATE `{CONFIG_CODE_MAPPING_TABLE}` SET {', '.join(fields)} WHERE id = %s"
            params.append(int(item_id))
            cur.execute(sql, tuple(params))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="记录不存在")
            cur.execute(
                f"SELECT id, code_value, description, stat_cost, channel_name, created_time "
                f"FROM `{CONFIG_CODE_MAPPING_TABLE}` WHERE id = %s",
                (int(item_id),),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="记录不存在")
        return CodeMappingOut(**_row_code_mapping_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/config/code-mapping/{item_id}")
async def delete_code_mapping(item_id: int, request: Request) -> Dict[str, Any]:
    _reject_if_readonly(request)
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"DELETE FROM `{CONFIG_CODE_MAPPING_TABLE}` WHERE id = %s",
                (int(item_id),),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="记录不存在")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------- API：股票仓位/买卖配置表 --------------------

@app.get("/api/config/stock-position/products", response_model=List[str])
async def list_stock_position_products() -> List[str]:
    """返回所有不重复的产品名称，用于筛选下拉。"""
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT product_name FROM `{CONFIG_STOCK_POSITION_TABLE}` "
                f"WHERE product_name IS NOT NULL AND product_name != '' ORDER BY product_name"
            )
            return [r["product_name"] for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/config/stock-position", response_model=Dict[str, Any])
async def list_stock_position(
    product_names: Optional[str] = None,
    page: int = 1,
    page_size: int = 30,
) -> Dict[str, Any]:
    """
    按产品名称筛选，按日期倒序，分页。
    product_names 为单个产品名称；不传时默认展示产品名为「短线王」的数据。
    """
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 500:
        page_size = 30
    if product_names is None or not product_names.strip():
        names = ["短线王"]
    else:
        first = product_names.split(",")[0].strip()
        names = [first] if first else ["短线王"]
    if not names:
        return {"total": 0, "items": [], "page": page, "page_size": page_size}

    try:
        with _db_cursor() as cur:
            placeholders = ", ".join(["%s"] * len(names))
            count_sql = (
                f"SELECT COUNT(*) AS cnt FROM `{CONFIG_STOCK_POSITION_TABLE}` "
                f"WHERE product_name IN ({placeholders})"
            )
            cur.execute(count_sql, tuple(names))
            total = (cur.fetchone() or {}).get("cnt") or 0

            offset = (page - 1) * page_size
            list_sql = (
                f"SELECT id, product_name, trade_date, stock_code, stock_name, "
                f"position_pct, side, price, created_at, updated_at "
                f"FROM `{CONFIG_STOCK_POSITION_TABLE}` "
                f"WHERE product_name IN ({placeholders}) "
                f"ORDER BY trade_date desc, created_at DESC, id DESC LIMIT %s OFFSET %s"
            )
            cur.execute(list_sql, tuple(names) + (page_size, offset))
            rows = cur.fetchall()

        items = [StockPositionOut(**_row_stock_position_fix(r)) for r in rows]
        return {"total": total, "items": items, "page": page, "page_size": page_size}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/config/stock-position/export", response_model=List[StockPositionOut])
async def export_stock_position(
    product_name: Optional[str] = None,
) -> List[StockPositionOut]:
    """
    导出指定产品名称的全部股票仓位/买卖记录（不分页）。
    """
    if product_name is None or not product_name.strip():
        first = "短线王"
    else:
        first = product_name.split(",")[0].strip() or "短线王"

    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, product_name, trade_date, stock_code, stock_name, "
                f"position_pct, side, price, created_at, updated_at "
                f"FROM `{CONFIG_STOCK_POSITION_TABLE}` "
                f"WHERE product_name = %s "
                f"ORDER BY created_at DESC, id DESC",
                (first,),
            )
            rows = cur.fetchall()
        return [StockPositionOut(**_row_stock_position_fix(r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/config/stock-position/export.xlsx")
async def export_stock_position_excel(
    product_name: Optional[str] = None,
):
    """
    下载 Excel（两个 sheet）：
    - Sheet1: 当前筛选产品下的仓位明细（全量，不分页）
    - Sheet2: 该产品的净值序列（product_nav_daily_detail row_type=3）
    """
    name = (product_name or "").strip() or "短线王"

    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, product_name, trade_date, stock_code, stock_name, "
                f"position_pct, side, price, created_at, updated_at "
                f"FROM `{CONFIG_STOCK_POSITION_TABLE}` "
                f"WHERE product_name = %s "
                f"ORDER BY created_at DESC, id DESC",
                (name,),
            )
            rows1 = cur.fetchall()

            cur.execute(
                f"SELECT biz_date, nav, hs300_nav "
                f"FROM `{PRODUCT_NAV_DAILY_DETAIL_TABLE}` "
                f"WHERE product_name = %s AND row_type = 3 "
                f"ORDER BY biz_date ASC",
                (name,),
            )
            rows2 = cur.fetchall()

        from openpyxl import Workbook

        wb = Workbook()

        # Sheet1: 当前页仓位明细
        ws1 = wb.active
        ws1.title = "仓位明细"
        ws1.append(["ID", "产品名称", "日期", "股票代码", "个股", "仓位(%)", "买入/卖出", "成交价", "创建时间", "更新时间"])
        for r in rows1:
            ws1.append(
                [
                    r.get("id"),
                    r.get("product_name"),
                    _parse_date(r.get("trade_date")),
                    r.get("stock_code"),
                    r.get("stock_name"),
                    float(r.get("position_pct") or 0) if r.get("position_pct") is not None else None,
                    r.get("side"),
                    float(r.get("price") or 0) if r.get("price") is not None else None,
                    _fmt_dt(r.get("created_at")),
                    _fmt_dt(r.get("updated_at")),
                ]
            )

        # Sheet2: 净值
        ws2 = wb.create_sheet("净值")
        ws2.append(["产品名称", "日期", "组合净值", "沪深300净值"])
        for r in rows2:
            ws2.append(
                [
                    name,
                    _parse_date(r.get("biz_date")),
                    float(r.get("nav") or 0) if r.get("nav") is not None else None,
                    float(r.get("hs300_nav") or 0) if r.get("hs300_nav") is not None else None,
                ]
            )

        bio = io.BytesIO()
        wb.save(bio)
        bio.seek(0)

        fname = f"{name}_仓位+净值.xlsx"
        headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{quote(fname)}"}
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 净值图：计算参数与 temp3.py 一致
NAV_INIT_CAPITAL = 10_000_000  # 初始资金 1000万
NAV_TRADE_UNIT = 100           # A股交易单位（股）
HSGT_PRICE_TABLE = "portal_db.hsgt_price_deliver"  # 收盘价表，biz_date=yyyymmdd, stock_code, last_price
PRODUCT_NAV_DAILY_TABLE = "product_nav_daily"     # 净值结果表，由定时任务按同口径写入，接口优先查此表
# 净值数据来源：product_nav_daily（默认）或 mv_product_nav_compute（物化视图直接计算，含沪深300）
NAV_SOURCE_TABLE = os.environ.get("NAV_SOURCE_TABLE", "product_nav_daily")
PRODUCT_NAV_DAILY_DETAIL_TABLE = "product_nav_daily_detail"  # 净值明细表，row_type=3 为当日汇总行


def _parse_date(v: Any) -> Optional[str]:
    """归一化为 YYYY-MM-DD。"""
    if v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if len(s) >= 8 and s.isdigit():
        return s[:4] + "-" + s[4:6] + "-" + s[6:8]
    if "-" in s:
        return s[:10]
    return None


def _compute_nav_series(product_name: str, cur) -> List[Dict[str, Any]]:
    """
    按 temp3 口径计算组合净值序列。
    交易来自 config_stock_position，收盘价来自 hsgt_price_deliver（biz_date=yyyymmdd, stock_code, last_price）。
    """
    cur.execute(
        f"SELECT trade_date, stock_code, side, position_pct, price "
        f"FROM `{CONFIG_STOCK_POSITION_TABLE}` "
        f"WHERE product_name = %s AND trade_date IS NOT NULL AND stock_code IS NOT NULL AND side IS NOT NULL "
        f"ORDER BY trade_date ASC",
        (product_name,),
    )
    trades = cur.fetchall()
    if not trades:
        return []

    # 日期范围
    dates_from_trades = set()
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            dates_from_trades.add(d)

    if not dates_from_trades:
        return []
    min_d, max_d = min(dates_from_trades), max(dates_from_trades)

    # 从 hsgt_price_deliver 拉取区间内收盘价（biz_date 可能是 yyyymmdd 或 date 类型）
    cur.execute(
        f"SELECT biz_date, stock_code, last_price FROM `{HSGT_PRICE_TABLE}` "
        f"WHERE biz_date IS NOT NULL AND last_price IS NOT NULL"
    )
    price_rows = cur.fetchall()

    # 构建 (date_str, stock_code) -> price；日期统一为 YYYY-MM-DD
    price_map: Dict[tuple, float] = {}
    trade_dates_set = set()
    for r in price_rows:
        dt = _parse_date(r.get("biz_date"))
        if not dt:
            continue
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        try:
            pr = float(r.get("last_price"))
        except (TypeError, ValueError):
            continue
        price_map[(dt, code)] = pr
        trade_dates_set.add(dt)

    # 交易日历：交易日期与有价格数据的日期的并集，且在 [min_d, max_d] 内
    all_dates = dates_from_trades | trade_dates_set
    sorted_dates = sorted(d for d in all_dates if min_d <= d <= max_d)
    if not sorted_dates:
        return []

    # 按日期分组的交易
    trade_by_date: Dict[str, list] = {}
    for r in trades:
        d = _parse_date(r.get("trade_date"))
        if d:
            trade_by_date.setdefault(d, []).append(r)

    def _get_price(date_str: str, code: str) -> float:
        v = price_map.get((date_str, code))
        if v is not None:
            return v
        # 尝试 SZ/SH 前缀对齐
        if code.upper().startswith(("SH", "SZ")) and len(code) > 2:
            v = price_map.get((date_str, code[2:]))
            if v is not None:
                return v
        if len(code) == 6 or len(code) <= 6:
            for p in ("SH", "SZ"):
                v = price_map.get((date_str, p + code))
                if v is not None:
                    return v
        return 0.0

    cash = float(NAV_INIT_CAPITAL)
    positions: Dict[str, int] = {}
    result: List[Dict[str, Any]] = []

    for date_str in sorted_dates:
        day_trades = trade_by_date.get(date_str, [])
        for row in day_trades:
            code = (row.get("stock_code") or "").strip()
            side = (row.get("side") or "").strip()
            try:
                pct = float(row.get("position_pct") or 0)
                price = float(row.get("price") or 0)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            target_amount = NAV_INIT_CAPITAL * (pct / 100.0)

            if side == "买入":
                shares = int(target_amount / price / NAV_TRADE_UNIT) * NAV_TRADE_UNIT
                if shares <= 0:
                    continue
                cost = shares * price
                if cash < cost:
                    shares = int(cash / price / NAV_TRADE_UNIT) * NAV_TRADE_UNIT
                    if shares <= 0:
                        continue
                    cost = shares * price
                cash -= cost
                positions[code] = positions.get(code, 0) + shares
            elif side == "卖出":
                if code not in positions or positions[code] <= 0:
                    continue
                shares = int(target_amount / price / NAV_TRADE_UNIT) * NAV_TRADE_UNIT
                shares = min(shares, positions[code])
                if shares <= 0:
                    continue
                cash += shares * price
                positions[code] -= shares
                if positions[code] == 0:
                    del positions[code]

        market_value = 0.0
        for c, sh in positions.items():
            market_value += sh * _get_price(date_str, c)
        total_asset = cash + market_value
        nav = total_asset / float(NAV_INIT_CAPITAL)
        result.append({"date": date_str, "nav": round(nav, 6), "hs300_nav": None})

    return result


class NavChartPoint(BaseModel):
    date: str
    nav: float
    hs300_nav: Optional[float] = None


@app.get("/api/config/stock-position/nav-chart", response_model=List[NavChartPoint])
async def get_stock_position_nav_chart(
    product_name: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[NavChartPoint]:
    """
    返回指定产品的净值序列，用于前端绘制净值图。
    直接从 product_nav_daily_detail 中查询 row_type=3 的当日汇总行（便于对账，且无需实时计算）。

    - 默认返回最近 90 天（约 3 个月）
    - 可通过 start_date/end_date（YYYY-MM-DD）指定区间
    """
    name = (product_name or "").strip() or "短线王"
    try:
        with _db_cursor() as cur:
            # 默认区间：最近 90 天
            from datetime import date, timedelta
            d_to = end_date.strip() if end_date and end_date.strip() else date.today().strftime("%Y-%m-%d")
            if start_date and start_date.strip():
                d_from = start_date.strip()
            else:
                d_from = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")

            cur.execute(
                f"SELECT biz_date, nav, hs300_nav FROM `{PRODUCT_NAV_DAILY_DETAIL_TABLE}` "
                f"WHERE product_name = %s AND row_type = 3 "
                f"AND biz_date BETWEEN %s AND %s "
                f"ORDER BY biz_date ASC",
                (name, d_from, d_to),
            )
            rows = cur.fetchall()
        series = []
        for r in rows:
            dt = _parse_date(r.get("biz_date"))
            if not dt:
                continue
            try:
                nav_val = float(r.get("nav") or 0)
            except (TypeError, ValueError):
                continue
            hs = r.get("hs300_nav")
            try:
                hs300_val = float(hs) if hs is not None else None
            except (TypeError, ValueError):
                hs300_val = None
            series.append({"date": dt, "nav": round(nav_val, 6), "hs300_nav": hs300_val})
        return [NavChartPoint(**x) for x in series]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/config/stock-position/nav-export.xlsx")
async def export_stock_position_nav_excel(
    product_name: Optional[str] = None,
):
    """
    下载指定产品的净值（来自 product_nav_daily_detail row_type=3），全量导出为 Excel。
    """
    name = (product_name or "").strip() or "短线王"
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT biz_date, total_position_mv, total_cash, total_asset, nav, hs300_nav "
                f"FROM `{PRODUCT_NAV_DAILY_DETAIL_TABLE}` "
                f"WHERE product_name = %s AND row_type = 3 "
                f"ORDER BY biz_date ASC",
                (name,),
            )
            rows = cur.fetchall()

        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "净值"
        ws.append(["日期", "持仓市值", "现金", "总资产", "组合净值", "沪深300净值"])

        for r in rows:
            dt = _parse_date(r.get("biz_date")) or ""
            ws.append(
                [
                    dt,
                    float(r.get("total_position_mv") or 0),
                    float(r.get("total_cash") or 0),
                    float(r.get("total_asset") or 0),
                    float(r.get("nav") or 0),
                    float(r.get("hs300_nav") or 0) if r.get("hs300_nav") is not None else None,
                ]
            )

        bio = io.BytesIO()
        wb.save(bio)
        bio.seek(0)

        fname = f"{name}_净值.xlsx"
        headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{quote(fname)}"}
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config/stock-position", response_model=StockPositionOut)
async def create_stock_position(body: StockPositionCreate, request: Request) -> StockPositionOut:
    _reject_if_readonly(request)
    """新增：股票代码、仓位、买入/卖出、成交价为必填；日期不填则默认当前日期。"""
    from datetime import date
    trade_date = body.trade_date
    if not trade_date or not trade_date.strip():
        trade_date = date.today().strftime("%Y-%m-%d")
    try:
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            # 优先走数据库自增 id：不传 id
            try:
                cur.execute(
                    f"INSERT INTO `{CONFIG_STOCK_POSITION_TABLE}` "
                    # StarRocks: AUTO_INCREMENT 列若在 sort key 上，必须显式写入（用 DEFAULT），否则会触发 partial update 报错
                    f"(id, product_name, trade_date, stock_code, stock_name, position_pct, side, price, created_at, updated_at) "
                    f"VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        (body.product_name or "").strip() or None,
                        trade_date.strip(),
                        (body.stock_code or "").strip(),
                        (body.stock_name or "").strip() or None,
                        body.position_pct,
                        (body.side or "").strip(),
                        body.price,
                        now_str,
                        now_str,
                    ),
                )
                new_id = cur.lastrowid
                if not new_id:
                    # 某些引擎/驱动可能拿不到 lastrowid，用本次插入的字段精确查刚插入的那条，避免取到旧行
                    cur.execute(
                        f"SELECT id FROM `{CONFIG_STOCK_POSITION_TABLE}` "
                        f"WHERE product_name <=> %s AND trade_date = %s AND stock_code = %s AND created_at = %s "
                        f"ORDER BY id DESC LIMIT 1",
                        (
                            (body.product_name or "").strip() or None,
                            trade_date.strip(),
                            (body.stock_code or "").strip(),
                            now_str,
                        ),
                    )
                    r = cur.fetchone() or {}
                    new_id = r.get("id")
            except Exception:
                # 若库不支持 AUTO_INCREMENT（或表仍为非自增），回退到后端生成 id
                next_id = _next_config_id(cur, CONFIG_STOCK_POSITION_TABLE)
                cur.execute(
                    f"INSERT INTO `{CONFIG_STOCK_POSITION_TABLE}` "
                    f"(id, product_name, trade_date, stock_code, stock_name, position_pct, side, price, created_at, updated_at) "
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        next_id,
                        (body.product_name or "").strip() or None,
                        trade_date.strip(),
                        (body.stock_code or "").strip(),
                        (body.stock_name or "").strip() or None,
                        body.position_pct,
                        (body.side or "").strip(),
                        body.price,
                        now_str,
                        now_str,
                    ),
                )
                new_id = next_id

            cur.execute(
                f"SELECT id, product_name, trade_date, stock_code, stock_name, "
                f"position_pct, side, price, created_at, updated_at "
                f"FROM `{CONFIG_STOCK_POSITION_TABLE}` WHERE id = %s",
                (int(new_id),),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=500, detail="创建失败")
        return StockPositionOut(**_row_stock_position_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/config/stock-position/{item_id}", response_model=StockPositionOut)
async def update_stock_position(item_id: int, body: StockPositionUpdate, request: Request) -> StockPositionOut:
    _reject_if_readonly(request)
    """全字段可选更新；未传的字段保持原值。"""
    try:
        from datetime import date
        now_str = datetime.now().strftime(_DT_FMT)
        with _db_cursor() as cur:
            cur.execute(
                f"SELECT id, product_name, trade_date, stock_code, stock_name, "
                f"position_pct, side, price, created_at, updated_at "
                f"FROM `{CONFIG_STOCK_POSITION_TABLE}` WHERE id = %s",
                (item_id,),
            )
            old = cur.fetchone()
            if not old:
                raise HTTPException(status_code=404, detail="记录不存在")

            def _v(key: str, new_val: Any, default: Any = None):
                if new_val is not None:
                    return new_val
                return old.get(key) if old else default

            trade_date = _v("trade_date", body.trade_date)
            if trade_date and isinstance(trade_date, str) and not trade_date.strip():
                trade_date = date.today().strftime("%Y-%m-%d")

            new_product = _v("product_name", body.product_name)
            new_product = (new_product or "").strip() or None
            new_code = (_v("stock_code", body.stock_code) or "").strip()
            new_name = (_v("stock_name", body.stock_name) or "").strip() or None
            new_pct = _v("position_pct", body.position_pct)
            new_side = (_v("side", body.side) or "").strip()
            new_price = _v("price", body.price)
            created_at = _fmt_dt(old.get("created_at")) or now_str

            cur.execute(
                f"DELETE FROM `{CONFIG_STOCK_POSITION_TABLE}` WHERE id = %s",
                (item_id,),
            )
            cur.execute(
                f"INSERT INTO `{CONFIG_STOCK_POSITION_TABLE}` "
                f"(id, product_name, trade_date, stock_code, stock_name, position_pct, side, price, created_at, updated_at) "
                f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    item_id,
                    new_product,
                    trade_date or date.today().strftime("%Y-%m-%d"),
                    new_code,
                    new_name,
                    new_pct,
                    new_side,
                    new_price,
                    created_at,
                    now_str,
                ),
            )
            cur.execute(
                f"SELECT id, product_name, trade_date, stock_code, stock_name, "
                f"position_pct, side, price, created_at, updated_at "
                f"FROM `{CONFIG_STOCK_POSITION_TABLE}` WHERE id = %s",
                (item_id,),
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="记录不存在")
        return StockPositionOut(**_row_stock_position_fix(row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/config/stock-position/{item_id}")
async def delete_stock_position(item_id: int, request: Request) -> Dict[str, Any]:
    _reject_if_readonly(request)
    try:
        with _db_cursor() as cur:
            cur.execute(
                f"DELETE FROM `{CONFIG_STOCK_POSITION_TABLE}` WHERE id = %s",
                (item_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="记录不存在")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------- API：沪深港通价格推送（portal_db.hsgt_price_deliver） --------------------

class HsgtPriceDeliverOut(BaseModel):
    biz_date: Optional[str] = None  # YYYY-MM-DD
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    last_price: Optional[float] = None


@app.get("/api/hsgt-price-deliver", response_model=List[HsgtPriceDeliverOut])
async def get_hsgt_price_deliver(
    biz_date: Optional[str] = None,
) -> List[HsgtPriceDeliverOut]:
    """
    查询 StarRocks 中 portal_db.hsgt_price_deliver 表。

    - 若传入 biz_date（格式如 '2026-02-11'），则按该日期筛选；
    - 若不传 biz_date，则返回表中全部数据。
    """
    try:
        with _db_cursor() as cur:
            sql = (
                "SELECT biz_date, stock_code, stock_name, last_price "
                "FROM portal_db.hsgt_price_deliver where biz_date is not null"
            )
            params: List[Any] = []
            if biz_date:
                sql += " and biz_date = %s"
                params.append(biz_date)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        # rows 已是 DictCursor 结果，字段名与模型一致，直接解包
        return [HsgtPriceDeliverOut(**row) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if FRONTEND_DIST.exists():
    _assets_dir = FRONTEND_DIST / "assets"

    @app.get("/assets/{file_path:path}")
    async def serve_assets(file_path: str):
        """显式路由优先于 catch-all；带 hash 的 JS/CSS 强缓存，来回切换时从缓存加载"""
        if ".." in file_path or file_path.startswith("/"):
            raise HTTPException(status_code=404, detail="Not Found")
        full = _assets_dir / file_path
        if not full.is_file():
            raise HTTPException(status_code=404, detail="Not Found")
        # 文件名带 hash（如 index-Bs3PAl4u.js），可长期强缓存；来回切换业务模块时不再重复下载
        headers = {"Cache-Control": "public, max-age=31536000, immutable"}
        return FileResponse(full, headers=headers)

    def _index_html(base_path: Optional[str] = None):
        """base_path: 门户代理时传 /sr_api，使静态资源与 API 请求走同源"""
        path = (base_path or MOUNT_PATH or "").strip("/")
        mount = ("/" + path) if path else ""
        html = (FRONTEND_DIST / "index.html").read_text(encoding="utf-8")
        if mount:
            html = html.replace('src="/assets/', 'src="' + mount + '/assets/').replace('href="/assets/', 'href="' + mount + '/assets/')
            if mount != "/sr_api":
                html = html.replace('src="/sr_api/', 'src="' + mount + '/').replace('href="/sr_api/', 'href="' + mount + '/')
            inject = '<script>window.__API_BASE__="' + mount + '";</script>'
            html = html.replace("<head>", "<head>" + inject, 1) if "<head>" in html else html.replace("<body>", "<body>" + inject, 1)
        return html

    _no_cache_headers = {"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"}

    @app.get("/")
    async def index(req: Request):
        prefix = (req.headers.get("X-Forwarded-Prefix") or "").strip()
        return HTMLResponse(_index_html(prefix or None), headers=_no_cache_headers)
    @app.get("/{path:path}")
    async def serve_spa(req: Request, path: str):
        if path.strip("/").startswith("assets"):
            raise HTTPException(status_code=404, detail="Not Found")
        prefix = (req.headers.get("X-Forwarded-Prefix") or "").strip()
        return HTMLResponse(_index_html(prefix or None), headers=_no_cache_headers)
else:
    @app.get("/")
    async def index():
        return {"status": "ok", "tip": "请先执行 cd business/frontend && VITE_BASE=/business/realtime_mv/ npm run build 构建前端"}

if __name__ == "__main__":
    port = int(os.environ.get("BUSINESS_PORT", "5003"))
    import uvicorn
    print("业务应用（端口 %s）: http://127.0.0.1:%s" % (port, port))
    uvicorn.run(app, host="0.0.0.0", port=port)
