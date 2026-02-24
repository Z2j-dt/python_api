# -*- coding: utf-8 -*-
"""
业务应用 - 单入口 app.py，合并 sr_api（实时加微监测等）。
business 下仅 app.py，frontend、sql 为资产目录。
"""
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
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
async def create_open_channel_tag(body: OpenChannelTagCreate) -> OpenChannelTagOut:
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
async def update_open_channel_tag(item_id: int, body: OpenChannelTagUpdate) -> OpenChannelTagOut:
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
async def delete_open_channel_tag(item_id: int) -> Dict[str, Any]:
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
async def create_channel_staff(body: ChannelStaffCreate) -> ChannelStaffOut:
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
async def update_channel_staff(item_id: int, body: ChannelStaffUpdate) -> ChannelStaffOut:
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
async def delete_channel_staff(item_id: int) -> Dict[str, Any]:
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
async def create_code_mapping(body: CodeMappingCreate) -> CodeMappingOut:
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
async def update_code_mapping(item_id: int, body: CodeMappingUpdate) -> CodeMappingOut:
    if body.code_value is None and body.description is None and body.stat_cost is None and body.channel_name is None:
        raise HTTPException(status_code=400, detail="至少提供一个需要更新的字段")
    try:
        fields = []
        params: list[Any] = []
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
async def delete_code_mapping(item_id: int) -> Dict[str, Any]:
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


# -------------------- API：沪深港通价格推送（test_db.hsgt_price_deliver） --------------------


@app.get("/api/hsgt-price-deliver", response_model=List[HsgtPriceDeliverOut])
async def get_hsgt_price_deliver(
    biz_date: Optional[str] = None,
) -> List[HsgtPriceDeliverOut]:
    """
    查询 StarRocks 中 test_db.hsgt_price_deliver 表。

    - 若传入 biz_date（格式如 '2026-02-11'），则按该日期筛选；
    - 若不传 biz_date，则返回表中全部数据。
    """
    try:
        with _db_cursor() as cur:
            sql = (
                "SELECT biz_date, stock_code, stock_name, last_price "
                "FROM test_db.hsgt_price_deliver where biz_date is not null"
            )
            params: list[Any] = []
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
