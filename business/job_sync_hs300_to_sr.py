# -*- coding: utf-8 -*-
"""
每天收盘后从新浪接口获取沪深300(000300) 当前价格，
写入 StarRocks 表 hsgt_price_deliver（一条记录，按 biz_date=yyyymmdd 区分）。
"""

import sys
from datetime import date
import requests
import pymysql

# ===== StarRocks 连接配置：按你实际情况修改 =====
SR_HOST = "10.8.93.40"
SR_PORT = 9030
SR_USER = "root"
SR_PASSWORD = "star@dt1988"
SR_DB = "portal_db"

PRICE_TABLE = "hsgt_price_deliver"  # 价格表
HS300_CODE = "000300"               # 沪深300 代码（纯数字）

# 新浪沪深300简要行情接口
SINA_HS300_URL = "https://hq.sinajs.cn/list=s_sh000300"
SINA_HEADERS = {
    "Referer": "https://finance.sina.com.cn",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    ),
}


def get_conn():
    return pymysql.connect(
        host=SR_HOST,
        port=SR_PORT,
        user=SR_USER,
        password=SR_PASSWORD,
        database=SR_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def fetch_hs300_from_sina():
    """
    从新浪接口获取沪深300当前价格。

    接口返回示例：
      var hq_str_s_sh000300="沪深300,4734.40,53.39,1.14,210676,290532";
    解析后：
      name = "沪深300"
      price = 4734.40
    """
    resp = requests.get(SINA_HS300_URL, headers=SINA_HEADERS, timeout=5)
    resp.raise_for_status()
    text = resp.text.strip()

    # 形如：var hq_str_s_sh000300="沪深300,4734.40,53.39,1.14,210676,290532";
    if "=" not in text or "\"" not in text:
        raise RuntimeError(f"新浪返回格式异常: {text}")

    inner = text.split("=", 1)[1].strip().strip(";").strip("\"")
    parts = inner.split(",")
    if len(parts) < 2:
        raise RuntimeError(f"解析字段失败: {inner}")

    name = parts[0]
    price = float(parts[1])  # 当前指数点位
    return name, price


def insert_hs300_to_sr(biz_date_yyyymmdd: str, name: str, price: float):
    """
    向 StarRocks 插入一条沪深300记录：
      biz_date: '20260302'
      stock_code: '000300'
      stock_name: 如 '沪深300'
      last_price: 收盘价/当前价
    """
    sql = f"""
        INSERT INTO {PRICE_TABLE} (biz_date, stock_code, stock_name, last_price)
        VALUES (%s, %s, %s, %s)
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (biz_date_yyyymmdd, HS300_CODE, name, price))
        conn.commit()

    print(
        f"已写入 StarRocks: biz_date={biz_date_yyyymmdd}, "
        f"code={HS300_CODE}, name={name}, price={price}"
    )


def main():
    # 今天的日期，格式 yyyymmdd（例如 20260302）
    today = date.today()
    biz_date = today.strftime("%Y%m%d")

    print(f"开始同步 {biz_date} 的沪深300价格（新浪接口）...")
    try:
        name, price = fetch_hs300_from_sina()
        print(f"从新浪获取到: {name} 当前价格: {price}")
        insert_hs300_to_sr(biz_date, name, price)
    except Exception as e:
        print(f"[ERROR] 同步失败: {e}", file=sys.stderr)
        sys.exit(1)

    print("任务完成。")


if __name__ == "__main__":
    main()