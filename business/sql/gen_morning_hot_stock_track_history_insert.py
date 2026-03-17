import sys
from pathlib import Path

import pandas as pd


def _excel_serial_to_date_str(v: int) -> str:
    origin = pd.Timestamp("1899-12-30")
    return str((origin + pd.to_timedelta(int(v), unit="D")).date())


def _esc_sql_str(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace("'", "''")


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    excel_path = repo_root / "早盘人气股.xlsx"
    out_sql = repo_root / "business" / "sql" / "config_morning_hot_stock_track_history_insert.sql"

    tg_name = "胡晶翔"

    if not excel_path.is_file():
        print(f"Excel 文件不存在: {excel_path}", file=sys.stderr)
        return 2

    df = pd.read_excel(excel_path)
    if df.shape[1] < 3:
        print("Excel 至少需要 3 列：时间、人气股、代码", file=sys.stderr)
        return 3

    c_date, c_name, c_code = df.columns[:3]

    values = []
    for d, n, c in zip(df[c_date], df[c_name], df[c_code]):
        if pd.isna(c) or str(c).strip() == "":
            continue
        biz_date = _excel_serial_to_date_str(d)
        stock_name = "" if pd.isna(n) else str(n).replace("\xa0", " ").strip()
        stock_code = str(c).strip()
        dt = biz_date + " 09:30:00"

        values.append(
            "(DEFAULT,'%s','%s','%s','%s',NULL,'%s','%s')"
            % (
                _esc_sql_str(tg_name),
                _esc_sql_str(biz_date),
                _esc_sql_str(stock_name),
                _esc_sql_str(stock_code),
                _esc_sql_str(dt),
                _esc_sql_str(dt),
            )
        )

    content = "\n".join(
        [
            "-- 历史数据初始化：来自 早盘人气股.xlsx",
            f"-- tg_name 固定为：{tg_name}",
            "INSERT INTO config_morning_hot_stock_track (id, tg_name, biz_date, stock_name, stock_code, remark, created_at, updated_at) VALUES",
            ",\n".join(values) + ";",
            "",
        ]
    )
    out_sql.write_text(content, encoding="utf-8")
    print(f"Wrote: {out_sql} (rows={len(values)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

