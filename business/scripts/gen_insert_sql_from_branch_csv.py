from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys


def sql_quote(v: str | None) -> str:
    if v is None:
        return "NULL"
    v = v.strip()
    if v == "":
        return "NULL"
    # StarRocks / MySQL 风格：字符串用单引号，内部单引号用两个单引号转义
    return "'" + v.replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate INSERT SQL from CSV for StarRocks staging table")
    parser.add_argument("--csv", type=str, default="branch_customer_ext_input.csv", help="CSV file path")
    parser.add_argument("--out", type=str, default="insert_stg_branch_customer_ext_excel.sql", help="Output SQL file")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per INSERT statement")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    out_path = Path(args.out)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    table = "ads.stg_branch_customer_ext_excel"
    columns = [
        "customer_name",
        "customer_account",
        "product_name",
        "in_month",
        "channel",
        "sales_owner",
        "wechat_nick",
    ]

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        missing = [c for c in columns if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"CSV missing columns: {missing}. header={reader.fieldnames}")

        rows = []
        insert_count = 0
        with out_path.open("w", encoding="utf-8") as out:
            out.write(f"-- Auto-generated from {csv_path.name}\n")
            out.write(f"TRUNCATE TABLE {table};\n\n")

            for i, row in enumerate(reader, start=1):
                values = [sql_quote(row.get(c, "")) for c in columns]
                rows.append("(" + ", ".join(values) + ")")
                if len(rows) >= args.batch_size:
                    insert_count += 1
                    out.write(
                        "INSERT INTO "
                        + table
                        + " ("
                        + ", ".join(columns)
                        + ")\nVALUES\n"
                        + ",\n".join(rows)
                        + ";\n\n"
                    )
                    rows = []

            if rows:
                insert_count += 1
                out.write(
                    "INSERT INTO "
                    + table
                    + " ("
                    + ", ".join(columns)
                    + ")\nVALUES\n"
                    + ",\n".join(rows)
                    + ";\n\n"
                )

        # 简单统计
        print(f"SQL generated: {out_path} (insert statements: {insert_count})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

