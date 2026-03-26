from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd


REQUIRED_CN_COLUMNS = {
    "客户姓名": "customer_name",
    "资金账号": "customer_account",
    "产品名称": "product_name",
    "进线月份": "in_month",
    "渠道": "channel",
    "销售归属": "sales_owner",
    "微信昵称": "wechat_nick",
}

# Terminal/encoding mojibake aliases observed on some Windows environments.
ALIAS_COLUMNS = {
    "�ͻ�����": "客户姓名",
    "�ʽ��˺�": "资金账号",
    "�������": "产品名称",
    "�����·�": "进线月份",
    "����": "渠道",
    "���۹���": "销售归属",
    "΢���ǳ�": "微信昵称",
}


def _norm_col(s: str) -> str:
    return str(s).strip().replace(" ", "")


def _canonical_col(col: str) -> str:
    c = _norm_col(col)
    # Direct alias mapping
    if c in ALIAS_COLUMNS:
        return ALIAS_COLUMNS[c]
    return c


def has_required_columns(path: Path) -> bool:
    try:
        cols = list(pd.read_excel(path, nrows=0).columns)
    except Exception:  # noqa: BLE001
        return False
    cols = {_canonical_col(c) for c in cols}
    if all(c in cols for c in REQUIRED_CN_COLUMNS):
        return True
    # Fallback for mojibake headers: expected 7 business columns in fixed order.
    return len(cols) >= 7


def find_default_excel(project_root: Path) -> Path | None:
    """Find xlsx containing required columns in project root."""
    candidates = sorted(project_root.glob("*.xlsx"))
    for p in candidates:
        if has_required_columns(p):
            return p
    return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Fix known mojibake headers first.
    rename_alias = {}
    for c in df.columns:
        canon = _canonical_col(c)
        if canon != c:
            rename_alias[c] = canon
    if rename_alias:
        df = df.rename(columns=rename_alias)

    missing = [c for c in REQUIRED_CN_COLUMNS if c not in df.columns]
    if missing:
        # Fallback by column order when headers are mojibake/garbled.
        if len(df.columns) >= 7:
            df = df.iloc[:, :7].copy()
            df.columns = list(REQUIRED_CN_COLUMNS.keys())
            missing = []
        else:
            raise ValueError(f"Excel 缺少必要列: {missing}")

    out = df[list(REQUIRED_CN_COLUMNS.keys())].rename(columns=REQUIRED_CN_COLUMNS)
    # Keep string consistency for key columns.
    for c in ("customer_account", "product_name"):
        out[c] = out[c].astype(str).str.strip()
    out["customer_name"] = out["customer_name"].astype(str).str.strip()
    return out


def format_in_month_display(v: object) -> str:
    """Convert Excel serial/date-like values to 'YYYY年M月' display text."""
    if pd.isna(v):
        return ""
    try:
        # Excel serial number
        if isinstance(v, (int, float)):
            dt = pd.to_datetime(v, unit="D", origin="1899-12-30")
            return f"{dt.year}年{dt.month}月"
        s = str(v).strip()
        if not s:
            return ""
        # "45962.0" style text
        try:
            fv = float(s)
            dt = pd.to_datetime(fv, unit="D", origin="1899-12-30")
            return f"{dt.year}年{dt.month}月"
        except Exception:  # noqa: BLE001
            pass
        dt = pd.to_datetime(s)
        return f"{dt.year}年{dt.month}月"
    except Exception:  # noqa: BLE001
        return str(v)


def main() -> int:
    parser = argparse.ArgumentParser(description="将 Excel 手工配置转换为 StarRocks 导入 CSV")
    parser.add_argument("--input", type=str, default="", help="Excel 文件路径（默认自动查找）")
    parser.add_argument(
        "--output",
        type=str,
        default="branch_customer_ext_input.csv",
        help="输出 CSV 文件路径",
    )
    parser.add_argument("--sheet", type=str, default="", help="sheet 名称，默认第一个")
    parser.add_argument(
        "--in-month-display",
        action="store_true",
        help="将 in_month 导出为 YYYY年M月（默认保持 Excel 原始值）",
    )
    args = parser.parse_args()

    project_root = Path.cwd()
    input_path = Path(args.input) if args.input else find_default_excel(project_root)
    if input_path is None or not input_path.exists():
        print("未找到 Excel 文件，请通过 --input 指定路径", file=sys.stderr)
        return 1

    try:
        df = pd.read_excel(input_path, sheet_name=args.sheet or 0)
        out = normalize_columns(df)
        if args.in_month_display:
            out["in_month"] = out["in_month"].apply(format_in_month_display)
        output_path = Path(args.output)
        out.to_csv(output_path, index=False, encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        print(f"转换失败: {exc}", file=sys.stderr)
        return 1

    print(f"转换完成: {input_path} -> {output_path}")
    print(f"导出列: {list(out.columns)}")
    print(f"记录数: {len(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
