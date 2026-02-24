# -*- coding: utf-8 -*-
"""根据项目根 c2e.csv 生成 support/column_map.json"""
import csv
import json
from pathlib import Path

def has_chinese(s: str) -> bool:
    return any("\u4e00" <= ch <= "\u9fff" for ch in s)

def main():
    root = Path(__file__).resolve().parent.parent
    csv_path = root / "c2e.csv"
    if not csv_path.exists():
        print(f"未找到 {csv_path}，请确认 c2e.csv 放在项目根目录。")
        return
    mapping = {}
    for enc in ("utf-8-sig", "gbk", "utf-8"):
        try:
            with csv_path.open("r", encoding=enc) as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 2:
                        continue
                    c0, c1 = (row[0] or "").strip(), (row[1] or "").strip()
                    if not c0 and not c1:
                        continue
                    if has_chinese(c0) and not has_chinese(c1):
                        zh, en = c0, c1
                    elif has_chinese(c1) and not has_chinese(c0):
                        zh, en = c1, c0
                    else:
                        zh, en = c0, c1
                    if zh and en:
                        mapping[zh] = en
            break
        except UnicodeDecodeError:
            mapping.clear()
            continue
    out = Path(__file__).resolve().parent / "column_map.generated.json"
    out.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已生成 {out}，共 {len(mapping)} 条。确认无误后改名为 column_map.json")

if __name__ == "__main__":
    main()
