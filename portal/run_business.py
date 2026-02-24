# -*- coding: utf-8 -*-
"""
业务应用模块 - 单端口 5003。实际逻辑在 business/app.py，此处仅为启动入口。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    import runpy
    runpy.run_path(str(ROOT / "business" / "app.py"), run_name="__main__")
