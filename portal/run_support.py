# -*- coding: utf-8 -*-
"""
技术支持模块 - 单端口 5002。实际逻辑在 support/app.py，此处仅为启动入口。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    import runpy
    runpy.run_path(str(ROOT / "support" / "app.py"), run_name="__main__")
