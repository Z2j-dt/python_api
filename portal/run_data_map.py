# -*- coding: utf-8 -*-
"""
数据地图模块 - 单端口 5001。实际逻辑在 data_map/app.py，此处仅为启动入口。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 直接运行 data_map/app.py
if __name__ == "__main__":
    import runpy
    runpy.run_path(str(ROOT / "data_map" / "app.py"), run_name="__main__")
