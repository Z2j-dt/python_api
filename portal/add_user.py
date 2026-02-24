# -*- coding: utf-8 -*-
"""
在项目根目录执行: python portal/add_user.py
按提示输入用户名、密码、权限，会输出一段配置，复制到 portal/auth_config.py 的 AUTH_USERS 里即可。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def main():
    from werkzeug.security import generate_password_hash

    print("--- 添加门户登录账号 ---\n")
    username = input("用户名（英文，例如 zhangsan）: ").strip()
    if not username:
        print("用户名不能为空")
        return
    password = input("密码: ").strip()
    if not password:
        print("密码不能为空")
        return
    print("\n权限模块（可多选，用逗号隔开）：")
    print("  1 = 数据字典  2 = 数据血缘  3 = 业务场景  0 = 全部")
    choice = input("输入 0 或 1,2,3 等（例如 1,2 表示只有数据字典+数据血缘）: ").strip()
    if choice == "0" or choice == "":
        modules = ["hive_metadata", "sql_lineage", "sr_api"]
    else:
        mapping = {"1": "hive_metadata", "2": "sql_lineage", "3": "sr_api"}
        modules = []
        for x in choice.replace("，", ",").split(","):
            x = x.strip()
            if x in mapping and mapping[x] not in modules:
                modules.append(mapping[x])
        if not modules:
            modules = ["hive_metadata", "sql_lineage", "sr_api"]
            print("未识别，已设为全部模块")

    ph = generate_password_hash(password)
    print("\n请把下面这段复制到 portal/auth_config.py 的 AUTH_USERS 里（注意逗号）：\n")
    print('    "%s": {' % username)
    print('        "password_hash": "%s",' % ph)
    print('        "modules": %s,' % modules)
    print("    },")
    print()

if __name__ == "__main__":
    main()
