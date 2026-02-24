# -*- coding: utf-8 -*-
# 复制为 auth_config.py 并修改。auth_config.py 不要提交到版本库。
#
# 生成密码哈希（在项目根目录、激活 venv 后执行）:
#   python -c "from werkzeug.security import generate_password_hash; print(generate_password_hash('你的密码'))"
#
# 开发环境可不用本文件，直接设环境变量：PORTAL_ADMIN_PASSWORD=你的密码，则 admin 用该密码登录、拥有全部模块。

# 账号与权限：用户名 -> 密码哈希、可访问的模块列表
# 模块 id：hive_metadata=数据字典, sql_lineage=数据血缘, sr_api=业务场景
AUTH_USERS = {
    "admin": {
        "password_hash": "scrypt:32768:8:1$xxx$yyy",  # 用上面命令生成后替换
        "modules": ["hive_metadata", "sql_lineage", "sr_api"],
    },
    "viewer": {
        "password_hash": "scrypt:32768:8:1$xxx$yyy",
        "modules": ["hive_metadata", "sql_lineage"],
    },
}
