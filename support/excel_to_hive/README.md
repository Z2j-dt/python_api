# Excel 入库 Hive（tmp 库）

在 `support` 下（仅 app.py 单入口）：上传 Excel，在 **tmp** 库建 Hive 表，转 UTF-8 CSV 上传 HDFS，并执行 Impala 刷新。

## 流程

1. **按 Excel 字段在 tmp 库建表**：根据列名与类型推断生成 Hive 外部表 DDL，建在 `tmp` 库。
2. **检测表头**：自动判断第一行是否为表头（启发式：第一行多为字符串、与第二行类型分布不同则视为表头）。
3. **转 CSV**：将 Excel 数据转为 CSV，UTF-8 编码。
4. **上传 HDFS**：使用 `hdfs dfs -put` 将 CSV 上传到配置的 HDFS 路径。
5. **Impala 刷新**：执行 `REFRESH tmp.表名`，使 Impala 识别新数据。

## 环境要求

- 运行本服务的机器需能执行 **hdfs** 命令（Hadoop 客户端），且能访问 HDFS。
- 需能连接 **Impala**（建库、建表、刷新）。
- Python 依赖见根目录 `requirements.txt`（pandas、openpyxl、xlrd、impyla）。

## 配置（环境变量）

| 变量 | 说明 | 默认 |
|------|------|------|
| `EXCEL_TO_HIVE_TMP_DB` | Hive/Impala 使用的库名 | `tmp` |
| `IMPALA_HOST` | Impala 主机 | `127.0.0.1` |
| `IMPALA_PORT` | Impala 端口 | `21050` |
| `EXCEL_TO_HIVE_HDFS_PATH` | HDFS 根路径（表数据目录在其下） | `/user/hive/tmp_excel` |
| `EXCEL_TO_HIVE_UPLOAD_DIR` | 本地上传临时目录 | `/tmp/excel_to_hive_uploads` |

## 门户集成

- 在统一门户中挂载为 **Excel 入库 Hive**，路径：`/support/excel_to_hive/`。
- 在 `portal/users.json` 中为用户配置模块 `excel_to_hive` 即可在左侧看到入口。

## 单独运行

```bash
# 在项目根目录、已激活 venv 时
python support/app.py
# 默认端口 5002，访问 http://127.0.0.1:5002/
```
