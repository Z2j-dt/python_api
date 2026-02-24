# 数据平台 · 统一门户

一个项目内包含多个微服务，**一个页面**通过模块/目录切换展示不同能力：

| 模块            | 目录/服务         | 说明 |
|-----------------|-------------------|------|
| 数据字典        | `hive_metadata`   | Hive 元数据查询 |
| 数据血缘        | `sql_lineage_web` | SQL 血缘关系可视化 |
| 业务场景        | `sr_api`          | 给业务侧用的 StarRocks 物化视图等场景 |
| Excel 入库 Hive | `excel_to_hive`   | Excel 按字段在 tmp 库建表，转 CSV 上传 HDFS，Impala 刷新 |

入口：**统一门户** `portal` —— 单页左侧导航，点击「数据字典 / 数据血缘 / 业务场景」在右侧展示对应界面。

**两种部署方式**（任选其一）：

| 方式 | 说明 | 端口 |
|------|------|------|
| **单端口（默认）** | `python portal/run_all.py`，门户 + 所有子模块同一进程 | 仅 **5000** |
| **每模块一端口** | 门户 5000 + 数据地图 5001 + 技术支持 5002 + 业务应用 5003，便于生产端口管理 | **5000 / 5001 / 5002 / 5003** |

每模块一端口时，三大模块入口为：`portal/run_data_map.py`（数据字典+数据血缘）、`portal/run_support.py`（Excel 入库等）、`business/sr_api/backend`（业务应用）。门户需配置环境变量 `DATA_MAP_URL`、`SUPPORT_URL`、`BUSINESS_URL` 指向对应地址（见下文「每模块一端口部署」）。

---

## 项目结构

```
python_api/
├── venv/               # 整个项目共用的虚拟环境（在根目录创建）
├── requirements.txt    # 根目录依赖（门户 + 数据字典 + 数据血缘 + 业务场景）
├── portal/
│   ├── app.py
│   └── run_all.py      # 单端口 5000 启动入口
├── data_map/           # 数据地图（仅 app.py 单入口，含数据字典 + 数据血缘）
├── support/          # 技术支持（仅 app.py 单入口，含 Excel 入库 Hive）
├── business/         # 业务应用（仅 app.py 单入口，含 frontend、sql）
└── README.md
```

---

## 虚拟环境（整个项目共用）

**在项目根目录建一个虚拟环境**，所有子服务共用，便于部署和 Supervisor 只认一个 Python。

```bash
# 进入项目根目录
cd python_api

# 创建并激活虚拟环境
# Windows:
python -m venv venv
venv\Scripts\activate
# Linux/Mac:
python3 -m venv venv
source venv/bin/activate

# 一次性安装全部依赖（门户 + 数据字典 + 数据血缘 + 业务场景）
pip install -r requirements.txt
```

若要用「业务场景」的前端页面，还需在 `business/frontend` 里执行 `npm install`，然后**按部署方式**构建：
- **挂载在统一门户（推荐）**：`VITE_BASE=/business/realtime_mv/ npm run build`，这样静态资源和 API 会走 `/business/realtime_mv/`。
- 单独起 sr_api 后端时：直接 `npm run build` 即可。

**部署时不想传很大的 dist 目录**：只同步源码到服务器（可排除 `business/frontend/dist` 和 `business/frontend/node_modules`），在服务器上执行一次构建即可：
```bash
cd business/frontend
npm install
VITE_BASE=/business/realtime_mv/ npm run build
```
或直接执行 `bash build-on-server.sh`。这样只需传小体积的源码，构建在服务器完成，省时省带宽。

**服务器没有 Node/npm 时**：
- **方案 A（推荐）**：在服务器上装一次 Node（有 root 用 `sudo yum install nodejs npm` 或 `sudo dnf install nodejs npm`；无 root 可用 [nvm](https://github.com/nvm-sh/nvm) 装到用户目录），然后按上面在服务器构建，之后都不用传 dist。
- **方案 B**：在本地（有 Node 的电脑）构建并打成一个压缩包再上传，比直接传整个 dist 目录快：
  1. 本地：`cd business/frontend` → `VITE_BASE=/business/realtime_mv/ npm run build` → 在项目根执行 `tar -czvf business/frontend/dist-portal.tar.gz -C business/frontend dist`
  2. 上传到服务器
  3. 服务器：`cd /path/to/python_api/business/frontend && tar -xzvf dist-portal.tar.gz`
  也可用脚本：`bash business/frontend/pack-dist-for-upload.sh`

---

## 快速运行（只用 5000 一个端口）

1. **确认已建好根目录虚拟环境并安装依赖**（见上一节）。

2. **启动（只起一个进程）**

   激活虚拟环境后，在项目根目录执行：

   ```bash
   python portal/run_all.py
   ```

   或在已激活 venv 的情况下只进根目录再执行：

   ```bash
   cd python_api
   python portal/run_all.py
   ```

   门户、数据字典、数据血缘、业务场景都在同一进程、同一端口。

3. **访问**

   - 浏览器只访问：**http://127.0.0.1:5000**，会先进入**登录页**。
   - 登录后左侧按权限展示「数据字典」「数据血缘」「业务场景」，点击即可切换；右上角可退出。

---

## 登录与权限

- **默认开发**：未配置账号时，可设环境变量 `PORTAL_ADMIN_PASSWORD=你的密码`，用用户名 **admin** 登录，拥有全部模块。
- **简单配置（推荐）**：单独存一份账号密码文件，无需生成哈希。复制 `portal/users.json.example` 为 **`portal/users.json`**（不要提交到版本库），按 JSON 格式写用户名、明文密码、可访问模块即可，例如：
  ```json
  [
    {"username": "admin", "password": "admin123", "modules": ["hive_metadata", "sql_lineage", "sr_api"]},
    {"username": "zhangsan", "password": "123456", "modules": ["hive_metadata", "sql_lineage"]}
  ]
  ```
  模块 id：`hive_metadata`（数据字典）、`sql_lineage`（数据血缘）、`sr_api`（业务场景）、`excel_to_hive`（Excel 入库 Hive）。修改后需重启 portal 生效。**注意**：密码为明文，请勿把 `users.json` 提交到版本库或泄露。
- **生产/哈希配置**：若需密码哈希方式，复制 `portal/auth_config.example.py` 为 `portal/auth_config.py`，在 `AUTH_USERS` 中配置（会覆盖 users.json）。
- 未登录访问任意受保护路径会重定向到 `/login`；登录态为 Cookie，有效期默认 24 小时（可配 `PORTAL_AUTH_COOKIE_MAX_AGE`）。

---

## 配置（可选）

- 门户端口：环境变量 `PORTAL_PORT`，默认 `5000`。
- 业务场景与门户同进程挂载在 `/business/realtime_mv`（原 `/sr_api` 仍可代理），无需单独配后端地址；仅在「未安装 sr_api 依赖、改用单独进程」时，才用 `portal/config.py` 的 `SR_API_URL` 做代理。
- **单端口 5000 时的访问路径**：数据地图 `/data_map/hive_metadata/`、`/data_map/sql_lineage_web/`；技术支持 `/support/excel_to_hive/`；业务应用 `/business/realtime_mv/`。

---

## 每模块一端口部署（生产端口管理）

若希望**每个大模块只占一个端口**，便于防火墙/运维管理，可采用下表布局：

| 端口 | 模块     | 进程/入口                | 子项目说明           |
|------|----------|---------------------------|----------------------|
| 5000 | 门户     | `portal/run_all.py` 仅门户部分 或单独 `portal/app.py` | 登录、导航到三大模块 |
| 5001 | 数据地图 | `portal/run_data_map.py`  | 数据字典、数据血缘   |
| 5002 | 技术支持 | `portal/run_support.py`   | Excel 入库 Hive 等   |
| 5003 | 业务应用 | `business/sr_api/backend` (uvicorn) | 实时加微监测等       |

**启动步骤**（在项目根目录、已激活 venv 下）：

1. 门户（仅导航+登录，不挂子应用）：  
   `PORTAL_PORT=5000 python portal/app.py`
2. 数据地图：`DATA_MAP_PORT=5001 python portal/run_data_map.py`
3. 技术支持：`SUPPORT_PORT=5002 python portal/run_support.py`
4. 业务应用：`python business/app.py` 或 `python portal/run_business.py`

**让门户跳转到三个模块**：设置环境变量后启动门户，使 iframe/链接指向对应端口：

```bash
export DATA_MAP_URL="http://本机IP或域名:5001"
export SUPPORT_URL="http://本机IP或域名:5002"
export BUSINESS_URL="http://本机IP或域名:5003"
python portal/app.py
```

若通过 **Nginx 反代** 做同源（推荐，Cookie 可共享、无需改端口访问）：  
例如 `/` → 5000，`/data-map/` → 5001，`/support/` → 5002，`/business/` → 5003，则门户启动时设：

```bash
export DATA_MAP_URL="/data-map"
export SUPPORT_URL="/support"
export BUSINESS_URL="/business"
```

多端口下，数据地图/技术支持模块的 `/login`、`/logout` 会重定向到门户；可通过环境变量 `PORTAL_URL`（如 `http://your-domain:5000`）指定门户地址，便于从模块页跳转回门户登录。

**没有 Nginx 时**：门户会在 iframe 地址后自动带上短期 `portal_token`（约 10 分钟有效），数据地图(5001)、技术支持(5002) 会校验该参数并放行，因此**一次在门户登录即可跨端口访问**，无需每个端口单独登录。业务应用(5003) 当前不校验 token，多为内网使用时可保持现状。

**Supervisor 示例**：除 `portal/supervisor_portal.conf` 外，可选用：

- `portal/supervisor_data_map.conf`（数据地图 5001）
- `portal/supervisor_support.conf`（技术支持 5002）
- `business/supervisor.conf`（业务应用）

将上述 conf 中 `directory`、`command`、日志路径改为实际值后，`supervisorctl reread && supervisorctl update` 再按需 `start data_map/support/sr_api`。

---

## 用 Supervisor 起服务

配置见 **`portal/supervisor_portal.conf`**。需要改的目录只有这些：

| 配置项 | 填什么 |
|--------|--------|
| **directory** | **项目根目录**（`python_api` 的绝对路径），例如 `/home/you/python_api`。必须用根目录，不能只填 `portal`，否则 `run_all.py` 找不到 `data_map`、`support`、`business`、`portal`。 |
| **command** | 用**项目根目录虚拟环境**里的 Python 执行 `portal/run_all.py`。例如：`/path/to/python_api/venv/bin/python portal/run_all.py`。 |
| **user** | 运行进程的系统用户（如 `tenant_sync`、`root`）。 |
| **stdout_logfile** | 日志文件路径（如 `/var/log/supervisor/portal.log`），目录需存在或有写权限。 |

把 `supervisor_portal.conf` 拷到 Supervisor 的 include 目录（或主配置里 include 该文件），改好上面几项后执行：

```bash
supervisorctl reread && supervisorctl update
supervisorctl start portal
```

---

## 单独运行某个服务（不用门户时）

先激活根目录虚拟环境，再在项目根目录执行（或进入对应子目录后用同一 venv 的 `python`）：

- 数据地图（数据字典+数据血缘）：`python data_map/app.py`（默认 5001，含 /hive_metadata/、/sql_lineage_web/）
- 业务场景：`python business/app.py`

这样就是一个「大项目 + 一根目录虚拟环境 + 一个门户端口」的架构，后续加新模块只需在门户里增加导航和挂载或代理即可。
