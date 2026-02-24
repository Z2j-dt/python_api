# 打包并上传整个项目

建议在 **项目上一级目录** 打包（解压后直接得到 `python_api` 文件夹）。

---

## 放到服务器上的内容概览

| 类型 | 内容 | 说明 |
|------|------|------|
| **整包上传** | 除下表「排除项」外的**整个 `python_api` 目录** | 源码、配置示例、静态资源、SQL 等 |
| **排除不传** | `venv/`、`.venv/`、`node_modules/`、`business/sr_api/frontend/dist`、`.git`、`__pycache__/` | 体积大或服务器上重建 |
| **服务器上新建** | `portal/users.json` 或 `portal/auth_config.py` | 登录账号，不提交到仓库 |
| **按需上传** | `support/column_map.json` | 若未纳入打包，可用 `python support/gen_column_map_from_c2e.py` 生成 |
| **业务场景前端** | `business/frontend/dist` | 可在服务器上 `cd business/frontend && VITE_BASE=/business/realtime_mv/ npm run build` |

**部署方式二选一：**
- **单进程**：用 `portal/supervisor_run_all.conf`，只起 `portal_run_all`（run_all.py），一个端口 5000。
- **四进程**：用 `portal/supervisor_portal.conf`、`portal/supervisor_data_map.conf`、`portal/supervisor_support.conf`、`business/supervisor.conf`，分别起 portal(5000)、data_map(5001)、support(5002)、business(5003)，四个服务互不影响。

**目录清单（上传后服务器上应有）：**

```
python_api/
├── portal/           # 门户 + 数据地图/技术支持聚合入口
├── data_map/         # 数据地图（仅 app.py 单入口，含数据字典 + 数据血缘）
├── support/           # 技术支持（仅 app.py 单入口，含 column_map.json）
├── business/          # 业务应用（仅 app.py 单入口，含 frontend、sql）
├── requirements.txt   # 根依赖
├── README.md
└── pack-and-upload.md
```

---

## 一、打包（排除 venv、node_modules 等大目录）

### Windows（PowerShell）

在 **E:\\** 下执行（即 python_api 的上一级）：

```powershell
cd E:\

tar -czvf python_api.tar.gz --exclude=python_api/venv --exclude=python_api/.venv --exclude=python_api/node_modules --exclude=python_api/business/frontend/node_modules --exclude=python_api/business/frontend/dist --exclude=python_api/.git python_api
```

若本机 tar 不支持多个 --exclude，可先进入目录再打（解压时需指定目录）：

```powershell
cd E:\python_api
tar -czvf E:\python_api.tar.gz --exclude=venv --exclude=node_modules --exclude=dist --exclude=.git .
```

得到 **E:\python_api.tar.gz**（或 E:\python_api\python_api.tar.gz）。

### Linux / Mac

在项目**上一级目录**执行：

```bash
cd /path/to   # 即 python_api 的父目录

tar -czvf python_api.tar.gz \
  --exclude=python_api/venv \
  --exclude=python_api/.venv \
  --exclude=python_api/node_modules \
  --exclude=python_api/business/frontend/node_modules \
  --exclude=python_api/business/frontend/dist \
  --exclude=python_api/.git \
  python_api
```

得到 **python_api.tar.gz**。

---

## 二、上传到服务器

把 **python_api.tar.gz** 传到服务器（把 `user`、`服务器IP`、路径换成你的）：

```bash
scp python_api.tar.gz user@服务器IP:/home/tenant_sync/zzj/
```

示例：

```bash
scp E:\python_api.tar.gz root@192.168.89.33:/home/tenant_sync/zzj/
```

Windows 下若没有 scp，可用 **WinSCP**、**Xftp** 等，把本机的 `python_api.tar.gz` 拖到服务器 `/home/tenant_sync/zzj/` 即可。

---

## 三、在服务器上解压

SSH 登录服务器后：

```bash
cd /home/tenant_sync/zzj/
tar -xzvf python_api.tar.gz
```

若打包时是“上一级目录 + python_api”打的，会得到 `python_api` 文件夹；若之前已有 `python_api`，可先备份再解压，或解压到临时目录再覆盖。

---

## 四、上传后要做的

1. **账号文件**：打包默认不会包含 `portal/users.json`（在 .gitignore）。若用 users.json 做登录，需在服务器上新建 `portal/users.json`，或本机单独传一次该文件。
2. **业务场景前端**：打包不含 `business/frontend/dist`。在本地 `cd business/frontend && VITE_BASE=/business/realtime_mv/ npm run build` 后单独上传 dist 或使用 pack-dist-for-upload.sh。
3. **虚拟环境**：服务器上在项目根重新建 venv 并安装依赖：
   ```bash
   cd /home/tenant_sync/zzj/python_api
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
4. **Supervisor**：四进程时把 4 个 conf 里 `directory`、`command`、`environment` 中的路径和 IP 改好，然后：
   ```bash
   supervisorctl reread && supervisorctl update
   supervisorctl start portal && supervisorctl start data_map && supervisorctl start support && supervisorctl start business
   supervisorctl status
   ```
   单进程时只起 `portal_run_all`（用 supervisor_run_all.conf），不要起 data_map、support、business。
