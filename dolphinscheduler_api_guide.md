# DolphinScheduler API 使用指南 - 查询失败任务

## 概述

本指南介绍如何使用 DolphinScheduler REST API 查询某一天失败的任务。

## 前置准备

### 1. 获取 API Token

1. 登录 DolphinScheduler Web 界面
2. 进入 **安全中心** -> **Token管理**
3. 点击 **创建 Token**
4. 复制生成的 Token（格式类似：`abc123def456...`）

### 2. 确定项目编码

项目编码（`project_code`）可以通过以下方式获取：

- **方式1**: 查看项目工作流时的 URL，例如：
  ```
  http://192.168.168.219:12345/dolphinscheduler/projects/123456/workflow/instance
  ```
  其中 `123456` 就是项目编码

- **方式2**: 使用 API 获取项目列表（见脚本中的 `get_projects()` 方法）

## API 端点说明

### 查询任务实例

**端点**: `/api/v2/projects/{projectCode}/task-instances`

**方法**: `GET`

**请求头**:
```
Accept: application/json
Content-Type: application/json
token: {your_token}
```

**查询参数**:
- `pageNo`: 页码（默认 1）
- `pageSize`: 每页数量（默认 10）
- `stateType`: 状态类型
  - `SUCCESS`: 成功
  - `FAILURE`: 失败
  - `RUNNING`: 运行中
  - `PAUSE`: 暂停
  - `STOP`: 停止
  - `KILL`: 已杀死
- `startDate`: 开始时间，格式: `YYYY-MM-DD HH:mm:ss`
- `endDate`: 结束时间，格式: `YYYY-MM-DD HH:mm:ss`
- `searchVal`: 搜索关键词（可选）
- `taskExecuteType`: 运行类型（可选），与页面「运行类型」一致
  - `BATCH`: 调度执行（只查调度触发的失败时传此参数）
  - 重跑等对应其它值，可在 Swagger 或接口返回的 `taskExecuteType` 中查看

**响应格式**:
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "totalList": [
      {
        "id": 12345,
        "taskInstanceId": 12345,
        "taskName": "任务名称",
        "taskType": "SHELL",
        "processInstanceId": 67890,
        "processInstanceName": "工作流名称",
        "state": "FAILURE",
        "startTime": "2026-02-05 10:00:00",
        "endTime": "2026-02-05 10:05:00",
        "duration": 300,
        "host": "192.168.1.100",
        "retryTimes": 0,
        "maxRetryTimes": 3
      }
    ],
    "total": 1,
    "totalPage": 1
  }
}
```

## 使用示例

### 方式1: 使用提供的 Python 脚本

1. 编辑 `dolphinscheduler_query_failed_tasks.py`
2. 修改配置：
   ```python
   BASE_URL = "http://192.168.168.219:12345/dolphinscheduler"
   TOKEN = "your_token_here"
   QUERY_DATE = "2026-02-05"
   ```
3. 运行脚本：
   ```bash
   python dolphinscheduler_query_failed_tasks.py
   ```

### 方式2: 使用 curl 命令

```bash
# 查询 2026-02-05 失败的任务
curl -X GET \
  "http://192.168.168.219:12345/dolphinscheduler/api/v2/projects/123456/task-instances?pageNo=1&pageSize=100&stateType=FAILURE&startDate=2026-02-05%2000:00:00&endDate=2026-02-05%2023:59:59" \
  -H "Accept: application/json" \
  -H "token: your_token_here"
```

### 方式3: 使用 Python requests

```python
import requests

BASE_URL = "http://192.168.168.219:12345/dolphinscheduler"
TOKEN = "your_token_here"
PROJECT_CODE = "123456"
DATE = "2026-02-05"

headers = {
    'Accept': 'application/json',
    'token': TOKEN
}

params = {
    'pageNo': 1,
    'pageSize': 100,
    'stateType': 'FAILURE',
    'startDate': f'{DATE} 00:00:00',
    'endDate': f'{DATE} 23:59:59'
}

response = requests.get(
    f"{BASE_URL}/api/v2/projects/{PROJECT_CODE}/task-instances",
    headers=headers,
    params=params
)

result = response.json()
failed_tasks = result['data']['totalList']

for task in failed_tasks:
    print(f"任务: {task['taskName']}, 状态: {task['state']}")
```

## 常见问题

### 1. Token 无效或过期

**错误**: `API 错误: token is invalid`

**解决**: 
- 检查 Token 是否正确
- 在 DolphinScheduler 安全中心重新创建 Token

### 2. 项目编码不存在

**错误**: `API 错误: project does not exist`

**解决**: 
- 使用 `get_projects()` 方法获取正确的项目编码
- 确认项目编码格式正确（通常是数字）

### 3. 权限不足

**错误**: `API 错误: user has no permission`

**解决**: 
- 确认 Token 对应的用户有查看该项目的权限
- 联系管理员分配权限

### 4. 日期格式错误

**错误**: `API 错误: date format error`

**解决**: 
- 确保日期格式为 `YYYY-MM-DD HH:mm:ss`
- 例如: `2026-02-05 00:00:00`

## 扩展功能

### 查询多个项目

```python
# 查询所有项目在指定日期的失败任务
for project in projects:
    project_code = project['code']
    failed_tasks = client.query_failed_tasks_by_date(
        project_code=project_code,
        date=QUERY_DATE
    )
    print(f"项目 {project['name']}: {len(failed_tasks)} 个失败任务")
```

### 查询时间范围

```python
# 查询 2026-02-01 到 2026-02-05 的失败任务
result = client.query_failed_tasks(
    project_code=PROJECT_CODE,
    start_date="2026-02-01 00:00:00",
    end_date="2026-02-05 23:59:59",
    page_size=1000
)
```

### 导出为 Excel

可以结合 `pandas` 库将结果导出为 Excel：

```python
import pandas as pd

df = pd.DataFrame(failed_tasks)
df.to_excel(f"failed_tasks_{QUERY_DATE}.xlsx", index=False)
```

## 参考资源

- DolphinScheduler Swagger UI: `http://192.168.168.219:12345/dolphinscheduler/swagger-ui/index.html?language=zh_CN&lang=cn`
- 官方文档: https://dolphinscheduler.apache.org/
- API 文档: 在 Swagger UI 中查看完整的 API 接口说明
