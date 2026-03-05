# -*- coding: utf-8 -*-
"""
DolphinScheduler API - 查询某一天失败的任务
独立脚本，不依赖当前项目
"""
import os
import sys
import time
import json
import requests
import pymysql
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Iterable


def _safe_ascii(s: str) -> str:
    """将字符串转为仅含 ASCII，避免在 latin-1 环境下 print/编码报错。"""
    if not isinstance(s, str):
        s = str(s)
    return s.encode("ascii", "replace").decode("ascii")

# 默认按顺序处理的 Dolphin 项目编码列表；可被环境变量 DOLPHIN_PROJECT_CODES 覆盖
DEFAULT_PROJECT_CODES_ORDER = [
    "13377370070752",
    "13321956116192",
    "14149466369984",
    "14945979919552",
    "15677652235104",
    "16605162982368",
    "19428378151520",
    "19428382306400",
    "19581182124384",
    "19428387078368",
    "19428380796256",
]


class DolphinSchedulerAPI:
    """DolphinScheduler API 客户端"""
    
    def __init__(self, base_url: str, token: str):
        """
        初始化 API 客户端
        
        Args:
            base_url: DolphinScheduler 服务地址，例如: http://192.168.168.219:12345/dolphinscheduler
            token: API Token（从 DolphinScheduler 安全中心 -> Token管理 获取）
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'token': self.token
        }
    
    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                 data: Optional[Dict] = None, form_data: Optional[Dict] = None,
                 allow_redirects: bool = True) -> Dict[str, Any]:
        """
        发送 API 请求
        
        Args:
            method: HTTP 方法 (GET, POST, etc.)
            endpoint: API 端点路径，例如: /api/v2/projects/{projectCode}/task-instances
            params: URL 参数
            data: 请求体数据（JSON，与 form_data 二选一）
            form_data: 表单 body（application/x-www-form-urlencoded），与 data 二选一；部分 Dolphin 接口要求用 form 提交
            allow_redirects: 是否跟随重定向（若重定向 Location 含非 ASCII 易触发 latin-1 编码错误，可设为 False）
            
        Returns:
            API 响应数据
        """
        url = f"{self.base_url}{endpoint}"
        # 请求 URL 与请求头仅使用 ASCII，避免 httplib/requests 按 latin-1 编码时报错
        url = _safe_ascii(url)
        headers_safe = {k: _safe_ascii(v) if isinstance(v, str) else str(v) for k, v in self.headers.items()}
        if form_data:
            # 使用 form 时由 requests 自动设置 Content-Type，去掉 JSON 头避免冲突
            headers_safe = {k: v for k, v in headers_safe.items() if k.lower() != "content-type"}
        params_safe = None
        if params:
            params_safe = {k: _safe_ascii(str(v)) if isinstance(v, str) else v for k, v in params.items()}
        else:
            params_safe = params

        try:
            if form_data:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers_safe,
                    params=params_safe,
                    data=form_data,
                    timeout=30,
                    allow_redirects=allow_redirects,
                )
            else:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers_safe,
                    params=params_safe,
                    json=data,
                    timeout=30,
                    allow_redirects=allow_redirects,
                )
            # 强制按 UTF-8 解码，避免服务端返回中文时被 latin-1 编码报错
            if response.encoding and response.encoding.lower() in ("iso-8859-1", "latin-1"):
                response.encoding = "utf-8"
            # 先检查响应内容，避免非 JSON 时直接 .json() 报错
            text = (response.text or "").strip()
            actual_url = response.url  # 重定向后的真实 URL
            if not text:
                raise Exception(
                    f"请求返回空内容 (状态码: {response.status_code})。"
                    f"请求 URL: {actual_url}。"
                    f"请检查: 1) 地址是否正确 2) Token 是否有效 3) 服务是否可访问"
                )
            try:
                result = response.json()
            except ValueError:
                # 返回的不是 JSON，可能是 HTML 前端页（说明没命中后端 API）
                preview = (text[:150] + "...") if len(text) > 150 else text
                raise Exception(
                    f"接口返回的是 HTML 页面而非 JSON (状态码: {response.status_code})。\n"
                    f"当前请求 URL: {actual_url}\n"
                    f"说明该路径没有命中后端 API，可能被前端路由返回了页面。\n"
                    f"请打开 Swagger 文档确认「项目列表」接口的实际路径：\n"
                    f"  {self.base_url}/swagger-ui/index.html?language=zh_CN\n"
                    f"在 Swagger 里找到 projects 或「项目」相关接口，查看其完整路径。\n"
                    f"返回内容预览: {preview!r}"
                )
            response.raise_for_status()
            
            # DolphinScheduler API 统一响应格式: {code: 0, msg: "success", data: {...}}
            if result.get('code') != 0:
                raise Exception(f"API 错误: {result.get('msg', 'Unknown error')}")
            
            return result.get('data', {})
        except requests.exceptions.RequestException as e:
            raise Exception(f"请求失败: {str(e)}")
    
    def get_projects(self) -> List[Dict[str, Any]]:
        """获取项目列表（多路径尝试：list/query-project-list 为常见文档路径）"""
        for endpoint in [
            "/projects/list",              # 2.x/3.x 常见
            "/projects/query-project-list", # 1.x 常见
            "/projects",
            "/api/v2/projects",
            "/api/v1/projects",
        ]:
            try:
                data = self._request('GET', endpoint)
                if isinstance(data, list):
                    return data
                # 分页接口可能返回 { totalList: [...] } 或 { data: [...] }
                out = data.get('totalList', data.get('data'))
                if isinstance(out, list):
                    return out
            except Exception:
                continue
        raise Exception(
            "无法获取项目列表。当前接口返回的是 HTML 而非 JSON。\n"
            f"请打开 Swagger 确认项目列表路径：{self.base_url}/swagger-ui/index.html?language=zh_CN"
        )
    
    def query_task_instances(
        self,
        project_code: str,
        start_date: str,
        end_date: Optional[str] = None,
        page_no: int = 1,
        page_size: int = 10,
        state_type: str = "FAILURE",
    ) -> Dict[str, Any]:
        """
        查询任务实例（可指定状态类型）。
        state_type: FAILURE=失败, KILL=已杀死, 不传或传空则可能返回全部（视 API 是否支持）。
        """
        if not end_date:
            end_date = f"{start_date[:10]} 23:59:59" if len(start_date) >= 10 else f"{start_date} 23:59:59"
        if len(start_date) == 10:
            start_date = f"{start_date} 00:00:00"
        return self.query_failed_tasks(
            project_code=project_code,
            start_date=start_date,
            end_date=end_date,
            page_no=page_no,
            page_size=page_size,
            state_type=state_type,
        )
    
    def query_process_instances(
        self,
        project_code: str,
        page_no: int = 1,
        page_size: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        查询流程实例（process-instances）接口。

        - 若传 start_date/end_date，则带时间范围参数；
        - 若不传，则不按时间过滤，直接分页拿所有流程实例。
        使用接口: GET /projects/{projectCode}/process-instances
        """
        params: Dict[str, Any] = {
            "pageNo": page_no,
            "pageSize": page_size,
        }
        if start_date:
            if not end_date:
                end_date = f"{start_date[:10]} 23:59:59" if len(start_date) >= 10 else f"{start_date} 23:59:59"
            if len(start_date) == 10:
                start_date = f"{start_date} 00:00:00"
            params["startDate"] = start_date
            params["endDate"] = end_date

        for path_prefix in ["", "/api/v2", "/api/v1"]:
            endpoint = f"{path_prefix}/projects/{project_code}/process-instances".lstrip("/")
            endpoint = "/" + endpoint if not endpoint.startswith("/") else endpoint
            try:
                return self._request("GET", endpoint, params=params)
            except Exception:
                continue
        raise Exception(f"未找到可用的流程实例接口，请确认项目编码 {project_code}")

    def query_instances_by_schedule_date(
        self,
        project_code: str,
        date: str,
    ) -> List[Dict[str, Any]]:
        """
        按调度日期（scheduleTime 的日期部分）查询某项目当天的所有流程实例。
        使用 /process-instances 接口，按 scheduleTime 过滤：
          - 不再依赖 startDate/endDate 的时间窗口，避免 Dolphin 内部实现导致的数据缺失
          - pageNo 从 1 开始，最多翻 10 页，每页 200 条（单项目 ~2000 条上限）
        """
        all_instances: List[Dict[str, Any]] = []
        page_no = 1
        page_size = 200
        while True:
            result: Dict[str, Any] = {}
            try:
                result = self.query_process_instances(
                    project_code=project_code,
                    page_no=page_no,
                    page_size=page_size,
                    start_date=None,
                    end_date=None,
                )
            except Exception as e:
                print(f"查询流程实例失败(project={project_code}, page={page_no}): {e}")
                break

            items = result.get("totalList") or result.get("data") or []
            if not items:
                break

            for pi in items:
                sch = (pi.get("scheduleTime") or "")[:10]
                if sch == date:
                    all_instances.append(pi)

            # 最多翻 10 页，或当页数量不足 page_size 时停止
            if len(items) < page_size or page_no >= 10:
                break
            page_no += 1

        return all_instances

    def query_failed_tasks(
        self,
        project_code: str,
        start_date: str,
        end_date: Optional[str] = None,
        page_no: int = 1,
        page_size: int = 10,
        state_type: Optional[str] = "FAILURE",
        task_execute_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        查询任务实例。
        state_type: FAILURE/KILL/None(不传则部分版本会返回全部)。
        task_execute_type: 运行类型，与页面「运行类型」一致时可只查调度失败。
          常见值: BATCH=调度执行, RECOVER_WAITTING/RERUN 等=重跑。
          若传 "BATCH" 则只查「调度执行」的失败任务（与 UI 上“运行类型=调度执行”对应）。
        """
        if not end_date:
            end_date = f"{start_date[:10]} 23:59:59" if len(start_date) >= 10 else f"{start_date} 23:59:59"
        if len(start_date) == 10:
            start_date = f"{start_date} 00:00:00"
        params = {
            'pageNo': page_no,
            'pageSize': page_size,
            'startDate': start_date,
            'endDate': end_date,
            'searchVal': '',
        }
        if state_type:
            params['stateType'] = state_type
        # 运行类型：BATCH=调度执行，仅查调度执行的失败时可传此参数（具体名以 Swagger 为准）
        if task_execute_type:
            params['taskExecuteType'] = task_execute_type
        for path_prefix in ["", "/api/v2", "/api/v1"]:
            endpoint = f"{path_prefix}/projects/{project_code}/task-instances".lstrip("/")
            endpoint = "/" + endpoint if not endpoint.startswith("/") else endpoint
            try:
                return self._request('GET', endpoint, params=params)
            except Exception:
                continue
        raise Exception(f"未找到可用的任务实例接口，请确认项目编码 {project_code}")

    def rerun_task_instance(self, project_code: str, task_instance_id: int) -> Dict[str, Any]:
        """
        触发单个任务实例重跑（使用 task_instance_id）。
        不同版本路径可能有差异，这里沿用查询接口的多路径探测。
        """
        for path_prefix in ["", "/api/v2", "/api/v1"]:
            endpoint = f"{path_prefix}/projects/{project_code}/task-instance/{task_instance_id}/rerun".lstrip("/")
            endpoint = "/" + endpoint if not endpoint.startswith("/") else endpoint
            try:
                return self._request("POST", endpoint)
            except Exception:
                continue
        raise Exception(f"未找到可用的任务重跑接口，请确认项目编码 {project_code} 和任务实例 {task_instance_id}")

    def execute_process_instance(
        self,
        project_code: str,
        process_instance_id: int,
        execute_type: str = "REPEAT_RUNNING",
    ) -> Dict[str, Any]:
        """
        执行流程实例操作（重跑、暂停、恢复等）。
        接口与 Swagger 一致：POST .../projects/{projectCode}/executors/execute?processInstanceId=xxx&executeType=REPEAT_RUNNING，body 为空。
        仅使用无前缀路径（/api/v1 等会返回 405）。
        """
        endpoint = "/projects/{}/executors/execute".format(project_code)
        return self._request(
            "POST",
            endpoint,
            params={
                "processInstanceId": int(process_instance_id),
                "executeType": str(execute_type),
            },
            allow_redirects=False,
        )

    def rerun_task_by_process_instance(self, project_code: str, process_instance_id: int, task_code: int) -> Dict[str, Any]:
        """
        触发任务重跑（使用 processInstanceId + taskCode）。
        路径：/projects/{projectCode}/process-instance/{processInstanceId}/task-instance/{taskCode}/rerun
        """
        for path_prefix in ["", "/api/v2", "/api/v1"]:
            endpoint = f"{path_prefix}/projects/{project_code}/process-instance/{process_instance_id}/task-instance/{task_code}/rerun".lstrip("/")
            endpoint = "/" + endpoint if not endpoint.startswith("/") else endpoint
            try:
                return self._request("POST", endpoint)
            except Exception:
                continue
        raise Exception(f"未找到可用的任务重跑接口（processInstanceId={process_instance_id}, taskCode={task_code}）")

    def rerun_task_by_code(self, project_code: str, process_definition_code: int, task_code: int) -> Dict[str, Any]:
        """
        触发任务重跑（使用 processDefinitionCode + taskCode）。
        路径：/projects/{projectCode}/process-definition/{processDefinitionCode}/task-instance/{taskCode}/rerun
        """
        for path_prefix in ["", "/api/v2", "/api/v1"]:
            endpoint = f"{path_prefix}/projects/{project_code}/process-definition/{process_definition_code}/task-instance/{task_code}/rerun".lstrip("/")
            endpoint = "/" + endpoint if not endpoint.startswith("/") else endpoint
            try:
                return self._request("POST", endpoint)
            except Exception:
                continue
        raise Exception(f"未找到可用的任务重跑接口（processDefinitionCode={process_definition_code}, taskCode={task_code}）")
    
    def query_failed_tasks_by_date(
        self,
        project_code: str,
        date: str,
        task_execute_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        查询指定日期所有失败的任务（自动分页；包含 FAILURE + KILL，去重）。
        task_execute_type: 若传 "BATCH" 则只查「调度执行」的失败（与 UI「运行类型=调度执行」一致）。
        """
        seen_ids = set()
        all_tasks = []
        page_size = 100
        for state_type in ("FAILURE", "KILL"):
            page_no = 1
            while True:
                result = self.query_failed_tasks(
                    project_code=project_code,
                    start_date=date,
                    page_no=page_no,
                    page_size=page_size,
                    state_type=state_type,
                    task_execute_type=task_execute_type,
                )
                tasks = result.get('totalList', [])
                if not tasks:
                    break
                for t in tasks:
                    tid = t.get('id')
                    if tid is not None and tid not in seen_ids:
                        seen_ids.add(tid)
                        all_tasks.append(t)
                total = result.get('total', 0)
                if len(tasks) < page_size or len(all_tasks) >= total:
                    break
                page_no += 1
        all_tasks.sort(key=lambda x: (x.get('startTime') or '', x.get('id') or 0))
        return all_tasks

    def query_all_task_instances_by_date(
        self,
        project_code: str,
        date: str,
    ) -> List[Dict[str, Any]]:
        """
        查询指定日期内所有任务实例（不限状态、不限运行类型），用于判断「同一任务是否有成功」。
        优先使用流程实例接口（process-instances）查询所有状态的任务。
        如果流程实例接口返回的是流程实例数据，需要进一步获取每个流程实例下的任务实例。
        """
        all_tasks = []
        page_size = 300
        page_no = 1
        
        # 使用流程实例接口查询所有流程实例
        while True:
            try:
                result = self.query_process_instances(
                    project_code=project_code,
                    start_date=date,
                    page_no=page_no,
                    page_size=page_size,
                )
                process_instances = result.get('totalList', [])
                if not process_instances:
                    break
                
                # 流程实例接口返回的是流程实例，需要转换为任务实例格式
                # 流程实例代表整个工作流，但我们需要的是工作流下的任务实例
                # 如果流程实例数据中包含任务列表，使用任务列表；否则将流程实例作为任务处理
                for pi in process_instances:
                    process_instance_id = pi.get('id')
                    process_instance_name = pi.get('name') or pi.get('processInstanceName')
                    state = pi.get('state')
                    
                    # 检查流程实例数据中是否包含任务列表（taskList 或 tasks）
                    task_list = pi.get('taskList') or pi.get('tasks') or []
                    
                    if task_list:
                        # 如果包含任务列表，提取每个任务
                        for task in task_list:
                            task_data = {
                                'id': task.get('id') or task.get('taskInstanceId'),
                                'processInstanceId': process_instance_id,
                                'processInstanceName': process_instance_name,
                                'taskCode': task.get('taskCode') or task.get('code'),
                                'taskName': task.get('taskName') or task.get('name'),
                                'taskType': task.get('taskType') or task.get('type', 'WORKFLOW'),
                                'state': task.get('state') or state,  # 任务状态优先，否则用流程实例状态
                                'startTime': task.get('startTime') or pi.get('startTime'),
                                'endTime': task.get('endTime') or pi.get('endTime'),
                                'host': task.get('host') or pi.get('host', ''),
                                'taskExecuteType': task.get('taskExecuteType') or pi.get('runMode', ''),
                            }
                            # 保留原始数据
                            task_data.update(task)
                            task_data['_processInstance'] = pi  # 保留流程实例信息
                            all_tasks.append(task_data)
                    else:
                        # 如果没有任务列表，将流程实例作为任务处理（流程实例级别的任务）
                        task_data = {
                            'id': process_instance_id,  # 流程实例ID作为任务实例ID
                            'processInstanceId': process_instance_id,
                            'processInstanceName': process_instance_name,
                            'taskCode': pi.get('processDefinitionCode') or pi.get('processDefinitionId'),
                            'taskName': process_instance_name,
                            'taskType': pi.get('taskType', 'WORKFLOW'),
                            'state': state,
                            'startTime': pi.get('startTime'),
                            'endTime': pi.get('endTime'),
                            'host': pi.get('host', ''),
                            'taskExecuteType': pi.get('runMode', '') or pi.get('taskExecuteType', ''),
                        }
                        # 保留原始数据
                        task_data.update(pi)
                        all_tasks.append(task_data)
                
                total = result.get('total', 0)
                if len(process_instances) < page_size or len(all_tasks) >= total:
                    break
                page_no += 1
            except Exception as e:
                # 如果流程实例接口失败，回退到原来的任务实例查询方式
                print(f"流程实例接口查询失败，回退到任务实例查询: {e}")
                # 回退逻辑：查询常见状态
                seen_ids = set()
                state_types = ["SUCCESS", "FAILURE", "KILL", "RUNNING", "PAUSE"]
                for state_type in state_types:
                    page_no_fallback = 1
                    while True:
                        try:
                            result_fallback = self.query_failed_tasks(
                                project_code=project_code,
                                start_date=date,
                                page_no=page_no_fallback,
                                page_size=page_size,
                                state_type=state_type,
                                task_execute_type=None,
                            )
                            tasks = result_fallback.get('totalList', [])
                            if not tasks:
                                break
                            for t in tasks:
                                tid = t.get('id')
                                if tid is not None and tid not in seen_ids:
                                    seen_ids.add(tid)
                                    all_tasks.append(t)
                            total_fallback = result_fallback.get('total', 0)
                            if len(tasks) < page_size or len(all_tasks) >= total_fallback:
                                break
                            page_no_fallback += 1
                        except Exception:
                            break
                break
        
        all_tasks.sort(key=lambda x: (x.get('startTime') or '', x.get('id') or 0))
        return all_tasks

    def query_failed_tasks_one_per_logical_task(self, project_code: str, date: str) -> List[Dict[str, Any]]:
        """
        按「调度日期 + 同一逻辑任务」判断是否失败：
        - 同一任务（同一工作流实例+同一任务节点）在当天只要有一条成功 → 视为成功，不列入失败。
        - 同一任务在当天没有任何成功记录 → 视为失败，只输出一条代表记录。
        不管执行次数、不管运行类型，只按「该任务当天是否有成功」判断。
        """
        all_records = self.query_all_task_instances_by_date(project_code=project_code, date=date)
        grouped = group_failed_tasks_by_logical_task(all_records)
        failed_one_per_task = []
        for key, records in grouped.items():
            has_success = any((r.get('state') or '').upper() == 'SUCCESS' for r in records)
            if has_success:
                continue
            # 该逻辑任务当天没有任何成功，取一条作为代表（取最后一次失败的记录，便于看时间）
            records_sorted = sorted(records, key=lambda x: (x.get('startTime') or '', x.get('id') or 0), reverse=True)
            failed_one_per_task.append(records_sorted[0])
        failed_one_per_task.sort(key=lambda x: (x.get('startTime') or '', x.get('id') or 0))
        return failed_one_per_task

    def query_all_tasks_latest_state(self, project_code: str, date: str) -> List[Dict[str, Any]]:
        """
        查询指定日期所有任务的最新状态：
        - 查询所有任务实例（不限状态：SUCCESS、FAILURE、KILL等）
        - 按逻辑任务分组（同一工作流实例+同一任务节点）
        - 每个逻辑任务只取最新一次执行的状态（按 startTime 和 id 排序，取最后一条）
        返回所有任务的最新状态列表。
        """
        all_records = self.query_all_task_instances_by_date(project_code=project_code, date=date)
        grouped = group_failed_tasks_by_logical_task(all_records)
        latest_tasks = []
        for key, records in grouped.items():
            # 每组内按开始时间和ID排序，取最后一条（最新的一次执行）
            records_sorted = sorted(records, key=lambda x: (x.get('startTime') or '', x.get('id') or 0), reverse=True)
            latest_tasks.append(records_sorted[0])
        # 最终结果按开始时间和ID排序
        latest_tasks.sort(key=lambda x: (x.get('startTime') or '', x.get('id') or 0))
        return latest_tasks


def group_failed_tasks_by_logical_task(tasks: List[Dict[str, Any]]) -> Dict[tuple, List[Dict[str, Any]]]:
    """
    按「逻辑任务」分组：同一工作流实例下的同一任务节点，失败多次（重跑再失败）会有多条记录，
    这里把 (processInstanceId, taskCode) 相同的归为同一个逻辑任务。
    返回: {(processInstanceId, taskCode): [记录1, 记录2, ...]}
    """
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for t in tasks:
        pid = t.get('processInstanceId')
        tc = t.get('taskCode')
        # 若没有 taskCode 用 taskName 兜底
        key = (pid, tc if tc is not None else t.get('taskName'))
        if key not in groups:
            groups[key] = []
        groups[key].append(t)
    # 每组内按开始时间排序，方便看第几次失败
    for key in groups:
        groups[key].sort(key=lambda x: (x.get('startTime') or '', x.get('id') or 0))
    return groups


# ===================== StarRocks 落地与查询 =====================

DEFAULT_SR_CONF = {
    "host": os.environ.get("STARROCKS_HOST", "10.8.93.40"),
    "port": int(os.environ.get("STARROCKS_PORT", "9030")),
    "user": os.environ.get("STARROCKS_USER", "root"),
    "password": os.environ.get("STARROCKS_PASSWORD", "star@dt1988"),
    "database": os.environ.get("STARROCKS_DATABASE", "portal_db"),
    "table": os.environ.get("STARROCKS_TABLE", "dolphin_failed_task"),
}


class StarRocksStore:
    """StarRocks 存取封装（基于 MySQL 协议）。"""

    def __init__(
        self,
        host: str = DEFAULT_SR_CONF["host"],
        port: int = DEFAULT_SR_CONF["port"],
        user: str = DEFAULT_SR_CONF["user"],
        password: str = DEFAULT_SR_CONF["password"],
        database: str = DEFAULT_SR_CONF["database"],
        table: str = DEFAULT_SR_CONF["table"],
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = table

    @contextmanager
    def _conn(self):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_table(self):
        """
        表结构需在 StarRocks 中提前创建，这里不再自动建表，避免 DDL 权限/兼容性问题。
        保留该方法以兼容旧调用，但内部什么也不做。
        """
        return

    def truncate_table(self):
        """兼容旧代码：不再使用 TRUNCATE，全量清空请在 StarRocks 手工执行。"""
        return

    def clear_by_date(self, query_date: str):
        """按调度日期清空当日数据，避免影响历史日期。"""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM `{self.table}` WHERE `query_date` = %s", (query_date,))

    def query_scalar(self, sql: str, params: Optional[Iterable[Any]] = None) -> Any:
        """执行查询并返回第一行第一列（无结果返回 None）。"""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                row = cur.fetchone()
                if not row:
                    return None
                # DictCursor: row 是 dict，取第一个 value
                if isinstance(row, dict):
                    return next(iter(row.values()), None)
                # 兜底：tuple/list
                try:
                    return row[0]
                except Exception:
                    return None

    def resolve_query_date_from_tradedate(
        self,
        tradedate: str,
        calendar_day: str = "20260101",
    ) -> Optional[str]:
        """
        按交易日计算 query_date（取 tradedate<=入参 的最近一个交易日，然后 date_add +1）。

        对应 SQL（StarRocks 外表/外部 catalog 可直接查询）：
        select date_add(tradedate, 1)
        from hive_catalog.ods.o_sd_thk_fxckhdata_t_pub_tradedate
        where day = '20260101' and iftradingday = '1' and tradedate < '{tradedate}'
        order by tradedate desc limit 1
        """
        tradedate = (tradedate or "").strip()
        if not tradedate:
            return None
        sql = """
        SELECT date_add(tradedate, 1) AS query_date
        FROM hive_catalog.ods.o_sd_thk_fxckhdata_t_pub_tradedate
        WHERE day = %s
          AND iftradingday = '1'
          AND tradedate < %s
        ORDER BY tradedate DESC
        LIMIT 1
        """
        val = self.query_scalar(sql, (calendar_day, tradedate))
        if val is None:
            return None
        # 可能返回 date/datetime 或字符串
        try:
            if hasattr(val, "strftime"):
                return val.strftime("%Y-%m-%d")
        except Exception:
            pass
        return str(val)[:10]

    def get_latest_query_date(self) -> Optional[str]:
        """从失败任务表中取最新 query_date（无数据返回 None）。"""
        sql = f"SELECT MAX(query_date) AS query_date FROM `{self.table}`"
        val = self.query_scalar(sql)
        if val is None:
            return None
        try:
            if hasattr(val, "strftime"):
                return val.strftime("%Y-%m-%d")
        except Exception:
            pass
        return str(val)[:10]

    def upsert_failed_tasks(
        self,
        query_date: str,
        project_code: str,
        project_name: str,
        tasks: Iterable[Dict[str, Any]],
    ) -> int:
        """
        将失败任务批量 upsert 到表中。
        返回写入行数（按输入计）。
        """
        if not tasks:
            return 0
        # StarRocks 不支持 MySQL 的 "ON DUPLICATE KEY UPDATE" 语法。
        # 本项目刷新时会先按 query_date 删除当日数据，因此这里直接 INSERT 即可。
        # 不写 process_definition_code 列，兼容无该列的表；重跑时从 raw_json 解析
        sql = f"""
        INSERT INTO `{self.table}` (
            query_date, project_code, project_name,
            process_instance_id, process_instance_name,
            task_instance_id, task_code, task_name,
            task_type, schedule_time, state, start_time, end_time, host, raw_json
        ) VALUES (
            %(query_date)s, %(project_code)s, %(project_name)s,
            %(process_instance_id)s, %(process_instance_name)s,
            %(task_instance_id)s, %(task_code)s, %(task_name)s,
            %(task_type)s, %(schedule_time)s, %(state)s, %(start_time)s, %(end_time)s, %(host)s, %(raw_json)s
        );
        """
        rows = []
        for t in tasks:
            rows.append({
                "query_date": query_date,
                "project_code": project_code,
                "project_name": project_name,
                "process_instance_id": t.get("processInstanceId"),
                "process_instance_name": t.get("processInstanceName"),
                "task_instance_id": t.get("id"),
                "task_code": t.get("taskCode"),
                "task_name": t.get("taskName"),
                "task_type": t.get("taskType"),
                "schedule_time": t.get("scheduleTime"),
                "state": t.get("state"),
                "start_time": t.get("startTime"),
                "end_time": t.get("endTime"),
                "host": t.get("host"),
                "raw_json": json.dumps(t, ensure_ascii=False),
            })
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        return len(rows)

    def list_failed_tasks(self, query_date: str, project_code: Optional[str] = None) -> List[Dict[str, Any]]:
        # 不查 process_definition_code 列，兼容未加该列的表；重跑时从 raw_json 解析
        sql = f"""
        SELECT
            query_date, project_code, project_name,
            process_instance_id, process_instance_name,
            task_instance_id, task_code, task_name,
            task_type, schedule_time, state, start_time, end_time, host,
            raw_json
        FROM `{self.table}`
        WHERE query_date = %s
          AND UPPER(state) IN ('FAILURE','KILL')
        """
        params = [query_date]
        if project_code:
            sql += " AND project_code = %s"
            params.append(project_code)
        sql += " ORDER BY project_code, schedule_time, process_instance_id, task_instance_id"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()


# ===================== 重跑封装 =====================

def rerun_single_task(client: DolphinSchedulerAPI, project_code: str, task_instance_id: int) -> Dict[str, Any]:
    return client.rerun_task_instance(project_code, task_instance_id)


def rerun_failed_tasks_for_project(
    client: DolphinSchedulerAPI,
    store: StarRocksStore,
    query_date: str,
    project_code: str,
    batch_size: int = 5,
    interval_seconds: int = 60,
) -> List[Dict[str, Any]]:
    """
    从 StarRocks 查指定日期+项目的失败任务，按 start_time 正序，
    每批 batch_size 个调用重跑 API，批间 sleep interval_seconds 秒。
    使用 Dolphin 官方接口：POST /projects/{projectCode}/executors/execute?processInstanceId=&executeType=REPEAT_RUNNING
    同一流程实例只调用一次（按 processInstanceId 去重）。
    """
    tasks = store.list_failed_tasks(query_date, project_code=project_code)
    # 按调度时间正序重跑，若 schedule_time 为空则退化到 start_time
    def _sort_key(x: Dict[str, Any]):
        sch = x.get("schedule_time") or ""
        st = x.get("start_time") or ""
        return (sch, st, x.get("task_code") or 0)

    tasks = sorted(tasks, key=_sort_key)
    seen_process_instance_ids = set()
    results = []
    idx = 0
    while idx < len(tasks):
        batch = tasks[idx: idx + batch_size]
        for t in batch:
            process_instance_id = t.get("process_instance_id")
            task_code = t.get("task_code")
            task_name = t.get("task_name") or "N/A"
            if t.get("raw_json"):
                try:
                    raw = json.loads(t["raw_json"]) if isinstance(t.get("raw_json"), str) else t.get("raw_json")
                    if not process_instance_id:
                        process_instance_id = raw.get("processInstanceId")
                    if task_name == "N/A":
                        task_name = raw.get("taskName") or raw.get("name") or "N/A"
                except Exception:
                    pass
            if process_instance_id is None:
                results.append({
                    "task_code": task_code,
                    "task_name": task_name,
                    "process_instance_id": None,
                    "success": False,
                    "error": "缺少 process_instance_id",
                })
                continue
            pid = int(process_instance_id)
            if pid in seen_process_instance_ids:
                results.append({
                    "task_code": task_code,
                    "task_name": task_name,
                    "process_instance_id": pid,
                    "success": True,
                    "resp": {"skipped": "同一流程实例已重跑"},
                })
                continue
            seen_process_instance_ids.add(pid)
            try:
                resp = client.execute_process_instance(project_code, pid, execute_type="REPEAT_RUNNING")
                results.append({
                    "task_code": task_code,
                    "task_name": task_name,
                    "process_instance_id": pid,
                    "success": True,
                    "resp": resp,
                })
            except Exception as e:
                results.append({
                    "task_code": task_code,
                    "task_name": task_name,
                    "process_instance_id": pid,
                    "success": False,
                    "error": str(e),
                })
        idx += batch_size
        if idx < len(tasks):
            time.sleep(interval_seconds)
    return results


def sync_failed_tasks_for_projects(
    client: DolphinSchedulerAPI,
    store: StarRocksStore,
    query_date: str,
    project_codes_order: List[str],
    auto_rerun: bool = False,
    batch_size: int = 5,
    interval_seconds: int = 30,
    list_all_failed_instances: bool = False,
) -> Dict[str, Any]:
    """
    按指定顺序只处理给定项目列表:
      - 查询每个项目在指定日期的失败任务并落地 StarRocks
      - 可选: 自动重跑（按 start_time 正序，批量限流）

    list_all_failed_instances:
      - False（默认）: 只把「当天从未成功过」的逻辑任务算作失败（同一任务当天只要有一次成功就不展示，即：失败任务后面调用的任务有成功的就算成功）
      - True: 当天所有失败/杀死实例都列出（先失败后成功的也会出现）

    返回一个汇总结果，便于接口/脚本展示。
    """
    # 若需要可在外部确保表已存在
    # 仅清理当前查询日期的数据，避免误删历史
    store.clear_by_date(query_date)

    # 获取项目名称映射
    code_to_name: Dict[str, str] = {}
    try:
        all_projects = client.get_projects()
        for p in all_projects or []:
            c = str(p.get("code")) if p.get("code") is not None else ""
            if c:
                code_to_name[c] = p.get("name") or f"项目({c})"
    except Exception as e:
        print(f"获取项目列表失败（将仅使用 projectCode 展示）: {e}")

    summary: Dict[str, Any] = {
        "date": query_date,
        "projects": [],
        "total_failed": 0,
    }

    for project_code in project_codes_order:
        project_name = code_to_name.get(project_code) or f"项目({project_code})"
        project_info = {
            "project_code": project_code,
            "project_name": project_name,
            "failed_count": 0,  # 此处先记录“当日流程实例总数”，后续再按失败筛选时可调整含义
            "rerun_total": 0,
            "rerun_success": 0,
            "rerun_fail": 0,
        }
        try:
            # 第一步：按调度日期，将当天所有流程实例都落地，先用于对数
            instances = client.query_instances_by_schedule_date(
                project_code=project_code,
                date=query_date,
            )
            project_info["failed_count"] = len(instances)
            summary["total_failed"] += len(instances)

            if instances:
                # 将流程实例转换成 StarRocksStore.upsert_failed_tasks 期望的字段结构
                tasks: List[Dict[str, Any]] = []
                for pi in instances:
                    tasks.append(
                        {
                            "id": pi.get("id"),
                            "processInstanceId": pi.get("id"),
                            "processInstanceName": pi.get("name"),
                            "taskCode": pi.get("processDefinitionCode"),
                            "taskName": pi.get("name"),
                            "taskType": "WORKFLOW",
                            "state": pi.get("state"),
                            # Dolphin JSON 中字段为 scheduleTime，这里透传，方便落库
                            "scheduleTime": pi.get("scheduleTime"),
                            "startTime": pi.get("startTime"),
                            "endTime": pi.get("endTime"),
                            "host": pi.get("host"),
                            # 其余字段完整保存在 raw_json 中，便于后续分析
                        }
                    )
                store.upsert_failed_tasks(
                    query_date=query_date,
                    project_code=project_code,
                    project_name=project_name,
                    tasks=tasks,
                )

                if auto_rerun:
                    rerun_results = rerun_failed_tasks_for_project(
                        client=client,
                        store=store,
                        query_date=query_date,
                        project_code=project_code,
                        batch_size=batch_size,
                        interval_seconds=interval_seconds,
                    )
                    project_info["rerun_total"] = len(rerun_results)
                    project_info["rerun_success"] = sum(1 for r in rerun_results if r.get("success"))
                    project_info["rerun_fail"] = project_info["rerun_total"] - project_info["rerun_success"]
        except Exception as e:
            project_info["error"] = str(e)
        summary["projects"].append(project_info)
    return summary


def rerun_from_sr(
    client: DolphinSchedulerAPI,
    store: StarRocksStore,
    query_date: str,
    project_codes_order: List[str],
    batch_size: int = 5,
    interval_seconds: int = 30,
) -> Dict[str, Any]:
    """
    只读 StarRocks 表内指定日期的失败任务，按项目顺序、按 start_time 正序批量重跑。
    不查 Dolphin、不写表，仅「读表 → 调重跑接口」。
    """
    result: Dict[str, Any] = {"date": query_date, "projects": []}
    for project_code in project_codes_order:
        proj = {
            "project_code": project_code,
            "rerun_total": 0,
            "rerun_success": 0,
            "rerun_fail": 0,
            "rerun_details": [],
        }
        try:
            rerun_results = rerun_failed_tasks_for_project(
                client=client,
                store=store,
                query_date=query_date,
                project_code=project_code,
                batch_size=batch_size,
                interval_seconds=interval_seconds,
            )
            proj["rerun_total"] = len(rerun_results)
            proj["rerun_success"] = sum(1 for r in rerun_results if r.get("success"))
            proj["rerun_fail"] = proj["rerun_total"] - proj["rerun_success"]
            proj["rerun_details"] = rerun_results
        except Exception as e:
            proj["error"] = str(e)
        result["projects"].append(proj)
    return result


def main():
    """
    两种模式（环境变量 MODE）：
      - refresh（默认）: 只落表。从 Dolphin 拉失败任务 → 清当日 SR → 写入 SR。
      - rerun: 只重跑。从 SR 读指定日期的失败任务 → 按项目/时间正序调用重跑（5 个/批，30 秒间隔）。
    公共环境变量：
      - TRADEDATE: 交易日 YYYY-MM-DD（refresh 用）。用于查询交易日表，计算 query_date=date_add(最近交易日, 1)
      - DOLPHIN_PROJECT_CODES: 项目编码逗号分隔，不传用脚本默认 11 个项目顺序
      - DOLPHIN_BASE_URL / DOLPHIN_TOKEN: Dolphin 接口（rerun 模式必填）。BASE_URL 需与登录/生成 Token 时使用的地址一致（内网 10.8.93.34 与公网 192.168.168.219 不一致时易 401）
      - STARROCKS_*: SR 连接（refresh 必填；rerun 必填）
    """
    from datetime import date as date_type
    # 强制标准输出使用 UTF-8，避免在 latin-1 环境下 print 中文触发 UnicodeEncodeError
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    # 与登录/Token 同源，否则易 401（例如内网用 10.8.93.34，公网用 192.168.168.219）
    BASE_URL = os.environ.get("DOLPHIN_BASE_URL", "http://192.168.168.219:12345/dolphinscheduler").rstrip("/")
    TOKEN = os.environ.get("DOLPHIN_TOKEN", "")
    TRADEDATE = (os.environ.get("TRADEDATE") or "").strip() or date_type.today().strftime("%Y-%m-%d")
    TRADE_CALENDAR_DAY = (os.environ.get("TRADE_CALENDAR_DAY") or "20260101").strip() or "20260101"
    env_codes = (os.environ.get("DOLPHIN_PROJECT_CODES") or "").strip()
    PROJECT_CODES_ORDER = (
        [c.strip() for c in env_codes.split(",") if c.strip()]
        if env_codes
        else DEFAULT_PROJECT_CODES_ORDER
    )
    MODE = (os.environ.get("MODE") or "refresh").strip().lower()
    ENABLE_STARROCKS = os.environ.get("ENABLE_STARROCKS", "1").lower() in ("1", "true", "yes")
    SR_TABLE = os.environ.get("STARROCKS_TABLE", DEFAULT_SR_CONF["table"])

    try:
        if MODE == "rerun":
            # ---------- 只重跑：读 SR → 按项目调用重跑 ----------
            if not TOKEN:
                print("MODE=rerun 需配置 DOLPHIN_TOKEN")
                return
            client = DolphinSchedulerAPI(base_url=BASE_URL, token=TOKEN)
            store = StarRocksStore(table=SR_TABLE)
            latest_date = store.get_latest_query_date()
            if not latest_date:
                print(f"[rerun] SR 表 `{store.table}` 中没有任何 query_date 数据，无法重跑")
                return
            print(f"[rerun] 使用 SR 最新日期 {latest_date}，从 SR 读表并重跑，项目数: {len(PROJECT_CODES_ORDER)}")
            result = rerun_from_sr(
                client=client,
                store=store,
                query_date=latest_date,
                project_codes_order=PROJECT_CODES_ORDER,
                batch_size=5,
                interval_seconds=30,
            )
            for p in result["projects"]:
                line = f"- 项目[{p['project_code']}] 重跑 {p['rerun_total']}（成功 {p['rerun_success']}，失败 {p['rerun_fail']}）"
                if p.get("error"):
                    line += f"，错误: {p['error']}"
                print(line)
                # 打印失败任务的详细信息
                if p.get("rerun_fail", 0) > 0 and p.get("rerun_details"):
                    for r in p["rerun_details"]:
                        if not r.get("success"):
                            task_code = r.get('task_code', 'N/A')
                            task_name = r.get('task_name', 'N/A')
                            proc_inst = r.get('process_instance_id', 'N/A')
                            error_msg = r.get('error', '未知错误')
                            print(f"    × taskCode={task_code} (processInstanceId={proc_inst}, 任务名={task_name}): {error_msg}")
            return

        # ---------- 只落表：拉 Dolphin → 写 SR ----------
        if not TOKEN:
            print("MODE=refresh 需配置 DOLPHIN_TOKEN 以调用 Dolphin 接口")
            return
        client = DolphinSchedulerAPI(base_url=BASE_URL, token=TOKEN)
        store = StarRocksStore(table=SR_TABLE) if ENABLE_STARROCKS else None
        if not store:
            print("ENABLE_STARROCKS 已关闭，仅打印失败任务，不落库。")
            all_failed = []
            for project_code in PROJECT_CODES_ORDER:
                try:
                    # 未落库时，仍用计算后的 query_date（由 tradedate 推导）
                    query_date = date_type.today().strftime("%Y-%m-%d")
                    tasks = client.query_failed_tasks_one_per_logical_task(project_code=project_code, date=query_date)
                    all_failed.extend(tasks)
                except Exception as e:
                    print(f"  项目 {project_code} 异常: {e}")
            print(f"合计失败 {len(all_failed)} 条")
            return
        # refresh：先通过 tradedate 表计算 query_date
        query_date = store.resolve_query_date_from_tradedate(tradedate=TRADEDATE, calendar_day=TRADE_CALENDAR_DAY)
        if not query_date:
            raise Exception(
                f"无法通过交易日表解析 query_date（TRADEDATE={TRADEDATE!r}, TRADE_CALENDAR_DAY={TRADE_CALENDAR_DAY!r}）。"
                f"请确认 hive_catalog.ods.o_sd_thk_fxckhdata_t_pub_tradedate 可查询且 tradedate 范围覆盖。"
            )
        print(f"[refresh] TRADEDATE={TRADEDATE} -> query_date={query_date}，拉取失败任务并落表，项目数: {len(PROJECT_CODES_ORDER)}")
        summary = sync_failed_tasks_for_projects(
            client=client,
            store=store,
            query_date=query_date,
            project_codes_order=PROJECT_CODES_ORDER,
            auto_rerun=False,
            batch_size=5,
            interval_seconds=30,
        )
        print(f"日期 {summary['date']}，共失败任务 {summary['total_failed']} 条")
        for p in summary["projects"]:
            line = f"- 项目[{p['project_code']}] {p['project_name']} 失败 {p['failed_count']}"
            if p.get("error"):
                line += f"，错误: {p['error']}"
            print(line)
        print(f"已落地到 StarRocks 表: {store.table} （库: {store.database}）")
    except Exception as e:
        print(f"错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
