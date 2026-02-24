import { useState, useEffect, useCallback } from 'react'
import * as XLSX from 'xlsx'

// 挂载在统一门户时为 /sr_api；单独起后端时用 ''。后端可注入 window.__API_BASE__，无需重新构建
const API_BASE =
  (typeof window !== 'undefined' && (window as { __API_BASE__?: string }).__API_BASE__) ||
  (typeof import.meta.env.VITE_BASE === 'string' ? import.meta.env.VITE_BASE.replace(/\/$/, '') : '')
const PAGE_SIZE = 20
const FETCH_TIMEOUT_MS = 8000
const DAILY_STATS_TABLE = 'mv_scrm_open_channel_tag_stats'

/** 带超时的 fetch，避免内网接口无响应时长期占用连接导致另一项挂死 */
async function fetchWithTimeout(url: string, options: RequestInit = {}, timeoutMs = FETCH_TIMEOUT_MS): Promise<Response> {
  const ctrl = new AbortController()
  const id = setTimeout(() => ctrl.abort(), timeoutMs)
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal })
    return res
  } finally {
    clearTimeout(id)
  }
}

// 不展示的列
const HIDDEN_COLUMNS = ['external_id', 'tag_id', 'group_id']

// 列英文 -> 中文
const COLUMN_LABELS: Record<string, string> = {
  dt: '日期',
  open_channel: '开户渠道',
  wechat_customer_tag: '客户标签',
  total_add_cnt: '总加微数',
  chengdu_add_cnt: '成都加微数',
  yunfen_add_cnt: '云分加微数',
  zhefen_add_cnt: '浙分加微数',
  haifen_add_cnt: '海分加微数',
  shujin_add_cnt: '数金加微数',
  douyin_use_amt: '抖音消耗值',
  name: '客户微信昵称',
  remark: '备注',
  user_id: '员工ID',
  user_name: '员工姓名',
  add_time: '添加时间',
  tag_name: '标签名称',
  group_name: '标签组名称',
}

function getDisplayColumns(keys: string[]) {
  return keys.filter((k) => !HIDDEN_COLUMNS.includes(k))
}

function getColumnLabel(key: string) {
  const k = key.trim()
  const direct = COLUMN_LABELS[k] ?? COLUMN_LABELS[k.toLowerCase()]
  if (direct) return direct
  // 兼容后端返回不同大小写（如 StarRocks 列名）
  const lower = k.toLowerCase()
  if (lower === 'shujin_add_cnt') return '数金加微数'
  if (lower === 'douyin_use_amt') return '抖音消耗值'
  return k
}

interface TableDataResponse {
  table: string
  count: number
  data: Record<string, unknown>[]
}

const DEFAULT_TAG = '官方直播开户-加企微'

/** 投流渠道承接员工：允许的营业部（仅支持这五个，请改为实际名称） */
const CHANNEL_STAFF_ALLOWED_BRANCHES = [
  '数金',
  '成都',
  '云分',
  '浙分',
  '海分',
]

// -------------------- 配置模块类型定义 --------------------

interface OpenChannelTagItem {
  id: number
  open_channel: string
  wechat_customer_tag: string
  created_at?: string
  updated_at?: string
}

interface ChannelStaffItem {
  id: number
  branch_name: string
  staff_name: string
  created_at?: string
  updated_at?: string
}

interface CodeMappingItem {
  id: number
  code_value: string
  description?: string | null
  stat_cost?: number | null
  channel_name?: string | null
  created_time?: string | null
}

type ConfigTab = 'open_channel_tag' | 'channel_staff' | 'code_mapping'

type ViewMode = 'realtime' | 'open_channel_daily' | 'config'

const CONFIG_TAB_LABEL: Record<ConfigTab, string> = {
  open_channel_tag: '开户渠道 & 企微客户标签',
  channel_staff: '投流渠道承接员工',
  code_mapping: '抖音广告主体渠道',
}

function getViewFromLocation(): ViewMode {
  try {
    const v = new URLSearchParams(window.location.search).get('view')
    if (v === 'config') return 'config'
    if (v === 'open_channel_daily') return 'open_channel_daily'
    return 'realtime'
  } catch {
    return 'realtime'
  }
}

function getConfigTabFromLocation(): ConfigTab {
  try {
    const tab = new URLSearchParams(window.location.search).get('tab')
    if (tab === 'code_mapping') return 'code_mapping'
    if (tab === 'channel_staff') return 'channel_staff'
    return 'open_channel_tag'
  } catch {
    return 'open_channel_tag'
  }
}

function App() {
  const [view] = useState<ViewMode>(() => getViewFromLocation())
  const isConfigMode = view === 'config'
  const isDailyStatsMode = view === 'open_channel_daily'

  // -------------------- 物化视图数据查看 --------------------
  const [selectedTable, setSelectedTable] = useState<string>('')
  const [data, setData] = useState<Record<string, unknown>[]>([])
  const tagNameFilter = DEFAULT_TAG
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  // 自营渠道页：仅日期选择。'' 表示默认今天+昨天，非空为选中日；dailyStatsShowAll 为 true 时请求全部日期
  const [dailyStatsDate, setDailyStatsDate] = useState<string>('')
  const [dailyStatsShowAll, setDailyStatsShowAll] = useState(false)

  const fetchTables = useCallback(async () => {
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/tables`)
      if (!res.ok) throw new Error('获取表列表失败')
      const list: string[] = await res.json()
      if (list.length > 0) {
        setSelectedTable(list[0])
      }
    } catch (e) {
      setError(e instanceof Error ? (e.message === 'The operation was aborted.' ? '请求超时，请检查网络或稍后重试' : e.message) : '未知错误')
    }
  }, [])

  const fetchData = useCallback(
    async (opts?: { limit?: number; silent?: boolean }) => {
      if (!selectedTable) return
      const limit = opts?.limit ?? 5000
      const silent = opts?.silent ?? false
      if (!silent) {
        setLoading(true)
        setError(null)
      }
      try {
        const params = new URLSearchParams({ limit: String(limit) })
        // 实时加微监测：按标签过滤
        if (!isDailyStatsMode && tagNameFilter && tagNameFilter.trim()) {
          params.set('tag_name', tagNameFilter.trim())
        }
        // 自营渠道页：日期筛选。不传或空 = 今天+昨天；选某天 = 该天；全部 = dt_all
        if (isDailyStatsMode) {
          if (dailyStatsShowAll) {
            params.set('dt_all', '1')
          } else if (dailyStatsDate.trim()) {
            params.set('dt_from', dailyStatsDate.trim())
            params.set('dt_to', dailyStatsDate.trim())
          }
          // dailyStatsDate 为空时不传 dt，后端默认今天+昨天
        }
        const res = await fetchWithTimeout(
          `${API_BASE}/api/data/${encodeURIComponent(selectedTable)}?${params}`
        )
        if (!res.ok) throw new Error(await res.text())
        const json: TableDataResponse = await res.json()
        const toTime = (v: unknown) => {
          if (typeof v === 'number') return v
          if (typeof v === 'string') {
            const t = Date.parse(v)
            return Number.isFinite(t) ? t : 0
          }
          return 0
        }
        const sorted = [...json.data].sort((a, b) => {
          const ta = toTime(a['add_time'] ?? a['dt'])
          const tb = toTime(b['add_time'] ?? b['dt'])
          return tb - ta
        })
        setData(sorted)
        setCurrentPage(1)
        setLastRefresh(new Date())
      } catch (e) {
        const msg = e instanceof Error ? e.message : '未知错误'
        setError(msg === 'The operation was aborted.' ? '请求超时，请检查网络或稍后重试' : msg)
      } finally {
        if (!silent) setLoading(false)
      }
    },
    [selectedTable, isDailyStatsMode, tagNameFilter, dailyStatsDate, dailyStatsShowAll]
  )

  useEffect(() => {
    if (view === 'realtime') {
      fetchTables()
    } else if (view === 'open_channel_daily') {
      setSelectedTable(DAILY_STATS_TABLE)
    }
  }, [view, fetchTables])

  useEffect(() => {
    if (!selectedTable) return
    if (isConfigMode) return

    if (isDailyStatsMode) {
      // 自营渠道：一次请求拿全量（今天+昨天），不先展示部分再替换，避免空白/闪烁
      void fetchData({ limit: 5000 })
    } else {
      // 实时加微：先加载一部分再后台拉全量
      ;(async () => {
        await fetchData({ limit: PAGE_SIZE * 2 })
        void fetchData({ limit: 5000, silent: true })
      })()
    }

    const timer = setInterval(() => {
      void fetchData({ limit: 5000 })
    }, 5 * 60 * 1000)
    return () => clearInterval(timer)
  }, [selectedTable, isConfigMode, isDailyStatsMode, fetchData])

  const columns = data.length > 0 ? getDisplayColumns(Object.keys(data[0])) : []
  const totalCount = data.length
  const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE))
  const pageData = data.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE)

  const downloadExcel = () => {
    if (data.length === 0) return
    const headers = columns.map((c) => getColumnLabel(c))
    const rows = data.map((row) =>
      columns.map((col) => String(row[col] ?? ''))
    )
    const wsData = [headers, ...rows]
    const ws = XLSX.utils.aoa_to_sheet(wsData)
    const wb = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(wb, ws, '数据')
    const name = `数据导出_${new Date().toISOString().slice(0, 10)}.xlsx`
    XLSX.writeFile(wb, name)
  }

  // -------------------- 配置界面：状态 --------------------
  const [configTab] = useState<ConfigTab>(() => getConfigTabFromLocation())
  const [openChannelItems, setOpenChannelItems] = useState<OpenChannelTagItem[]>([])
  const [channelStaffItems, setChannelStaffItems] = useState<ChannelStaffItem[]>([])
  const [configLoading, setConfigLoading] = useState(false)
  const [configError, setConfigError] = useState<string | null>(null)

  const [editingOpenItem, setEditingOpenItem] = useState<OpenChannelTagItem | null>(null)
  const [openForm, setOpenForm] = useState<{ open_channel: string; wechat_customer_tag: string }>({
    open_channel: '',
    wechat_customer_tag: '',
  })
  const [openModal, setOpenModal] = useState<'add' | 'edit' | null>(null)

  const [editingStaffItem, setEditingStaffItem] = useState<ChannelStaffItem | null>(null)
  const [staffForm, setStaffForm] = useState<{ branch_name: string; staff_name: string }>({
    branch_name: '',
    staff_name: '',
  })
  const [staffModal, setStaffModal] = useState<'add' | 'edit' | null>(null)
  const [staffModalError, setStaffModalError] = useState<string | null>(null)

  // code_mapping（新增在业务配置下方）
  const [codeMappingItems, setCodeMappingItems] = useState<CodeMappingItem[]>([])
  const [codeMappingLoading, setCodeMappingLoading] = useState(false)
  const [codeMappingError, setCodeMappingError] = useState<string | null>(null)
  const [editingCodeMapping, setEditingCodeMapping] = useState<CodeMappingItem | null>(null)
  const [codeMappingModal, setCodeMappingModal] = useState<'add' | 'edit' | null>(null)
  const [codeMappingForm, setCodeMappingForm] = useState<{
    id: string
    code_value: string
    description: string
    stat_cost: string
    channel_name: string
  }>({
    id: '',
    code_value: '',
    description: '',
    stat_cost: '',
    channel_name: '',
  })

  // -------------------- 配置界面：请求 --------------------

  const loadOpenChannelTags = useCallback(async () => {
    setConfigLoading(true)
    setConfigError(null)
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/open-channel-tags`)
      if (!res.ok) throw new Error(await res.text())
      const list: OpenChannelTagItem[] = await res.json()
      setOpenChannelItems(list)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载开户渠道配置失败'
      const display = msg === 'The operation was aborted.' || msg.includes('fetch') || msg.includes('Failed')
        ? '请求超时或网络不可达，请检查内网连接'
        : msg
      setConfigError(display)
    } finally {
      setConfigLoading(false)
    }
  }, [])

  const loadChannelStaff = useCallback(async () => {
    setConfigLoading(true)
    setConfigError(null)
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/channel-staff`)
      if (!res.ok) throw new Error(await res.text())
      const list: ChannelStaffItem[] = await res.json()
      setChannelStaffItems(list)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载承接员工配置失败'
      const display = msg === 'The operation was aborted.' || msg.includes('fetch') || msg.includes('Failed')
        ? '请求超时或网络不可达，请检查内网连接'
        : msg
      setConfigError(display)
    } finally {
      setConfigLoading(false)
    }
  }, [])

  const loadCodeMapping = useCallback(async () => {
    setCodeMappingLoading(true)
    setCodeMappingError(null)
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/code-mapping`)
      if (!res.ok) throw new Error(await res.text())
      const list: CodeMappingItem[] = await res.json()
      setCodeMappingItems(list)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载 code_mapping 失败'
      const display = msg === 'The operation was aborted.' || msg.includes('fetch') || msg.includes('Failed')
        ? '请求超时或网络不可达，请检查内网连接'
        : msg
      setCodeMappingError(display)
    } finally {
      setCodeMappingLoading(false)
    }
  }, [])

  const refreshCurrentConfig = () => {
    if (configTab === 'open_channel_tag') {
      void loadOpenChannelTags()
    } else {
      void loadChannelStaff()
    }
  }

  // 配置界面数据加载：进入配置视图时加载一次，切换 Tab 时按需加载
  useEffect(() => {
    if (!isConfigMode) return
    if (configTab === 'open_channel_tag') {
      void loadOpenChannelTags()
    } else {
      void loadChannelStaff()
    }
  }, [isConfigMode, configTab, loadOpenChannelTags, loadChannelStaff])

  useEffect(() => {
    if (!isConfigMode) return
    if (configTab !== 'code_mapping') return
    void loadCodeMapping()
  }, [isConfigMode, configTab, loadCodeMapping])

  const openAddCodeMapping = () => {
    setEditingCodeMapping(null)
    setCodeMappingForm({ id: '', code_value: '', description: '', stat_cost: '', channel_name: '' })
    setCodeMappingModal('add')
  }

  const openEditCodeMapping = (item: CodeMappingItem) => {
    setEditingCodeMapping(item)
    setCodeMappingForm({
      id: String(item.id),
      code_value: item.code_value ?? '',
      description: String(item.description ?? ''),
      stat_cost: item.stat_cost == null ? '' : String(item.stat_cost),
      channel_name: String(item.channel_name ?? ''),
    })
    setCodeMappingModal('edit')
  }

  const saveCodeMapping = async () => {
    setCodeMappingError(null)
    const isEdit = codeMappingModal === 'edit' && !!editingCodeMapping

    // 新增时：校验广告主体 id
    let idNum: number | undefined
    if (!isEdit) {
      idNum = Number(codeMappingForm.id)
      if (!Number.isFinite(idNum) || idNum <= 0) {
        setCodeMappingError('广告主体id 需要为正整数')
        return
      }
    }

    // 新增 & 修改：都必须填写渠道名称
    if (!codeMappingForm.channel_name.trim()) {
      setCodeMappingError('渠道为必填项')
      return
    }

    const payload: Record<string, unknown> = {
      code_value: codeMappingForm.code_value.trim()
        ? codeMappingForm.code_value.trim()
        : null,
      description: codeMappingForm.description.trim()
        ? codeMappingForm.description.trim()
        : null,
      channel_name: codeMappingForm.channel_name.trim(),
    }
    if (codeMappingForm.stat_cost.trim()) {
      const v = Number(codeMappingForm.stat_cost)
      if (!Number.isFinite(v)) {
        setCodeMappingError('stat_cost 必须为数字')
        return
      }
      payload.stat_cost = v
    } else {
      payload.stat_cost = null
    }
    try {
      const url = isEdit
        ? `${API_BASE}/api/config/code-mapping/${editingCodeMapping!.id}`
        : `${API_BASE}/api/config/code-mapping`
      const method = isEdit ? 'PUT' : 'POST'
      const body = isEdit ? payload : { id: idNum!, ...payload }
      const res = await fetchWithTimeout(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) throw new Error(await res.text())
      setCodeMappingModal(null)
      setEditingCodeMapping(null)
      await loadCodeMapping()
    } catch (e) {
      const msg = e instanceof Error ? e.message : '保存失败'
      setCodeMappingError(msg)
    }
  }

  const deleteCodeMapping = async (item: CodeMappingItem) => {
    if (!window.confirm(`确定删除 code_mapping: ${item.id} 吗？`)) return
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/code-mapping/${item.id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error(await res.text())
      await loadCodeMapping()
    } catch (e) {
      const msg = e instanceof Error ? e.message : '删除失败'
      setCodeMappingError(msg)
    }
  }

  // 新增 / 保存：开户渠道 & 标签
  const handleSaveOpenChannel = async () => {
    if (!openForm.open_channel.trim() || !openForm.wechat_customer_tag.trim()) {
      setConfigError('开户渠道和企微客户标签不能为空')
      return
    }
    setConfigError(null)
    try {
      const isEdit = !!editingOpenItem
      const url = isEdit
        ? `${API_BASE}/api/config/open-channel-tags/${editingOpenItem!.id}`
        : `${API_BASE}/api/config/open-channel-tags`
      const method = isEdit ? 'PUT' : 'POST'
      const body =
        method === 'PUT'
          ? { wechat_customer_tag: openForm.wechat_customer_tag.trim() }
          : {
              open_channel: openForm.open_channel.trim(),
              wechat_customer_tag: openForm.wechat_customer_tag.trim(),
            }
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) throw new Error(await res.text())
      await refreshCurrentConfig()
      setEditingOpenItem(null)
      setOpenForm({ open_channel: '', wechat_customer_tag: '' })
      setOpenModal(null)
    } catch (e) {
      setConfigError(e instanceof Error ? e.message : '保存失败')
    }
  }

  const handleEditOpenChannel = (item: OpenChannelTagItem) => {
    setEditingOpenItem(item)
    setOpenForm({
      open_channel: item.open_channel,
      wechat_customer_tag: item.wechat_customer_tag,
    })
    setOpenModal('edit')
  }

  const handleDeleteOpenChannel = async (item: OpenChannelTagItem) => {
    if (!window.confirm(`确定删除【${item.open_channel} - ${item.wechat_customer_tag}】这条配置吗？`)) {
      return
    }
    setConfigError(null)
    try {
      const res = await fetch(`${API_BASE}/api/config/open-channel-tags/${item.id}`, {
        method: 'DELETE',
      })
      if (!res.ok) throw new Error(await res.text())
      // 删除成功后刷新页面，避免显示"没有该条记录"等错误
      window.location.reload()
    } catch (e) {
      // 删除失败（如记录已不存在）时也刷新页面，不显示错误
      window.location.reload()
    }
  }

  // 新增 / 保存：投流渠道承接员工
  const handleSaveStaff = async () => {
    if (!staffForm.branch_name.trim() || !staffForm.staff_name.trim()) {
      setConfigError('营业部和姓名不能为空')
      return
    }
    if (!CHANNEL_STAFF_ALLOWED_BRANCHES.includes(staffForm.branch_name.trim())) {
      setStaffModalError('保存失败，请确认是否有该营业部，联系管理员添加。')
      return
    }
    setStaffModalError(null)
    setConfigError(null)
    try {
      const isEdit = !!editingStaffItem
      const url = isEdit
        ? `${API_BASE}/api/config/channel-staff/${editingStaffItem!.id}`
        : `${API_BASE}/api/config/channel-staff`
      const method = isEdit ? 'PUT' : 'POST'
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          branch_name: staffForm.branch_name.trim(),
          staff_name: staffForm.staff_name.trim(),
        }),
      })
      if (!res.ok) throw new Error(await res.text())
      await refreshCurrentConfig()
      setEditingStaffItem(null)
      setStaffForm({ branch_name: '', staff_name: '' })
      setStaffModal(null)
      setStaffModalError(null)
    } catch (e) {
      setStaffModalError(e instanceof Error ? e.message : '保存失败')
    }
  }

  const handleEditStaff = (item: ChannelStaffItem) => {
    setEditingStaffItem(item)
    setStaffForm({
      branch_name: item.branch_name,
      staff_name: item.staff_name,
    })
    setStaffModalError(null)
    setStaffModal('edit')
  }

  const handleDeleteStaff = async (item: ChannelStaffItem) => {
    if (!window.confirm(`确定删除【${item.branch_name} - ${item.staff_name}】这条配置吗？`)) {
      return
    }
    setConfigError(null)
    try {
      const res = await fetch(`${API_BASE}/api/config/channel-staff/${item.id}`, {
        method: 'DELETE',
      })
      if (!res.ok) throw new Error(await res.text())
      // 删除成功后刷新页面，避免显示"没有该条记录"等错误
      window.location.reload()
    } catch (e) {
      // 删除失败（如记录已不存在）时也刷新页面，不显示错误
      window.location.reload()
    }
  }

  // -------------------- 渲染：顶部导航 --------------------

  return (
    <div className="min-h-screen bg-white text-slate-800 font-sans">
      <header className="border-b border-slate-200 bg-white/95 backdrop-blur sticky top-0 z-10">
        <div className="max-w-[1600px] mx-auto px-6 py-4 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h1 className="text-xl font-semibold text-sky-600">
              StarRocks 业务应用
            </h1>
            {!isConfigMode ? (
              <>
                <p className="text-sm text-slate-500 mt-0.5">
                  {isDailyStatsMode ? '自营渠道的每日加微数据' : '物化视图 · 近实时数据'} · 每 5 分钟自动刷新 · 上次刷新时间:{' '}
                  {lastRefresh ? lastRefresh.toLocaleTimeString('zh-CN') : '-'}
                </p>
                {selectedTable && (
                  <p className="text-base font-medium text-amber-600 mt-1">
                    共 {totalCount} 条记录
                  </p>
                )}
              </>
            ) : (
              <p className="text-sm text-slate-500 mt-0.5">
                配置界面 · {CONFIG_TAB_LABEL[configTab]}
              </p>
            )}
          </div>
        </div>
      </header>

      <main className="max-w-[1600px] mx-auto px-6 py-6">
        {/* 内容区域（左侧菜单移交给中台门户，这里只渲染当前页面） */}
        <section className="min-w-0">
            {!isConfigMode ? (
              <>
            {error && (
              <div className="mb-4 p-4 rounded-lg bg-red-50 border border-red-200 text-red-700">
                {error}
              </div>
            )}

            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              {!isDailyStatsMode ? (
                <div className="flex items-center gap-2">
                  <span className="text-sm text-slate-500 whitespace-nowrap">当前标签:</span>
                  <span className="px-3 py-1.5 rounded-lg bg-slate-100 border border-slate-300 text-slate-700 text-sm">
                    {DEFAULT_TAG}
                  </span>
                </div>
              ) : (
                <div className="flex items-center gap-3 flex-wrap items-center">
                  <label className="text-sm font-medium text-slate-600 whitespace-nowrap">选择日期</label>
                  <input
                    type="date"
                    value={dailyStatsDate}
                    onChange={(e) => {
                      setDailyStatsDate(e.target.value)
                      setDailyStatsShowAll(false)
                    }}
                    className="px-3 py-2 rounded-lg border border-slate-300 text-slate-700 text-sm bg-white"
                  />
                  <button
                    type="button"
                    onClick={() => {
                      setDailyStatsShowAll(true)
                      setDailyStatsDate('')
                    }}
                    className={`text-sm py-1 px-2 rounded ${dailyStatsShowAll ? 'bg-slate-200 font-medium text-slate-800' : 'text-slate-600 hover:text-sky-600'}`}
                  >
                    全部日期
                  </button>
                </div>
              )}
              <div className="flex items-center gap-3">
                <button
                  onClick={() => void fetchData({ limit: 5000 })}
                  disabled={loading}
                  className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 disabled:opacity-50 disabled:cursor-not-allowed text-white font-medium transition-colors"
                >
                  {loading ? '加载中...' : '刷新'}
                </button>
                <button
                  onClick={downloadExcel}
                  disabled={data.length === 0}
                  className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-800 font-medium transition-colors"
                  title="导出全部数据"
                >
                  导出 Excel（全部）
                </button>
              </div>
            </div>

            <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
              <div className="overflow-x-auto">
                {loading && data.length === 0 ? (
                  <div className="py-20 text-center text-slate-500">加载中...</div>
                ) : data.length === 0 ? (
                  <div className="py-20 text-center text-slate-500">
                    {selectedTable ? '暂无数据' : '加载中...'}
                  </div>
                ) : (
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-slate-200 bg-slate-100">
                        {columns.map((col) => (
                          <th
                            key={col}
                            className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap"
                          >
                            {getColumnLabel(col)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {pageData.map((row, i) => (
                        <tr
                          key={(currentPage - 1) * PAGE_SIZE + i}
                          className="border-b border-slate-200 hover:bg-slate-100/80 transition-colors"
                        >
                          {columns.map((col) => (
                            <td key={col} className="px-4 py-3 text-slate-700 whitespace-nowrap max-w-xs truncate">
                              {String(row[col] ?? '-')}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
              {data.length > 0 && (
                <div className="px-4 py-3 border-t border-slate-200 flex flex-wrap items-center justify-between gap-2 bg-white">
                  <span className="text-slate-600 text-sm">
                    第 {(currentPage - 1) * PAGE_SIZE + 1}-{Math.min(currentPage * PAGE_SIZE, totalCount)} 条 / 共 {totalCount} 条
                  </span>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
                      disabled={currentPage <= 1}
                      className="px-3 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-700 text-sm"
                    >
                      上一页
                    </button>
                    <span className="text-slate-500 text-sm">
                      {currentPage} / {totalPages}
                    </span>
                    <button
                      onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
                      disabled={currentPage >= totalPages}
                      className="px-3 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-700 text-sm"
                    >
                      下一页
                    </button>
                  </div>
                </div>
              )}
            </div>
              </>
            ) : (
              <>
            {configError && (
              <div className="mb-4 p-4 rounded-lg bg-red-50 border border-red-200 text-red-700">
                {configError}
              </div>
            )}

            {configTab !== 'code_mapping' && (
              <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      if (configTab === 'open_channel_tag') {
                        setEditingOpenItem(null)
                        setOpenForm({ open_channel: '', wechat_customer_tag: '' })
                        setOpenModal('add')
                      } else {
                        setEditingStaffItem(null)
                        setStaffForm({ branch_name: '', staff_name: '' })
                        setStaffModalError(null)
                        setStaffModal('add')
                      }
                    }}
                    className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium transition-colors"
                  >
                    新增
                  </button>
                  <button
                    onClick={refreshCurrentConfig}
                    disabled={configLoading}
                    className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-800 font-medium transition-colors"
                  >
                    {configLoading ? '刷新中...' : '刷新'}
                  </button>
                </div>
              </div>
            )}

            {configTab === 'open_channel_tag' ? (
              <div>
                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    {configLoading && openChannelItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">加载中...</div>
                    ) : openChannelItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                    ) : (
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-slate-200 bg-slate-100">
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              ID
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              开户渠道
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              企微客户标签
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              创建时间
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              更新时间
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              操作
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {openChannelItems.map((item) => (
                            <tr
                              key={item.id}
                              className="border-b border-slate-200 hover:bg-slate-100/80 transition-colors"
                            >
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                {item.id}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                {item.open_channel}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                {item.wechat_customer_tag}
                              </td>
                              <td className="px-4 py-3 text-slate-500 whitespace-nowrap">
                                {item.created_at ?? '-'}
                              </td>
                              <td className="px-4 py-3 text-slate-500 whitespace-nowrap">
                                {item.updated_at ?? '-'}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                <button
                                  onClick={() => handleEditOpenChannel(item)}
                                  className="mr-2 px-3 py-1 text-sky-600 hover:text-sky-700 text-xs font-medium"
                                >
                                  修改
                                </button>
                                <button
                                  onClick={() => void handleDeleteOpenChannel(item)}
                                  className="px-3 py-1 text-red-600 hover:text-red-700 text-xs font-medium"
                                >
                                  删除
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
              </div>
            ) : configTab === 'channel_staff' ? (
              <div>
                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    {configLoading && channelStaffItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">加载中...</div>
                    ) : channelStaffItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                    ) : (
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-slate-200 bg-slate-100">
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              ID
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              营业部
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              姓名
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              创建时间
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              更新时间
                            </th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">
                              操作
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {channelStaffItems.map((item) => (
                            <tr
                              key={item.id}
                              className="border-b border-slate-200 hover:bg-slate-100/80 transition-colors"
                            >
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                {item.id}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                {item.branch_name}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                {item.staff_name}
                              </td>
                              <td className="px-4 py-3 text-slate-500 whitespace-nowrap">
                                {item.created_at ?? '-'}
                              </td>
                              <td className="px-4 py-3 text-slate-500 whitespace-nowrap">
                                {item.updated_at ?? '-'}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                <button
                                  onClick={() => handleEditStaff(item)}
                                  className="mr-2 px-3 py-1 text-sky-600 hover:text-sky-700 text-xs font-medium"
                                >
                                  修改
                                </button>
                                <button
                                  onClick={() => void handleDeleteStaff(item)}
                                  className="px-3 py-1 text-red-600 hover:text-red-700 text-xs font-medium"
                                >
                                  删除
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
              </div>
            ) : null}

            {/* 追加：code_mapping 配置表（仅在抖音广告主体渠道页面展示） */}
            {configTab === 'code_mapping' && (
            <div className="mt-4">
              <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div className="text-base font-semibold text-slate-800">抖音广告主体渠道</div>
                  <div className="text-sm text-slate-500">渠道映射 / 消耗配置（来自 StarRocks 物化视图）</div>
                </div>
                <div className="flex items-center gap-3">
                  <button
                    onClick={openAddCodeMapping}
                    className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium transition-colors"
                  >
                    新增
                  </button>
                  <button
                    onClick={() => void loadCodeMapping()}
                    disabled={codeMappingLoading}
                    className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-800 font-medium transition-colors"
                  >
                    {codeMappingLoading ? '刷新中...' : '刷新'}
                  </button>
                </div>
              </div>

              {codeMappingError && (
                <div className="mb-4 p-4 rounded-lg bg-red-50 border border-red-200 text-red-700">
                  {codeMappingError}
                </div>
              )}

              <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                <div className="overflow-x-auto">
                  {codeMappingLoading && codeMappingItems.length === 0 ? (
                    <div className="py-10 text-center text-slate-500">加载中...</div>
                  ) : codeMappingItems.length === 0 ? (
                    <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                  ) : (
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-slate-200 bg-slate-100">
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">广告主体id</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">代码值</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">描述</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">消耗</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">渠道</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">创建时间</th>
                          <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {codeMappingItems.map((item) => (
                          <tr
                            key={item.id}
                            className="border-b border-slate-200 hover:bg-slate-100/80 transition-colors"
                          >
                            <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.id}</td>
                            <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.code_value}</td>
                            <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.description ?? '-'}</td>
                            <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.stat_cost ?? '-'}</td>
                            <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.channel_name ?? '-'}</td>
                            <td className="px-4 py-3 text-slate-500 whitespace-nowrap">{item.created_time ?? '-'}</td>
                            <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                              <button
                                onClick={() => openEditCodeMapping(item)}
                                className="mr-2 px-3 py-1 text-sky-600 hover:text-sky-700 text-xs font-medium"
                              >
                                修改
                              </button>
                              <button
                                onClick={() => void deleteCodeMapping(item)}
                                className="px-3 py-1 text-red-600 hover:text-red-700 text-xs font-medium"
                              >
                                删除
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </div>
            )}

            {/* code_mapping 弹窗 */}
            {codeMappingModal && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                onClick={() => {
                  setCodeMappingModal(null)
                  setEditingCodeMapping(null)
                }}
              >
                <div
                  className="w-full max-w-xl bg-white rounded-xl shadow-lg border border-slate-200 p-5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between mb-4">
                    <div className="text-base font-semibold text-slate-800">
                      {codeMappingModal === 'edit' ? '修改抖音广告主体渠道' : '新增抖音广告主体渠道'}
                    </div>
                    <button
                      onClick={() => {
                        setCodeMappingModal(null)
                        setEditingCodeMapping(null)
                      }}
                      className="text-slate-500 hover:text-slate-700"
                      aria-label="关闭"
                    >
                      ✕
                    </button>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <div>
                      <div className="text-sm text-slate-600 mb-1">广告主体id（必填）</div>
                      <input
                        value={codeMappingForm.id}
                        onChange={(e) => setCodeMappingForm((p) => ({ ...p, id: e.target.value }))}
                        disabled={codeMappingModal === 'edit'}
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm disabled:bg-slate-100"
                        placeholder="例如 advertiser_id"
                      />
                    </div>
                    <div>
                      <div className="text-sm text-slate-600 mb-1">代码值（可选）</div>
                      <input
                        value={codeMappingForm.code_value}
                        onChange={(e) => setCodeMappingForm((p) => ({ ...p, code_value: e.target.value }))}
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm"
                        placeholder="如计划 ID / 单元 ID 等"
                      />
                    </div>
                    <div className="md:col-span-2">
                      <div className="text-sm text-slate-600 mb-1">描述（可选）</div>
                      <input
                        value={codeMappingForm.description}
                        onChange={(e) => setCodeMappingForm((p) => ({ ...p, description: e.target.value }))}
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm"
                        placeholder="可选"
                      />
                    </div>
                    <div>
                      <div className="text-sm text-slate-600 mb-1">消耗（可选，数字）</div>
                      <input
                        value={codeMappingForm.stat_cost}
                        onChange={(e) => setCodeMappingForm((p) => ({ ...p, stat_cost: e.target.value }))}
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm"
                        placeholder="例如对应抖音消耗金额"
                      />
                    </div>
                    <div>
                      <div className="text-sm text-slate-600 mb-1">渠道（必填）</div>
                      <input
                        value={codeMappingForm.channel_name}
                        onChange={(e) => setCodeMappingForm((p) => ({ ...p, channel_name: e.target.value }))}
                        className="w-full px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm"
                        placeholder="例如：抖音信息流-自建"
                      />
                    </div>
                  </div>

                  <div className="mt-4 flex items-center justify-end gap-2">
                    <button
                      onClick={() => {
                        setCodeMappingModal(null)
                        setEditingCodeMapping(null)
                      }}
                      className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 font-medium transition-colors"
                    >
                      取消
                    </button>
                    <button
                      onClick={() => void saveCodeMapping()}
                      className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium transition-colors"
                    >
                      保存
                    </button>
                  </div>
                </div>
              </div>
            )}

            {/* 开户渠道 & 企微客户标签 弹窗 */}
            {openModal && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                onClick={() => {
                  setOpenModal(null)
                  setEditingOpenItem(null)
                  setOpenForm({ open_channel: '', wechat_customer_tag: '' })
                }}
              >
                <div
                  className="rounded-xl border border-slate-200 bg-white p-4 shadow-xl w-full max-w-md mx-4"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h2 className="text-sm font-semibold text-slate-800 mb-3">
                    {openModal === 'edit' ? '修改配置' : '新增配置'} · 开户渠道 & 企微客户标签
                  </h2>
                  <div className="space-y-3">
                    <div
                      className={`flex flex-col gap-0.5 ${openModal === 'edit' ? 'cursor-not-allowed' : ''}`}
                    >
                      <span className="text-[11px] text-slate-500">开户渠道</span>
                      <input
                        readOnly={openModal === 'edit'}
                        tabIndex={openModal === 'edit' ? -1 : 0}
                        className={`px-2.5 py-1.5 rounded border text-xs focus:outline-none focus:ring-2 focus:ring-sky-500 ${
                          openModal === 'edit'
                            ? 'bg-slate-100 border-slate-300 text-slate-500 pointer-events-none'
                            : 'bg-slate-50 border-slate-300 text-slate-800'
                        }`}
                        value={openForm.open_channel}
                        onChange={(e) =>
                          setOpenForm((prev) => ({ ...prev, open_channel: e.target.value }))
                        }
                        placeholder="请输入开户渠道"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">企微客户标签</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={openForm.wechat_customer_tag}
                        onChange={(e) =>
                          setOpenForm((prev) => ({ ...prev, wechat_customer_tag: e.target.value }))
                        }
                        placeholder="请输入企微客户标签"
                      />
                    </div>
                  </div>
                  <div className="flex gap-2 mt-4 justify-end">
                    <button
                      onClick={() => {
                        setOpenModal(null)
                        setEditingOpenItem(null)
                        setOpenForm({ open_channel: '', wechat_customer_tag: '' })
                      }}
                      className="px-3 py-1.5 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-xs font-medium"
                    >
                      取消
                    </button>
                    <button
                      onClick={() => void handleSaveOpenChannel()}
                      className="px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-medium"
                    >
                      保存
                    </button>
                  </div>
                </div>
              </div>
            )}

            {/* 投流渠道承接员工 弹窗 */}
            {staffModal && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                onClick={() => {
                  setStaffModal(null)
                  setEditingStaffItem(null)
                  setStaffForm({ branch_name: '', staff_name: '' })
                  setStaffModalError(null)
                }}
              >
                <div
                  className="rounded-xl border border-slate-200 bg-white p-4 shadow-xl w-full max-w-md mx-4"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h2 className="text-sm font-semibold text-slate-800 mb-3">
                    {staffModal === 'edit' ? '修改配置' : '新增配置'} · 投流渠道承接员工
                  </h2>
                  <div className="space-y-3">
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">营业部</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500 placeholder:text-slate-400"
                        value={staffForm.branch_name}
                        onChange={(e) =>
                          setStaffForm((prev) => ({ ...prev, branch_name: e.target.value }))
                        }
                        placeholder="请输入营业部（目前仅支持：数金，成都，云分，浙分，海分）"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">姓名</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={staffForm.staff_name}
                        onChange={(e) =>
                          setStaffForm((prev) => ({ ...prev, staff_name: e.target.value }))
                        }
                        placeholder="请输入姓名"
                      />
                    </div>
                  </div>
                  {staffModalError && (
                    <p className="mt-3 text-xs text-red-700 rounded-lg bg-red-50 border border-red-200 px-2.5 py-1.5">
                      {staffModalError}
                    </p>
                  )}
                  <div className="flex gap-2 mt-4 justify-end">
                    <button
                      onClick={() => {
                        setStaffModal(null)
                        setEditingStaffItem(null)
                        setStaffForm({ branch_name: '', staff_name: '' })
                        setStaffModalError(null)
                      }}
                      className="px-3 py-1.5 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-xs font-medium"
                    >
                      取消
                    </button>
                    <button
                      onClick={() => void handleSaveStaff()}
                      className="px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-medium"
                    >
                      保存
                    </button>
                  </div>
                </div>
              </div>
            )}
              </>
            )}
        </section>
      </main>
    </div>
  )
}

export default App
