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

interface StockPositionItem {
  id: number
  product_name?: string | null
  trade_date?: string | null
  stock_code?: string | null
  stock_name?: string | null
  position_pct?: number | null
  side?: string | null
  price?: number | null
  created_at?: string | null
  updated_at?: string | null
}

interface NavChartPoint {
  date: string
  nav: number
  hs300_nav?: number | null
}

type ConfigTab = 'open_channel_tag' | 'channel_staff' | 'code_mapping' | 'stock_position'

type ViewMode = 'realtime' | 'open_channel_daily' | 'config'

const CONFIG_TAB_LABEL: Record<ConfigTab, string> = {
  open_channel_tag: '开户渠道 & 企微客户标签',
  channel_staff: '投流渠道承接员工',
  code_mapping: '[市场中心] 抖音投流账号',
  stock_position: '[投顾中心] 产品净值',
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
    if (tab === 'stock_position') return 'stock_position'
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
  const isConfigReadOnly = (() => {
    try {
      const p = new URLSearchParams(window.location.search)
      const ro = p.get('readonly')
      if (ro === '1' || ro === 'true') return true
      const role = p.get('role')
      if (role && role.toLowerCase() === 'readonly') return true
      const w = window as unknown as { __READ_ONLY__?: boolean }
      return w.__READ_ONLY__ === true
    } catch {
      return false
    }
  })()

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

  // 股票仓位/买卖配置
  const STOCK_POSITION_PAGE_SIZE = 30
  const [stockPositionItems, setStockPositionItems] = useState<StockPositionItem[]>([])
  const [stockPositionTotal, setStockPositionTotal] = useState(0)
  const [stockPositionPage, setStockPositionPage] = useState(1)
  const [stockPositionProducts, setStockPositionProducts] = useState<string[]>([])
  const [stockPositionFilter, setStockPositionFilter] = useState<string>('短线王')
  const [stockPositionLoading, setStockPositionLoading] = useState(false)
  const [stockPositionError, setStockPositionError] = useState<string | null>(null)

  // 净值图（弹窗）
  const [navModalOpen, setNavModalOpen] = useState(false)
  const [navChartLoading, setNavChartLoading] = useState(false)
  const [navChartError, setNavChartError] = useState<string | null>(null)
  const [navSeries, setNavSeries] = useState<NavChartPoint[]>([])
  const [navHoverIndex, setNavHoverIndex] = useState<number | null>(null)
  const [navStartDate, setNavStartDate] = useState<string>(() => {
    const d = new Date()
    d.setDate(d.getDate() - 90)
    return d.toISOString().slice(0, 10)
  })
  const [navEndDate, setNavEndDate] = useState<string>(() => new Date().toISOString().slice(0, 10))
  const [navZoomMode, setNavZoomMode] = useState<'30d' | '90d' | 'all'>('90d')

  const getFirstProductName = useCallback((v: string) => {
    const first = v
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)[0]
    return first || '短线王'
  }, [])
  const [stockPositionNewRow, setStockPositionNewRow] = useState<{
    product_name: string
    trade_date: string
    stock_code: string
    stock_name: string
    position_pct: string
    side: string
    price: string
  }>({
    product_name: '短线王',
    trade_date: '',
    stock_code: '',
    stock_name: '',
    position_pct: '',
    side: '买入',
    price: '',
  })
  const [editingStockPositionId, setEditingStockPositionId] = useState<number | null>(null)
  const [editingStockPositionRow, setEditingStockPositionRow] = useState<StockPositionItem | null>(null)

  useEffect(() => {
    if (configTab !== 'stock_position') return
    const first = getFirstProductName(stockPositionFilter)
    setStockPositionNewRow((p) => ({ ...p, product_name: first || p.product_name }))
  }, [configTab, stockPositionFilter, getFirstProductName])

  // ESC 关闭净值图弹窗
  useEffect(() => {
    if (!navModalOpen) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setNavModalOpen(false)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [navModalOpen])

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

  const loadStockPositionProducts = useCallback(async () => {
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/products`)
      if (!res.ok) throw new Error(await res.text())
      const list: string[] = await res.json()
      setStockPositionProducts(list)
    } catch {
      setStockPositionProducts([])
    }
  }, [])

  const loadStockPosition = useCallback(async () => {
    setStockPositionLoading(true)
    setStockPositionError(null)
    try {
      const params = new URLSearchParams()
      if (stockPositionFilter.trim()) params.set('product_names', stockPositionFilter.trim())
      params.set('page', String(stockPositionPage))
      params.set('page_size', String(STOCK_POSITION_PAGE_SIZE))
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const data: { total: number; items: StockPositionItem[] } = await res.json()
      setStockPositionTotal(data.total)
      setStockPositionItems(data.items)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载失败'
      setStockPositionError(msg === 'The operation was aborted.' ? '请求超时' : msg)
    } finally {
      setStockPositionLoading(false)
    }
  }, [stockPositionFilter, stockPositionPage])

  const refreshCurrentConfig = () => {
    if (configTab === 'open_channel_tag') {
      void loadOpenChannelTags()
    } else if (configTab === 'stock_position') {
      void loadStockPosition()
      void loadStockPositionProducts()
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

  useEffect(() => {
    if (!isConfigMode) return
    if (configTab !== 'stock_position') return
    void loadStockPositionProducts()
  }, [isConfigMode, configTab, loadStockPositionProducts])

  useEffect(() => {
    if (!isConfigMode) return
    if (configTab !== 'stock_position') return
    void loadStockPosition()
  }, [isConfigMode, configTab, stockPositionFilter, stockPositionPage, loadStockPosition])

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
    if (isConfigReadOnly) {
      setCodeMappingError('只读账号不可修改')
      return
    }
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
    if (isConfigReadOnly) {
      setCodeMappingError('只读账号不可修改')
      return
    }
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

  const saveStockPositionNewRow = async () => {
    if (isConfigReadOnly) {
      setStockPositionError('只读账号不可修改')
      return
    }
    const r = stockPositionNewRow
    if (!r.stock_code.trim()) {
      setStockPositionError('股票代码为必填')
      return
    }
    if (!r.position_pct.trim()) {
      setStockPositionError('仓位为必填')
      return
    }
    const pct = Number(r.position_pct)
    if (!Number.isFinite(pct)) {
      setStockPositionError('仓位需为数字')
      return
    }
    if (!r.side || (r.side !== '买入' && r.side !== '卖出')) {
      setStockPositionError('请选择买入或卖出')
      return
    }
    if (!r.price.trim()) {
      setStockPositionError('成交价为必填')
      return
    }
    const price = Number(r.price)
    if (!Number.isFinite(price)) {
      setStockPositionError('成交价需为数字')
      return
    }
    setStockPositionError(null)
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          product_name: r.product_name.trim() || null,
          trade_date: r.trade_date.trim() || null,
          stock_code: r.stock_code.trim(),
          stock_name: r.stock_name.trim() || null,
          position_pct: pct,
          side: r.side,
          price,
        }),
      })
      if (!res.ok) throw new Error(await res.text())
      setStockPositionNewRow({
        product_name: r.product_name,
        trade_date: '',
        stock_code: '',
        stock_name: '',
        position_pct: '',
        side: '买入',
        price: '',
      })
      // 新增后回到第 1 页并重新拉取列表（后端 ORDER BY created_at DESC，新记录自然在最上，避免 lastrowid 不可靠导致返回错行、前端误插重复）
      setStockPositionPage(1)
      await loadStockPosition()
      await loadStockPositionProducts()
    } catch (e) {
      setStockPositionError(e instanceof Error ? e.message : '新增失败')
    }
  }

  const saveStockPositionEdit = async () => {
    if (isConfigReadOnly) {
      setStockPositionError('只读账号不可修改')
      return
    }
    if (editingStockPositionId == null || !editingStockPositionRow) return
    const r = editingStockPositionRow
    if (!r.stock_code?.trim()) {
      setStockPositionError('股票代码为必填')
      return
    }
    const pct = r.position_pct
    if (pct == null || !Number.isFinite(Number(pct))) {
      setStockPositionError('仓位需为有效数字')
      return
    }
    if (!r.side || (r.side !== '买入' && r.side !== '卖出')) {
      setStockPositionError('请选择买入或卖出')
      return
    }
    const price = r.price
    if (price == null || !Number.isFinite(Number(price))) {
      setStockPositionError('成交价需为有效数字')
      return
    }
    setStockPositionError(null)
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/${editingStockPositionId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          trade_date: r.trade_date ?? undefined,
          stock_code: r.stock_code,
          stock_name: r.stock_name ?? undefined,
          position_pct: Number(pct),
          side: r.side,
          price: Number(price),
        }),
      })
      if (!res.ok) throw new Error(await res.text())
      setEditingStockPositionId(null)
      setEditingStockPositionRow(null)
      await loadStockPosition()
    } catch (e) {
      setStockPositionError(e instanceof Error ? e.message : '保存失败')
    }
  }

  const deleteStockPosition = async (item: StockPositionItem) => {
    if (isConfigReadOnly) {
      setStockPositionError('只读账号不可修改')
      return
    }
    if (!window.confirm(`确定删除该条记录吗？`)) return
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/${item.id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error(await res.text())
      await loadStockPosition()
    } catch (e) {
      setStockPositionError(e instanceof Error ? e.message : '删除失败')
    }
  }

  const downloadStockPositionExcel = async () => {
    const productName = getFirstProductName(stockPositionFilter.trim() || '短线王')
    setStockPositionError(null)
    try {
      const params = new URLSearchParams()
      params.set('product_name', productName)
      // 后端生成双 sheet：Sheet1=仓位明细，Sheet2=净值（用于核对/绘图）
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/export.xlsx?${params}`, {}, 60000)
      if (!res.ok) throw new Error(await res.text())

      const cd = res.headers.get('content-disposition')
      const pickName = () => {
        if (!cd) return null
        const mStar = /filename\*\s*=\s*UTF-8''([^;]+)/i.exec(cd)
        if (mStar?.[1]) {
          try {
            return decodeURIComponent(mStar[1])
          } catch {
            return mStar[1]
          }
        }
        const m = /filename\s*=\s*"?([^";]+)"?/i.exec(cd)
        return m?.[1] ?? null
      }

      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = pickName() || `${productName}_仓位+净值.xlsx`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e) {
      setStockPositionError(e instanceof Error ? e.message : '导出失败')
    }
  }

  const loadNavSeries = useCallback(async () => {
    const productName = getFirstProductName(stockPositionFilter)
    setNavChartLoading(true)
    setNavChartError(null)
    try {
      const params = new URLSearchParams()
      params.set('product_name', productName)
      if (navStartDate.trim()) params.set('start_date', navStartDate.trim())
      if (navEndDate.trim()) params.set('end_date', navEndDate.trim())
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/nav-chart?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const list: NavChartPoint[] = await res.json()
      setNavSeries(Array.isArray(list) ? list : [])
      setNavHoverIndex(null)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载净值失败'
      setNavChartError(msg === 'The operation was aborted.' ? '请求超时' : msg)
      setNavSeries([])
    } finally {
      setNavChartLoading(false)
    }
  }, [getFirstProductName, stockPositionFilter, navStartDate, navEndDate])

  const openNavModal = async () => {
    setNavModalOpen(true)
    setNavHoverIndex(null)
    await loadNavSeries()
  }

  // 新增 / 保存：开户渠道 & 标签
  const handleSaveOpenChannel = async () => {
    if (isConfigReadOnly) {
      setConfigError('只读账号不可修改')
      return
    }
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
    if (isConfigReadOnly) return
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
    } catch {
      // 删除失败（如记录已不存在）时也刷新页面，不显示错误
      window.location.reload()
    }
  }

  // 新增 / 保存：投流渠道承接员工
  const handleSaveStaff = async () => {
    if (isConfigReadOnly) {
      setConfigError('只读账号不可修改')
      return
    }
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
    if (isConfigReadOnly) return
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
    } catch {
      // 删除失败（如记录已不存在）时也刷新页面，不显示错误
      window.location.reload()
    }
  }

  // -------------------- 渲染：顶部导航 --------------------

  const pageTitle = (() => {
    if (!isConfigMode) {
      if (view === 'realtime') return '【市场中心】实时加微名单'
      if (view === 'open_channel_daily') return '自营渠道加微统计'
      return 'StarRocks 业务应用'
    }
    // 配置视图下按 tab 区分
    if (configTab === 'code_mapping') return '[市场中心] 抖音投流账号'
    if (configTab === 'open_channel_tag') return '渠道字典配置'
    if (configTab === 'channel_staff') return '承接人员配置'
    if (configTab === 'stock_position') return '产品净值'
    return 'StarRocks 业务应用'
  })()

  return (
    <div className="min-h-screen bg-white text-slate-800 font-sans">
      <header className="border-b border-slate-200 bg-white/95 backdrop-blur sticky top-0 z-10">
        <div className="max-w-[1600px] mx-auto px-6 py-4 flex flex-wrap items-center justify-between gap-4">
          <div>
            <h1 className="text-xl font-semibold text-sky-600">
              {pageTitle}
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

            {configTab !== 'code_mapping' && configTab !== 'stock_position' && (
              <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      if (isConfigReadOnly) return
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
                    disabled={isConfigReadOnly}
                    className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                      isConfigReadOnly
                        ? 'bg-slate-200 text-slate-400 cursor-not-allowed'
                        : 'bg-sky-600 hover:bg-sky-500 text-white'
                    }`}
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
                                  disabled={isConfigReadOnly}
                                  className={`mr-2 px-3 py-1 text-xs font-medium ${
                                    isConfigReadOnly ? 'text-slate-400 cursor-not-allowed' : 'text-sky-600 hover:text-sky-700'
                                  }`}
                                >
                                  修改
                                </button>
                                <button
                                  onClick={() => void handleDeleteOpenChannel(item)}
                                  disabled={isConfigReadOnly}
                                  className={`px-3 py-1 text-xs font-medium ${
                                    isConfigReadOnly ? 'text-slate-300 cursor-not-allowed' : 'text-red-600 hover:text-red-700'
                                  }`}
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
                                  disabled={isConfigReadOnly}
                                  className={`mr-2 px-3 py-1 text-xs font-medium ${
                                    isConfigReadOnly ? 'text-slate-400 cursor-not-allowed' : 'text-sky-600 hover:text-sky-700'
                                  }`}
                                >
                                  修改
                                </button>
                                <button
                                  onClick={() => void handleDeleteStaff(item)}
                                  disabled={isConfigReadOnly}
                                  className={`px-3 py-1 text-xs font-medium ${
                                    isConfigReadOnly ? 'text-slate-300 cursor-not-allowed' : 'text-red-600 hover:text-red-700'
                                  }`}
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
            ) : configTab === 'stock_position' ? (
              <div className="mt-4">
                <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                  <div className="flex items-center gap-3 flex-wrap">
                    <label className="text-sm text-slate-600">产品名称筛选（默认 短线王）：</label>
                    <input
                      list="stock-position-products"
                      value={stockPositionFilter}
                      onChange={(e) => setStockPositionFilter(e.target.value)}
                      onBlur={() => void loadStockPosition()}
                      className="px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm w-48"
                      placeholder="短线王"
                    />
                    <datalist id="stock-position-products">
                      {stockPositionProducts.map((p) => (
                        <option key={p} value={p} />
                      ))}
                    </datalist>
                    <button
                      onClick={() => void loadStockPosition()}
                      disabled={stockPositionLoading}
                      className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-800 font-medium"
                    >
                      {stockPositionLoading ? '加载中...' : '查询'}
                    </button>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => void openNavModal()}
                      className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium transition-colors"
                      title="查看净值曲线（默认近 90 天）"
                    >
                      净值图
                    </button>
                    <button
                      onClick={() => void downloadStockPositionExcel()}
                      className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium transition-colors"
                      title="导出当前产品全部数据"
                    >
                      下载 Excel
                    </button>
                    <button
                      onClick={() => void loadStockPosition()}
                      disabled={stockPositionLoading}
                      className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-800 font-medium"
                    >
                      刷新
                    </button>
                  </div>
                </div>
                {stockPositionError && (
                  <div className="mb-4 p-4 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
                    {stockPositionError}
                  </div>
                )}
                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-slate-200 bg-slate-100">
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">产品名称</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">日期</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">股票代码</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">个股</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">仓位(%)</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">买入/卖出</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">成交价</th>
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">操作</th>
                        </tr>
                      </thead>
                      <tbody>
                        {isConfigReadOnly && (
                          <tr className="border-b border-slate-200 bg-slate-50">
                            <td colSpan={8} className="px-3 py-2 text-slate-500 text-xs">
                              当前为只读账号：不可新增/修改/删除，但可查看、筛选、下载 Excel、查看净值图。
                            </td>
                          </tr>
                        )}

                        {/* 新增行：顶部一行，右箭头切下一格，买入/卖出用 select 键盘可操作 */}
                        <tr className={`border-b border-slate-200 ${isConfigReadOnly ? 'bg-slate-50' : 'bg-sky-50/60'}`}>
                          <td className="px-3 py-2">
                            <input
                              value={stockPositionNewRow.product_name}
                              readOnly
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLInputElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = e.key === 'ArrowRight' ? idx + 1 : idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                }
                              }}
                              className={`w-full min-w-[80px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-200 bg-slate-100 text-slate-500 cursor-not-allowed'
                              }`}
                              placeholder="产品名称（随上方筛选）"
                            />
                          </td>
                          <td className="px-3 py-2">
                            <input
                              value={stockPositionNewRow.trade_date}
                              onChange={(e) => setStockPositionNewRow((p) => ({ ...p, trade_date: e.target.value }))}
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLInputElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = e.key === 'ArrowRight' ? idx + 1 : idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                }
                              }}
                              className={`w-full min-w-[100px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-300 text-slate-800'
                              }`}
                              placeholder="YYYY-MM-DD，空=今天"
                            />
                          </td>
                          <td className="px-3 py-2">
                            <input
                              value={stockPositionNewRow.stock_code}
                              onChange={(e) => setStockPositionNewRow((p) => ({ ...p, stock_code: e.target.value }))}
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLInputElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = e.key === 'ArrowRight' ? idx + 1 : idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                }
                              }}
                              className={`w-full min-w-[90px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-300 text-slate-800'
                              }`}
                              placeholder="必填"
                            />
                          </td>
                          <td className="px-3 py-2">
                            <input
                              value={stockPositionNewRow.stock_name}
                              onChange={(e) => setStockPositionNewRow((p) => ({ ...p, stock_name: e.target.value }))}
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLInputElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = e.key === 'ArrowRight' ? idx + 1 : idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                }
                              }}
                              className={`w-full min-w-[80px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-300 text-slate-800'
                              }`}
                              placeholder="个股"
                            />
                          </td>
                          <td className="px-3 py-2">
                            <input
                              value={stockPositionNewRow.position_pct}
                              onChange={(e) => setStockPositionNewRow((p) => ({ ...p, position_pct: e.target.value }))}
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLInputElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = e.key === 'ArrowRight' ? idx + 1 : idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                }
                              }}
                              className={`w-full min-w-[60px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-300 text-slate-800'
                              }`}
                              placeholder="必填"
                            />
                          </td>
                          <td className="px-3 py-2">
                            <select
                              value={stockPositionNewRow.side}
                              onChange={(e) => setStockPositionNewRow((p) => ({ ...p, side: e.target.value }))}
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLSelectElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = e.key === 'ArrowRight' ? idx + 1 : idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                }
                                if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
                                  e.preventDefault()
                                  setStockPositionNewRow((p) => ({ ...p, side: p.side === '买入' ? '卖出' : '买入' }))
                                }
                              }}
                              className={`min-w-[72px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-300 text-slate-800 bg-white'
                              }`}
                            >
                              <option value="买入">买入</option>
                              <option value="卖出">卖出</option>
                            </select>
                          </td>
                          <td className="px-3 py-2">
                            <input
                              value={stockPositionNewRow.price}
                              onChange={(e) => setStockPositionNewRow((p) => ({ ...p, price: e.target.value }))}
                              disabled={isConfigReadOnly}
                              onKeyDown={(e) => {
                                if (e.key === 'ArrowLeft') {
                                  e.preventDefault()
                                  const tr = (e.target as HTMLElement).closest('tr')
                                  const inputs = tr?.querySelectorAll<HTMLInputElement | HTMLSelectElement>('input, select')
                                  const idx = inputs ? Array.from(inputs).indexOf(e.target as HTMLInputElement) : -1
                                  if (idx >= 0 && inputs) {
                                    const nextIdx = idx - 1
                                    if (nextIdx >= 0 && nextIdx < inputs.length) (inputs[nextIdx] as HTMLElement).focus()
                                  }
                                  return
                                }
                                if (e.key === 'ArrowRight' || e.key === 'Enter') {
                                  e.preventDefault()
                                  void saveStockPositionNewRow()
                                }
                              }}
                              className={`w-full min-w-[70px] px-2 py-1.5 rounded border text-xs ${
                                isConfigReadOnly ? 'border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed' : 'border-slate-300 text-slate-800'
                              }`}
                              placeholder="必填"
                            />
                          </td>
                          <td className="px-3 py-2">
                            <button
                              type="button"
                              onClick={() => void saveStockPositionNewRow()}
                              disabled={isConfigReadOnly}
                              className={`px-2 py-1 rounded text-xs ${
                                isConfigReadOnly ? 'bg-slate-200 text-slate-400 cursor-not-allowed' : 'bg-sky-600 hover:bg-sky-500 text-white'
                              }`}
                            >
                              保存
                            </button>
                          </td>
                        </tr>
                        {stockPositionLoading && stockPositionItems.length === 0 ? (
                          <tr><td colSpan={8} className="py-8 text-center text-slate-500">加载中...</td></tr>
                        ) : stockPositionItems.length === 0 ? (
                          <tr><td colSpan={8} className="py-8 text-center text-slate-500">暂无数据（默认展示产品「短线王」）</td></tr>
                        ) : (
                          stockPositionItems.map((item) => (
                            editingStockPositionId === item.id ? (
                              <tr key={item.id} className="border-b border-slate-200 bg-amber-50/60">
                                <td className="px-3 py-2">
                                  <input
                                    value={editingStockPositionRow?.product_name ?? ''}
                                    readOnly
                                    className="w-full min-w-[80px] px-2 py-1.5 rounded border border-slate-200 text-xs bg-slate-100 text-slate-500 cursor-not-allowed"
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <input
                                    value={editingStockPositionRow?.trade_date ?? ''}
                                    onChange={(e) => setEditingStockPositionRow((p) => p ? { ...p, trade_date: e.target.value } : null)}
                                    className="w-full min-w-[100px] px-2 py-1.5 rounded border border-slate-300 text-xs"
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <input
                                    value={editingStockPositionRow?.stock_code ?? ''}
                                    onChange={(e) => setEditingStockPositionRow((p) => p ? { ...p, stock_code: e.target.value } : null)}
                                    className="w-full min-w-[90px] px-2 py-1.5 rounded border border-slate-300 text-xs"
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <input
                                    value={editingStockPositionRow?.stock_name ?? ''}
                                    onChange={(e) => setEditingStockPositionRow((p) => p ? { ...p, stock_name: e.target.value } : null)}
                                    className="w-full min-w-[80px] px-2 py-1.5 rounded border border-slate-300 text-xs"
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <input
                                    type="number"
                                    value={editingStockPositionRow?.position_pct != null ? String(editingStockPositionRow.position_pct) : ''}
                                    onChange={(e) => setEditingStockPositionRow((p) => p ? { ...p, position_pct: e.target.value ? Number(e.target.value) : null } : null)}
                                    className="w-full min-w-[60px] px-2 py-1.5 rounded border border-slate-300 text-xs"
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <select
                                    value={editingStockPositionRow?.side ?? '买入'}
                                    onChange={(e) => setEditingStockPositionRow((p) => p ? { ...p, side: e.target.value } : null)}
                                    className="min-w-[72px] px-2 py-1.5 rounded border border-slate-300 text-xs bg-white"
                                  >
                                    <option value="买入">买入</option>
                                    <option value="卖出">卖出</option>
                                  </select>
                                </td>
                                <td className="px-3 py-2">
                                  <input
                                    type="number"
                                    value={editingStockPositionRow?.price != null ? String(editingStockPositionRow.price) : ''}
                                    onChange={(e) => setEditingStockPositionRow((p) => p ? { ...p, price: e.target.value ? Number(e.target.value) : null } : null)}
                                    className="w-full min-w-[70px] px-2 py-1.5 rounded border border-slate-300 text-xs"
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <button type="button" onClick={() => void saveStockPositionEdit()} className="mr-1 px-2 py-1 rounded bg-sky-600 text-white text-xs">保存</button>
                                  <button type="button" onClick={() => { setEditingStockPositionId(null); setEditingStockPositionRow(null); }} className="px-2 py-1 rounded bg-slate-200 text-xs">取消</button>
                                </td>
                              </tr>
                            ) : (
                              <tr key={item.id} className="border-b border-slate-200 hover:bg-slate-100/80">
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.product_name ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.trade_date ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.stock_code ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.stock_name ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.position_pct != null ? `${item.position_pct}%` : '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.side ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">
                                  {item.price == null || !Number.isFinite(Number(item.price)) ? '-' : Number(item.price).toFixed(2)}
                                </td>
                                <td className="px-3 py-2 whitespace-nowrap">
                                  {!isConfigReadOnly ? (
                                    <>
                                      <button type="button" onClick={() => { setEditingStockPositionId(item.id); setEditingStockPositionRow({ ...item }); }} className="mr-1 px-2 py-1 text-sky-600 hover:text-sky-700 text-xs">修改</button>
                                      <button type="button" onClick={() => void deleteStockPosition(item)} className="px-2 py-1 text-red-600 hover:text-red-700 text-xs">删除</button>
                                    </>
                                  ) : (
                                    <span className="text-slate-400 text-xs">只读</span>
                                  )}
                                </td>
                              </tr>
                            )
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
                {stockPositionTotal > 0 && (
                  <div className="mt-3 flex items-center justify-between">
                    <span className="text-sm text-slate-500">共 {stockPositionTotal} 条，每页 {STOCK_POSITION_PAGE_SIZE} 条</span>
                    <div className="flex items-center gap-2">
                      <button
                        disabled={stockPositionPage <= 1}
                        onClick={() => setStockPositionPage((p) => Math.max(1, p - 1))}
                        className="px-3 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-700 text-sm"
                      >
                        上一页
                      </button>
                      <span className="text-slate-500 text-sm">{stockPositionPage} / {Math.max(1, Math.ceil(stockPositionTotal / STOCK_POSITION_PAGE_SIZE))}</span>
                      <button
                        disabled={stockPositionPage >= Math.ceil(stockPositionTotal / STOCK_POSITION_PAGE_SIZE)}
                        onClick={() => setStockPositionPage((p) => p + 1)}
                        className="px-3 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-700 text-sm"
                      >
                        下一页
                      </button>
                    </div>
                  </div>
                )}

                {/* 净值图弹窗 */}
                {navModalOpen && (
                  <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                    onClick={() => setNavModalOpen(false)}
                  >
                    <div
                      className="w-full max-w-5xl bg-white rounded-xl shadow-lg border border-slate-200 p-5 mx-4"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-base font-semibold text-slate-800">
                            净值图 · {getFirstProductName(stockPositionFilter)}
                          </div>
                          <div className="text-sm text-slate-500 mt-0.5">默认展示近 90 天，可按日期筛选</div>
                        </div>
                        <button
                          onClick={() => setNavModalOpen(false)}
                          className="text-slate-500 hover:text-slate-700"
                          aria-label="关闭"
                        >
                          ✕
                        </button>
                      </div>

                      <div className="mt-4 flex flex-wrap items-end justify-between gap-3">
                        <div className="flex flex-wrap items-end gap-3">
                          <div>
                            <div className="text-xs text-slate-600 mb-1">开始日期</div>
                            <input
                              type="date"
                              value={navStartDate}
                              onChange={(e) => setNavStartDate(e.target.value)}
                              className="px-3 py-2 rounded-lg border border-slate-300 text-slate-700 text-sm bg-white"
                            />
                          </div>
                          <div>
                            <div className="text-xs text-slate-600 mb-1">结束日期</div>
                            <input
                              type="date"
                              value={navEndDate}
                              onChange={(e) => setNavEndDate(e.target.value)}
                              className="px-3 py-2 rounded-lg border border-slate-300 text-slate-700 text-sm bg-white"
                            />
                          </div>
                          <button
                            type="button"
                            onClick={() => void loadNavSeries()}
                            disabled={navChartLoading}
                            className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-800 font-medium"
                          >
                            {navChartLoading ? '加载中...' : '查询'}
                          </button>
                        </div>
                        <div className="flex flex-wrap items-center gap-4 text-sm">
                          <div className="flex items-center gap-2">
                            <span className="inline-block text-xs text-slate-500">缩放：</span>
                            <button
                              type="button"
                              onClick={() => setNavZoomMode('30d')}
                              className={`px-2 py-1 rounded text-xs border ${
                                navZoomMode === '30d'
                                  ? 'bg-sky-600 text-white border-sky-600'
                                  : 'bg-white text-slate-700 border-slate-300 hover:bg-slate-100'
                              }`}
                            >
                              近30天
                            </button>
                            <button
                              type="button"
                              onClick={() => setNavZoomMode('90d')}
                              className={`px-2 py-1 rounded text-xs border ${
                                navZoomMode === '90d'
                                  ? 'bg-sky-600 text-white border-sky-600'
                                  : 'bg-white text-slate-700 border-slate-300 hover:bg-slate-100'
                              }`}
                            >
                              近90天
                            </button>
                            <button
                              type="button"
                              onClick={() => setNavZoomMode('all')}
                              className={`px-2 py-1 rounded text-xs border ${
                                navZoomMode === 'all'
                                  ? 'bg-sky-600 text-white border-sky-600'
                                  : 'bg-white text-slate-700 border-slate-300 hover:bg-slate-100'
                              }`}
                            >
                              全部
                            </button>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="inline-block w-3 h-3 rounded bg-red-500" />
                            <span className="text-slate-600">组合净值</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="inline-block w-3 h-3 rounded bg-amber-500" />
                            <span className="text-slate-600">沪深300净值</span>
                          </div>
                        </div>
                      </div>

                      {navChartError && (
                        <div className="mt-4 p-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
                          {navChartError}
                        </div>
                      )}

                      <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                        <div className="p-4 bg-white border-b border-slate-200 flex items-center justify-between">
                          <div className="text-sm text-slate-600">
                            共 {navSeries.length} 个点
                          </div>
                        </div>
                        <div className="p-4">
                          {navChartLoading && navSeries.length === 0 ? (
                            <div className="py-16 text-center text-slate-500">加载中...</div>
                          ) : navSeries.length < 2 ? (
                            <div className="py-16 text-center text-slate-500">暂无可绘制数据</div>
                          ) : (
                            (() => {
                              // 根据缩放模式裁剪可见数据（简单缩放：只看最后 N 天）
                              let visible = navSeries
                              if (navZoomMode !== 'all' && navSeries.length > 0) {
                                const n = navZoomMode === '30d' ? 30 : 90
                                visible = navSeries.slice(-n)
                              }

                              const w = 1000
                              const h = 360
                              const padL = 56
                              const padR = 16
                              const padT = 28 // 顶部留白：放标题
                              const padB = 36

                              const navVals = visible.map((p) => Number(p.nav)).filter((v) => Number.isFinite(v))
                              const hsVals = visible
                                .map((p) => (p.hs300_nav == null ? NaN : Number(p.hs300_nav)))
                                .filter((v) => Number.isFinite(v))
                              const allVals = [...navVals, ...hsVals]
                              // 强制包含基准 1.0000，并在顶部额外留白，避免最高点标注遮挡
                              const rawMin0 = Math.min(...allVals, 1.0)
                              const rawMax0 = Math.max(...allVals, 1.0)
                              const rawSpan0 = rawMax0 - rawMin0 || 1
                              const minV = rawMin0 - rawSpan0 * 0.02
                              const maxV = rawMax0 + rawSpan0 * 0.10
                              const span = maxV - minV || 1

                              const xOf = (i: number) =>
                                padL + (i * (w - padL - padR)) / Math.max(1, visible.length - 1)
                              const yOf = (v: number) =>
                                padT + ((maxV - v) * (h - padT - padB)) / span

                              const mkPath = (vals: Array<number | null | undefined>) => {
                                let d = ''
                                let started = false
                                for (let i = 0; i < vals.length; i++) {
                                  const v = vals[i]
                                  if (v == null || !Number.isFinite(Number(v))) {
                                    started = false
                                    continue
                                  }
                                  const x = xOf(i)
                                  const y = yOf(Number(v))
                                  if (!started) {
                                    d += `M ${x} ${y}`
                                    started = true
                                  } else {
                                    d += ` L ${x} ${y}`
                                  }
                                }
                                return d
                              }

                              const dNav = mkPath(visible.map((p) => p.nav))
                              const dHs = mkPath(visible.map((p) => p.hs300_nav ?? null))

                              const yTicks = 5
                              const ticks = Array.from({ length: yTicks + 1 }, (_, i) => i)
                              const fmt4 = (v: number) => {
                                const n = Number(v)
                                if (!Number.isFinite(n)) return '-'
                                return n.toFixed(4)
                              }

                              const last = visible[visible.length - 1]
                              const lastNav = Number.isFinite(Number(last.nav)) ? Number(last.nav) : null
                              const lastHs =
                                last.hs300_nav == null
                                  ? null
                                  : Number.isFinite(Number(last.hs300_nav))
                                    ? Number(last.hs300_nav)
                                    : null

                              // 计算 X 轴刻度（多几个日期）
                              const xTickCount = Math.min(6, visible.length)
                              const xTicks = Array.from({ length: xTickCount }, (_, i) => {
                                const idx =
                                  xTickCount === 1
                                    ? 0
                                    : Math.round((i * (visible.length - 1)) / (xTickCount - 1))
                                return { idx, date: visible[idx].date }
                              })

                              // 产品净值拐点（上升后转折的“尖尖”）：局部峰值
                              const navNums = visible.map((p) => (Number.isFinite(Number(p.nav)) ? Number(p.nav) : NaN))
                              const rawPeaks: number[] = []
                              for (let i = 1; i < navNums.length - 1; i++) {
                                const a = navNums[i - 1]
                                const b = navNums[i]
                                const c = navNums[i + 1]
                                if (!Number.isFinite(a) || !Number.isFinite(b) || !Number.isFinite(c)) continue
                                if (a < b && b > c) rawPeaks.push(i)
                              }
                              const PEAK_EPS = 0.0001
                              let peaks = rawPeaks.filter((i) => navNums[i] - Math.max(navNums[i - 1], navNums[i + 1]) >= PEAK_EPS)
                              // 峰值过多时只标注最“尖”的几个（避免满屏文字）
                              if (peaks.length > 8) {
                                peaks = [...peaks]
                                  .sort((i, j) => navNums[j] - navNums[i])
                                  .slice(0, 8)
                                  .sort((a, b) => a - b)
                              }
                              const labelIdxSet = new Set<number>([...peaks, visible.length - 1])

                              const handleSvgMouseMove = (evt: React.MouseEvent<SVGSVGElement>) => {
                                const rect = (evt.currentTarget as SVGSVGElement).getBoundingClientRect()
                                const px = evt.clientX - rect.left
                                const scaleX = w / Math.max(1, rect.width)
                                const svgX = px * scaleX
                                const xMin = padL
                                const xMax = w - padR
                                if (svgX < xMin || svgX > xMax) {
                                  setNavHoverIndex(null)
                                  return
                                }
                                const t = (svgX - xMin) / Math.max(1, xMax - xMin)
                                const idx = Math.round(t * (visible.length - 1))
                                if (idx >= 0 && idx < visible.length) {
                                  setNavHoverIndex(idx)
                                } else {
                                  setNavHoverIndex(null)
                                }
                              }

                              const handleSvgMouseLeave = () => {
                                setNavHoverIndex(null)
                              }

                              const hover =
                                navHoverIndex != null && navHoverIndex >= 0 && navHoverIndex < visible.length
                                  ? visible[navHoverIndex]
                                  : null

                              const hoverX = hover ? xOf(navHoverIndex!) : null
                              const hoverNavY =
                                hover && Number.isFinite(Number(hover.nav)) ? yOf(Number(hover.nav)) : null
                              const hoverHsY =
                                hover && hover.hs300_nav != null && Number.isFinite(Number(hover.hs300_nav))
                                  ? yOf(Number(hover.hs300_nav))
                                  : null

                              return (
                                <div className="w-full">
                                  <div className="w-full">
                                    <svg
                                      viewBox={`0 0 ${w} ${h}`}
                                      className="w-full h-[360px] cursor-crosshair"
                                      onMouseMove={handleSvgMouseMove}
                                      onMouseLeave={handleSvgMouseLeave}
                                      onClick={handleSvgMouseMove}
                                    >
                                      {/* title */}
                                      <text x={w / 2} y={14} textAnchor="middle" fontSize="14" fill="#0f172a" fontWeight="600">
                                        {getFirstProductName(stockPositionFilter)} vs 沪深300
                                      </text>

                                      {/* grid */}
                                      {ticks.map((t) => {
                                        const y = padT + (t * (h - padT - padB)) / yTicks
                                        const v = maxV - (t * span) / yTicks
                                        return (
                                          <g key={t}>
                                            <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="#e2e8f0" strokeWidth="1" />
                                            <text x={padL - 10} y={y + 4} textAnchor="end" fontSize="12" fill="#64748b">
                                              {fmt4(v)}
                                            </text>
                                          </g>
                                        )
                                      })}

                                      {/* axes */}
                                      <line x1={padL} y1={padT} x2={padL} y2={h - padB} stroke="#94a3b8" strokeWidth="1" />
                                      <line x1={padL} y1={h - padB} x2={w - padR} y2={h - padB} stroke="#94a3b8" strokeWidth="1" />

                                      {/* series */}
                                      <path d={dNav} fill="none" stroke="#ef4444" strokeWidth="2.5" />
                                      {dHs ? <path d={dHs} fill="none" stroke="#f59e0b" strokeWidth="2.5" /> : null}

                                      {/* 1.0000 基准线（纵轴额外标注） */}
                                      {(() => {
                                        const y = yOf(1.0)
                                        if (y < padT || y > h - padB) return null
                                        return (
                                          <g>
                                            <line x1={padL} y1={y} x2={w - padR} y2={y} stroke="#cbd5e1" strokeWidth="1" strokeDasharray="3 3" />
                                            <text x={padL - 10} y={y + 4} textAnchor="end" fontSize="12" fill="#111827">
                                              1.0000
                                            </text>
                                          </g>
                                        )
                                      })()}

                                      {/* 产品净值拐点 & 最后一天：标注净值 */}
                                      {Array.from(labelIdxSet).map((idx) => {
                                        const p = visible[idx]
                                        const v = Number(p?.nav)
                                        if (!Number.isFinite(v)) return null
                                        const x = xOf(idx)
                                        const y = yOf(v)
                                        const isLast = idx === visible.length - 1
                                        const anchor = isLast ? 'end' : 'middle'
                                        const dx = isLast ? -6 : 0
                                        const textY = Math.max(padT + 12, y - 10)
                                        return (
                                          <g key={`nav-label-${idx}`}>
                                            <circle cx={x} cy={y} r={3} fill="#ef4444" stroke="#ffffff" strokeWidth={1.5} />
                                            <text
                                              x={x}
                                              y={textY}
                                              dx={dx}
                                              textAnchor={anchor}
                                              fontSize="12"
                                              fill="#111827"
                                              style={{ paintOrder: 'stroke', stroke: '#ffffff', strokeWidth: 3 }}
                                            >
                                              {fmt4(v)}
                                            </text>
                                          </g>
                                        )
                                      })}

                                      {/* x labels：多日期刻度 */}
                                      {xTicks.map((t, idx) => {
                                        const x = xOf(t.idx)
                                        return (
                                          <text
                                            key={idx}
                                            x={x}
                                            y={h - 10}
                                            textAnchor={idx === 0 ? 'start' : idx === xTicks.length - 1 ? 'end' : 'middle'}
                                            fontSize="12"
                                            fill="#64748b"
                                          >
                                            {t.date}
                                          </text>
                                        )
                                      })}

                                      {/* hover 指示线与点 */}
                                      {hover && hoverX != null && (
                                        <g>
                                          <line
                                            x1={hoverX}
                                            y1={padT}
                                            x2={hoverX}
                                            y2={h - padB}
                                            stroke="#94a3b8"
                                            strokeWidth="1"
                                            strokeDasharray="4 4"
                                          />
                                          {hoverNavY != null && (
                                            <circle cx={hoverX} cy={hoverNavY} r={4} fill="#ef4444" stroke="#ffffff" strokeWidth={1.5} />
                                          )}
                                          {hoverHsY != null && (
                                            <circle cx={hoverX} cy={hoverHsY} r={4} fill="#f59e0b" stroke="#ffffff" strokeWidth={1.5} />
                                          )}
                                        </g>
                                      )}
                                    </svg>
                                    <div className="mt-2 text-sm text-slate-600 flex flex-wrap gap-x-6 gap-y-1">
                                      <div>
                                        最新日期：{last.date}（右端） 组合净值：{lastNav == null ? '-' : fmt4(lastNav)} 沪深300净值：
                                        {lastHs == null ? '-' : fmt4(lastHs)}
                                      </div>
                                      {hover && (
                                        <div className="text-slate-700">
                                          当前点：日期 {hover.date} · 组合净值 {fmt4(Number(hover.nav))}
                                          {hover.hs300_nav != null ? ` · 沪深300净值 ${fmt4(Number(hover.hs300_nav))}` : ''}
                                        </div>
                                      )}
                                    </div>
                                  </div>
                                </div>
                              )
                            })()
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ) : null}

            {/* 追加：code_mapping 配置表 */}
            {configTab === 'code_mapping' && (
            <div className="mt-4">
              <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                {/* <div>
                  <div className="text-base font-semibold text-slate-800">[市场中心] 抖音投流账号</div>
                  <div className="text-sm text-slate-500">渠道映射 / 消耗配置（来自 StarRocks 物化视图）</div>
                </div> */}
                <div className="flex items-center gap-3">
                  {!isConfigReadOnly && (
                    <button
                      onClick={openAddCodeMapping}
                      className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium transition-colors"
                    >
                      新增
                    </button>
                  )}
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
                              {!isConfigReadOnly ? (
                                <>
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
                                </>
                              ) : (
                                <span className="text-slate-400 text-xs">只读</span>
                              )}
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
                      {codeMappingModal === 'edit' ? '修改[市场中心]抖音投流账号' : '新增[市场中心]抖音投流账号'}
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
                      disabled={isConfigReadOnly}
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
                      disabled={isConfigReadOnly}
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
                      disabled={isConfigReadOnly}
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
