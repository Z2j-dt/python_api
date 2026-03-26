import { useState, useEffect, useCallback, useMemo, useRef } from 'react'
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

function maskPhone(v?: string | null) {
  const s = String(v || '').trim()
  return s.length >= 11 ? `${s.slice(0, 3)}****${s.slice(-4)}` : (s || '-')
}

function getCurrentYearMonthValue(): string {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
}

function formatYearMonthLabel(ymValue: string): string {
  // ymValue: YYYY-MM
  const m = ymValue.trim().match(/^(\d{4})-(\d{2})$/)
  if (!m) return ymValue
  return `${m[1]}年${m[2]}月`
}

function buildYearMonthRange(centerYm: string, beforeMonths: number, afterMonths: number): string[] {
  const m = centerYm.match(/^(\d{4})-(\d{2})$/)
  if (!m) return [centerYm]
  const year = Number(m[1])
  const monthIndex0 = Number(m[2]) - 1
  const base = new Date(year, monthIndex0, 1)
  const out: string[] = []
  for (let i = -beforeMonths; i <= afterMonths; i++) {
    const d = new Date(base.getFullYear(), base.getMonth() + i, 1)
    out.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`)
  }
  return out
}

type MonthWheelPickerProps = {
  value: string // YYYY-MM
  onChange: (value: string) => void
  onPick?: (value: string) => void
  disabled?: boolean
}

type MonthDropdownWheelPickerProps = {
  value: string // YYYY-MM
  onChange: (value: string) => void
  disabled?: boolean
}

function MonthDropdownWheelPicker({ value, onChange, disabled }: MonthDropdownWheelPickerProps) {
  const [open, setOpen] = useState(false)
  const rootRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!open) return
    const onDocMouseDown = (e: MouseEvent) => {
      const el = rootRef.current
      if (!el) return
      const target = e.target as Node | null
      if (target && el.contains(target)) return
      setOpen(false)
    }
    document.addEventListener('mousedown', onDocMouseDown)
    return () => document.removeEventListener('mousedown', onDocMouseDown)
  }, [open])

  const hasValue = !!(value && /^\d{4}-\d{2}$/.test(value))
  const displayValue = hasValue ? value : ''
  // 只用于“滚轮中间默认定位”：当未填写时仍定位到当前年月，方便用户滚动选择
  const wheelSafeValue = hasValue ? (value as string) : getCurrentYearMonthValue()

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        disabled={disabled}
        onClick={() => {
          if (disabled) return
          setOpen((p) => !p)
        }}
        className={`w-full flex items-center justify-between gap-3 px-3 py-2 rounded-lg border text-left ${
          disabled ? 'bg-slate-100 text-slate-400 border-slate-200 cursor-not-allowed' : 'bg-white border-slate-300 text-slate-800'
        }`}
      >
        <span className={`text-sm ${hasValue ? 'text-slate-800' : 'text-slate-400'}`}>
          {hasValue ? formatYearMonthLabel(displayValue) : '不填'}
        </span>
        <span className="text-slate-500 text-sm">{open ? '▲' : '▼'}</span>
      </button>

      {open && (
        <div className="absolute z-50 left-0 right-0 mt-1">
          <div className="rounded-lg border border-slate-200 bg-white shadow-lg p-2">
            <button
              type="button"
              className={`w-full px-2 py-2 rounded-md text-sm ${
                !hasValue ? 'bg-sky-50 text-sky-700 font-semibold' : 'text-slate-700 hover:bg-slate-50'
              }`}
              onClick={() => {
                onChange('')
                setOpen(false)
              }}
              disabled={disabled}
            >
              不填
            </button>
            <div className="h-2" />
            <MonthWheelPicker
              value={wheelSafeValue}
              onChange={(v) => {
                onChange(v)
              }}
              onPick={() => setOpen(false)}
              disabled={disabled}
            />
          </div>
        </div>
      )}
    </div>
  )
}

function MonthWheelPicker({ value, onChange, onPick, disabled }: MonthWheelPickerProps) {
  const ITEM_HEIGHT = 30
  const VISIBLE_COUNT = 5
  const wheelHeight = ITEM_HEIGHT * VISIBLE_COUNT

  const initialSelectedValue = value && /^\d{4}-\d{2}$/.test(value) ? value : getCurrentYearMonthValue()
  // baseCenter 锁定为组件挂载时的初始值：避免用户滚动时列表“跟着选中项重置”
  const baseCenter = useMemo(() => initialSelectedValue, [])
  const selectedValue = value && /^\d{4}-\d{2}$/.test(value) ? value : baseCenter
  const months = useMemo(() => buildYearMonthRange(baseCenter, 120, 120), [baseCenter])
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const rafRef = useRef<number | null>(null)
  const userScrollingRef = useRef(false)
  const scrollEndTimerRef = useRef<number | null>(null)

  const selectedIndex = Math.max(0, months.indexOf(selectedValue))

  // 初始化/更新滚动位置
  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    if (userScrollingRef.current) return
    el.scrollTo({ top: selectedIndex * ITEM_HEIGHT, behavior: 'auto' })
  }, [selectedIndex, selectedValue])

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    if (disabled) return
    if (rafRef.current) cancelAnimationFrame(rafRef.current)
    rafRef.current = requestAnimationFrame(() => {
      const idx = Math.round(el.scrollTop / ITEM_HEIGHT)
      const safeIdx = Math.min(months.length - 1, Math.max(0, idx))
      const v = months[safeIdx]
      if (v && v !== value) onChange(v)
      userScrollingRef.current = true
      if (scrollEndTimerRef.current) window.clearTimeout(scrollEndTimerRef.current)
      scrollEndTimerRef.current = window.setTimeout(() => {
        userScrollingRef.current = false
      }, 120)
    })
  }

  return (
    <div className="w-full">
      <div className="relative">
        {/* 中间对齐线：让用户感知当前选中月份 */}
        <div
          className="pointer-events-none absolute left-0 right-0"
          style={{
            top: (wheelHeight - ITEM_HEIGHT) / 2,
            height: ITEM_HEIGHT,
            borderTop: '1px solid rgba(56, 189, 248, 0.6)',
            borderBottom: '1px solid rgba(56, 189, 248, 0.6)',
          }}
        />
        <div
          ref={scrollRef}
          onScroll={handleScroll}
          className={`overflow-y-auto rounded-lg border border-slate-300 bg-white ${disabled ? 'opacity-60' : ''}`}
          style={{
            height: wheelHeight,
            scrollSnapType: 'y mandatory',
          }}
        >
          {months.map((m) => {
            const isActive = m === selectedValue
            return (
              <div
                key={m}
                className={`flex items-center justify-center px-2 text-sm scroll-snap-align-start ${
                  isActive ? 'text-sky-700 font-semibold bg-sky-50/70' : 'text-slate-700'
                }`}
                style={{ height: ITEM_HEIGHT }}
                role="button"
                tabIndex={0}
                onClick={() => {
                  if (disabled) return
                  onChange(m)
                  onPick?.(m)
                }}
                onKeyDown={(e) => {
                  if (disabled) return
                  if (e.key !== 'Enter' && e.key !== ' ') return
                  e.preventDefault()
                  onChange(m)
                  onPick?.(m)
                }}
              >
                {formatYearMonthLabel(m)}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
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

interface OpportunityLeadItem {
  id: number
  biz_category_big?: string | null
  biz_category_small?: string | null
  clue_name?: string | null
  is_important?: boolean | null
  remark?: string | null
  table_name?: string | null
  created_at?: string | null
  updated_at?: string | null
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
  row_id?: number | null
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

interface NavDetailRow {
  biz_date: string
  stock_name?: string | null
  stock_code?: string | null
  position_after?: number | null
}

interface NavDetailResp {
  product_name: string
  biz_date?: string | null
  total: number
  items: NavDetailRow[]
}

interface MorningHotStockTrackItem {
  id: number
  tg_name?: string | null
  biz_date?: string | null
  stock_name?: string | null
  stock_code?: string | null
  remark?: string | null
  created_at?: string | null
  updated_at?: string | null
}

interface MorningHotStockTrackPerfItem {
  biz_date: string | null
  stock_name?: string | null
  openprice?: number | null
  t1_pct?: string | null
  t3_pct?: string | null
}

interface MorningHotStockTrackPerfResp {
  tg_name: string
  start_date: string
  end_date: string
  push_count: number
  next_win_rate?: string | null
  t3_win_rate?: string | null
  items: MorningHotStockTrackPerfItem[]
}

interface SalesOrderConfigItem {
  sole_code: string
  customer_account: string
  product_name: string
  pay_time?: string | null
  in_month?: string | null
  channel?: string | null
  wechat_nick?: string | null
  sales_owner?: string | null
}

interface SalesOrderConfigResp {
  date: string
  total: number
  items: SalesOrderConfigItem[]
}

interface SalesOrderDetailItem {
  pay_time?: string | null
  pay_time_end?: string | null
  customer_name?: string | null
  customer_account?: string | null
  customer_phone?: string | null
  sole_code?: string | null
  product_name?: string | null
  product_type?: string | null
  product_class?: string | null
  sign_method?: string | null
  sign_type?: string | null
  pay_amount?: number | null
  pay_amount_display?: number | null
  pay_commission?: number | null
  sign_attr?: string | null
  refund_amount?: number | null
  curr_total_asset?: number | null
  customer_layer?: string | null
  in_month?: string | null
  channel?: string | null
  sales_owner?: string | null
  wechat_nick?: string | null
}

interface SalesOrderSummaryItem {
  sales_owner?: string | null
  commission_count?: number | null
  cash_count?: number | null
  total_count?: number | null
  cash_amount?: number | null
  new_count?: number | null
  renew_count?: number | null
  repurchase_count?: number | null
}

interface SignCustomerGroupItem {
  sole_code: string
  customer_name?: string | null
  customer_phone?: string | null
  customer_account?: string | null
  wechat_nick?: string | null
  pay_time?: string | null
  sign_type?: string | null
  curr_total_asset?: number | null
  pay_time_end?: string | null
  refund_amount?: number | null
  in_group?: number | null
  updated_time?: string | null
}

interface SignCustomerGroupListResp {
  month: string
  total: number
  items: SignCustomerGroupItem[]
}

type ConfigTab =
  | 'open_channel_tag'
  | 'channel_staff'
  | 'code_mapping'
  | 'stock_position'
  | 'opportunity_lead'
  | 'morning_hot_stock_track'
  | 'sales_order'
  | 'sign_customer_group'

type ViewMode = 'realtime' | 'open_channel_daily' | 'config'

// const CONFIG_TAB_LABEL: Record<ConfigTab, string> = {
//   open_channel_tag: '开户渠道 & 企微客户标签',
//   channel_staff: '投流渠道承接员工',
//   code_mapping: '[市场中心] 抖音投流账号',
//   stock_position: '[投顾中心] 产品净值',
// }
const CONFIG_TAB_LABEL: Record<ConfigTab, string> = {
  open_channel_tag: '',
  channel_staff: '',
  code_mapping:  '',
  stock_position: '',
  opportunity_lead: '',
  morning_hot_stock_track: '',
  sales_order: '',
  sign_customer_group: '',
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
    if (tab === 'opportunity_lead') return 'opportunity_lead'
    if (tab === 'morning_hot_stock_track') return 'morning_hot_stock_track'
    if (tab === 'sales_order') return 'sales_order'
    if (tab === 'sign_customer_group') return 'sign_customer_group'
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
  const [salesOrderLoading, setSalesOrderLoading] = useState(false)
  const [salesOrderError, setSalesOrderError] = useState<string | null>(null)
  const [salesOrderItems, setSalesOrderItems] = useState<SalesOrderConfigItem[]>([])
  // 用于销售订单配置筛选的“成交日期（YYYY-MM-DD）”
  const [salesOrderMonth, setSalesOrderMonth] = useState<string>('') // 兼容旧变量名
  const [salesOrderPage, setSalesOrderPage] = useState<number>(1)
  const SALES_ORDER_PAGE_SIZE = 500
  const [salesOrderTotal, setSalesOrderTotal] = useState<number>(0)
  const [salesOrderSaving, setSalesOrderSaving] = useState<Record<string, boolean>>({})
  const [salesOrderDetailOpen, setSalesOrderDetailOpen] = useState(false)
  const [salesOrderDetailLoading, setSalesOrderDetailLoading] = useState(false)
  const [salesOrderDetailItems, setSalesOrderDetailItems] = useState<SalesOrderDetailItem[]>([])
  // 明细浮框内的月份筛选（YYYY-MM），默认跟随主页面“成交日期”的月份
  const [salesOrderDetailMonthFilter, setSalesOrderDetailMonthFilter] = useState<string>('')
  const [salesOrderSummaryOpen, setSalesOrderSummaryOpen] = useState(false)
  const [salesOrderSummaryLoading, setSalesOrderSummaryLoading] = useState(false)
  const [salesOrderSummaryItems, setSalesOrderSummaryItems] = useState<SalesOrderSummaryItem[]>([])
  // 汇总浮框内的月份筛选（YYYY-MM），默认跟随主页面“成交日期”的月份
  const [salesOrderSummaryMonthFilter, setSalesOrderSummaryMonthFilter] = useState<string>('')
  const [salesOrderEditOpen, setSalesOrderEditOpen] = useState(false)
  const [salesOrderEditRow, setSalesOrderEditRow] = useState<SalesOrderConfigItem | null>(null)
  const [salesOrderEditError, setSalesOrderEditError] = useState<string | null>(null)
  const [salesOrderEditForm, setSalesOrderEditForm] = useState<{
    in_month: string
    channel: string
    wechat_nick: string
    sales_owner: string
  }>({ in_month: '', channel: '', wechat_nick: '', sales_owner: '' })
  const [signCustomerGroupLoading, setSignCustomerGroupLoading] = useState(false)
  const [signCustomerGroupError, setSignCustomerGroupError] = useState<string | null>(null)
  const [signCustomerGroupMonth, setSignCustomerGroupMonth] = useState<string>(() => {
    const d = new Date()
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
  })
  const [signCustomerGroupPage, setSignCustomerGroupPage] = useState<number>(1)
  const SIGN_CUSTOMER_GROUP_PAGE_SIZE = 20
  const [signCustomerGroupTotal, setSignCustomerGroupTotal] = useState<number>(0)
  const [signCustomerGroupItems, setSignCustomerGroupItems] = useState<SignCustomerGroupItem[]>([])
  const [signRowSaving, setSignRowSaving] = useState<Record<string, boolean>>({})
  const signCustomerGroupReqTokenRef = useRef(0)

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

  // 客户中心：商机线索配置
  const [opportunityLeadItems, setOpportunityLeadItems] = useState<OpportunityLeadItem[]>([])
  const [editingOpportunityLead, setEditingOpportunityLead] = useState<OpportunityLeadItem | null>(null)
  const [opportunityLeadModal, setOpportunityLeadModal] = useState<'add' | 'edit' | null>(null)
  const [opportunityLeadForm, setOpportunityLeadForm] = useState<{
    biz_category_big: string
    biz_category_small: string
    clue_name: string
    is_important: '1' | '0'
    remark: string
    table_name: string
  }>({
    biz_category_big: '',
    biz_category_small: '',
    clue_name: '',
    is_important: '0',
    remark: '',
    table_name: '',
  })

  // 客户中心：早盘人气股战绩追踪配置
  const [morningHotStockTrackItems, setMorningHotStockTrackItems] = useState<MorningHotStockTrackItem[]>([])
  const [editingMorningHotStockTrack, setEditingMorningHotStockTrack] = useState<MorningHotStockTrackItem | null>(null)
  const [morningHotStockTrackModal, setMorningHotStockTrackModal] = useState<'add' | 'edit' | null>(null)
  const [morningHotStockTrackTgName, setMorningHotStockTrackTgName] = useState<string>('胡晶翔')
  const [morningHotStockTrackTgNames, setMorningHotStockTrackTgNames] = useState<string[]>([])
  const MORNING_HOT_STOCK_TRACK_PAGE_SIZE = 30
  const [morningHotStockTrackPage, setMorningHotStockTrackPage] = useState<number>(1)
  const [morningHotStockPerfOpen, setMorningHotStockPerfOpen] = useState(false)
  const [morningHotStockPerfLoading, setMorningHotStockPerfLoading] = useState(false)
  const [morningHotStockPerfError, setMorningHotStockPerfError] = useState<string | null>(null)
  const [morningHotStockPerfData, setMorningHotStockPerfData] = useState<MorningHotStockTrackPerfResp | null>(null)
  const [morningHotStockPerfStart, setMorningHotStockPerfStart] = useState<string>(() => {
    const d = new Date()
    d.setDate(1) // 本月 1 号
    const lastPrev = new Date(d.getTime() - 24 * 60 * 60 * 1000)
    const firstPrev = new Date(lastPrev)
    firstPrev.setDate(1)
    return firstPrev.toISOString().slice(0, 10)
  })
  const [morningHotStockPerfEnd, setMorningHotStockPerfEnd] = useState<string>(() => {
    const d = new Date()
    d.setDate(1)
    const lastPrev = new Date(d.getTime() - 24 * 60 * 60 * 1000)
    return lastPrev.toISOString().slice(0, 10)
  })
  const [morningHotStockTrackForm, setMorningHotStockTrackForm] = useState<{
    tg_name: string
    biz_date: string
    stock_name: string
    stock_code: string
    remark: string
  }>({
    tg_name: '胡晶翔',
    biz_date: '',
    stock_name: '',
    stock_code: '',
    remark: '',
  })

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
  // 净值明细（浮框 Tab 共用数据）
  const [navDetailLoading, setNavDetailLoading] = useState(false)
  const [navDetailError, setNavDetailError] = useState<string | null>(null)
  const [navDetailItems, setNavDetailItems] = useState<NavDetailRow[]>([])

  // 产品净值预览（浮框，仓位明细/净值/净值明细）
  const [stockPositionPreviewOpen, setStockPositionPreviewOpen] = useState(false)
  const [stockPositionPreviewTab, setStockPositionPreviewTab] = useState<'position' | 'nav' | 'nav_detail'>('position')
  const [stockPositionPreviewPositionLoading, setStockPositionPreviewPositionLoading] = useState(false)
  const [stockPositionPreviewPositionError, setStockPositionPreviewPositionError] = useState<string | null>(null)
  const [stockPositionPreviewPositionItems, setStockPositionPreviewPositionItems] = useState<StockPositionItem[]>([])
  const [stockPositionPreviewNavLoading, setStockPositionPreviewNavLoading] = useState(false)
  const [stockPositionPreviewNavError, setStockPositionPreviewNavError] = useState<string | null>(null)
  const [stockPositionPreviewNavItems, setStockPositionPreviewNavItems] = useState<NavChartPoint[]>([])
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

  // ESC 关闭战绩弹窗
  useEffect(() => {
    if (!morningHotStockPerfOpen) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setMorningHotStockPerfOpen(false)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [morningHotStockPerfOpen])

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

  const loadOpportunityLeads = useCallback(async () => {
    setConfigLoading(true)
    setConfigError(null)
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/opportunity-leads`)
      if (!res.ok) throw new Error(await res.text())
      const list: OpportunityLeadItem[] = await res.json()
      setOpportunityLeadItems(list)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载商机线索配置失败'
      const display = msg === 'The operation was aborted.' || msg.includes('fetch') || msg.includes('Failed')
        ? '请求超时或网络不可达，请检查内网连接'
        : msg
      setConfigError(display)
    } finally {
      setConfigLoading(false)
    }
  }, [])

  const loadMorningHotStockTrack = useCallback(async () => {
    setConfigLoading(true)
    setConfigError(null)
    try {
      const params = new URLSearchParams()
      if (morningHotStockTrackTgName.trim()) params.set('tg_name', morningHotStockTrackTgName.trim())
      const res = await fetchWithTimeout(`${API_BASE}/api/config/morning-hot-stock-track?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const list: MorningHotStockTrackItem[] = await res.json()
      setMorningHotStockTrackItems(list)
      setMorningHotStockTrackPage(1)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载早盘人气股战绩追踪配置失败'
      const display =
        msg === 'The operation was aborted.' || msg.includes('fetch') || msg.includes('Failed')
          ? '请求超时或网络不可达，请检查内网连接'
          : msg
      setConfigError(display)
    } finally {
      setConfigLoading(false)
    }
  }, [morningHotStockTrackTgName])

  const loadMorningHotStockPerf = useCallback(async () => {
    const name = (morningHotStockTrackTgName || '').trim()
    if (!name) return
    setMorningHotStockPerfLoading(true)
    setMorningHotStockPerfError(null)
    try {
      const params = new URLSearchParams()
      params.set('tg_name', name)
      if (morningHotStockPerfStart.trim()) params.set('start_date', morningHotStockPerfStart.trim())
      if (morningHotStockPerfEnd.trim()) params.set('end_date', morningHotStockPerfEnd.trim())
      const res = await fetchWithTimeout(`${API_BASE}/api/config/morning-hot-stock-track/performance?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const data: MorningHotStockTrackPerfResp = await res.json()
      setMorningHotStockPerfData(data)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载战绩失败'
      const display =
        msg === 'The operation was aborted.' || msg.includes('fetch') || msg.includes('Failed')
          ? '请求超时或网络不可达，请检查内网连接'
          : msg
      setMorningHotStockPerfError(display)
    } finally {
      setMorningHotStockPerfLoading(false)
    }
  }, [morningHotStockTrackTgName, morningHotStockPerfStart, morningHotStockPerfEnd])

  const downloadMorningHotStockPerfCsv = useCallback(async () => {
    const name = (morningHotStockTrackTgName || '').trim()
    if (!name) return
    try {
      const params = new URLSearchParams()
      params.set('tg_name', name)
      if (morningHotStockPerfStart.trim()) params.set('start_date', morningHotStockPerfStart.trim())
      if (morningHotStockPerfEnd.trim()) params.set('end_date', morningHotStockPerfEnd.trim())
      const res = await fetchWithTimeout(`${API_BASE}/api/config/morning-hot-stock-track/performance/export.csv?${params}`, {}, 60000)
      if (!res.ok) throw new Error(await res.text())
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      const start = morningHotStockPerfStart.trim() || ''
      const end = morningHotStockPerfEnd.trim() || ''
      a.download = `${name}_早评人气股_${start}~${end}.csv`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '下载失败'
      setMorningHotStockPerfError(msg)
    }
  }, [morningHotStockTrackTgName, morningHotStockPerfStart, morningHotStockPerfEnd])

  const loadMorningHotStockTrackTgNames = useCallback(async () => {
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/morning-hot-stock-track/tg-names`)
      if (!res.ok) throw new Error(await res.text())
      const list: string[] = await res.json()
      setMorningHotStockTrackTgNames(list)
      if (list.length > 0 && list.indexOf(morningHotStockTrackTgName) < 0) {
        setMorningHotStockTrackTgName(list[0])
      }
    } catch {
      setMorningHotStockTrackTgNames([])
    }
  }, [morningHotStockTrackTgName])

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

  const loadSalesOrderConfig = useCallback(async (page: number = salesOrderPage, overrideDate?: string) => {
    setSalesOrderLoading(true)
    setSalesOrderError(null)
    try {
      const params = new URLSearchParams()
      const targetDate = (overrideDate ?? salesOrderMonth).trim()
      if (targetDate) params.set('date', targetDate)
      params.set('page', String(page))
      params.set('page_size', String(SALES_ORDER_PAGE_SIZE))
      const res = await fetchWithTimeout(`${API_BASE}/api/config/sales-order?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const ct = (res.headers.get('content-type') || '').toLowerCase()
      if (!ct.includes('application/json')) {
        const raw = await res.text()
        const snippet = raw.replace(/\s+/g, ' ').slice(0, 120)
        throw new Error(`接口未返回 JSON（可能跳转到登录/门户页）：${snippet}`)
      }
      const data: SalesOrderConfigResp = await res.json()
      setSalesOrderItems(Array.isArray(data.items) ? data.items : [])
      setSalesOrderTotal(Number(data.total || 0))
      if (data.date) setSalesOrderMonth(data.date)
      setSalesOrderPage(page)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载销售订单配置失败'
      setSalesOrderError(msg === 'The operation was aborted.' ? '请求超时' : msg)
      setSalesOrderItems([])
      setSalesOrderTotal(0)
    } finally {
      setSalesOrderLoading(false)
    }
  }, [salesOrderMonth, salesOrderPage])

  const saveSalesOrderRow = useCallback(async (row: SalesOrderConfigItem): Promise<boolean> => {
    const sole = String(row.sole_code || '').trim()
    const acct = String(row.customer_account || '').trim()
    const prod = String(row.product_name || '').trim()
    if (!sole || !acct || !prod) return false
    const key = `${sole}__${acct}__${prod}`
    setSalesOrderSaving((prev) => ({ ...prev, [key]: true }))
    try {
      const res = await fetchWithTimeout(
        `${API_BASE}/api/config/sales-order/${encodeURIComponent(sole)}/${encodeURIComponent(acct)}/${encodeURIComponent(prod)}`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            in_month: row.in_month ?? '',
            channel: row.channel ?? '',
            wechat_nick: row.wechat_nick ?? '',
            sales_owner: row.sales_owner ?? '',
          }),
        },
      )
      if (!res.ok) throw new Error(await res.text())
      setSalesOrderItems((prev) =>
        prev.map((it) =>
          String(it.sole_code) === sole && String(it.customer_account || '') === acct && String(it.product_name || '') === prod
            ? {
                ...it,
                in_month: row.in_month ?? it.in_month ?? '',
                channel: row.channel ?? it.channel ?? '',
                wechat_nick: row.wechat_nick ?? it.wechat_nick ?? '',
                sales_owner: row.sales_owner ?? it.sales_owner ?? '',
              }
            : it,
        ),
      )
      return true
    } catch (e) {
      const msg = e instanceof Error ? e.message : '保存失败'
      setSalesOrderError(msg)
      return false
    } finally {
      setSalesOrderSaving((prev) => ({ ...prev, [key]: false }))
    }
  }, [])

  const openSalesOrderEdit = useCallback((row: SalesOrderConfigItem) => {
    setSalesOrderEditError(null)
    setSalesOrderEditRow(row)
    const nowYm = getCurrentYearMonthValue()
    const inMonthValue = row.in_month && /^\d{4}-\d{2}$/.test(row.in_month) ? row.in_month : nowYm
    setSalesOrderEditForm({
      in_month: inMonthValue,
      channel: row.channel ?? '',
      wechat_nick: row.wechat_nick ?? '',
      sales_owner: row.sales_owner ?? '',
    })
    setSalesOrderEditOpen(true)
  }, [])

  const closeSalesOrderEdit = useCallback(() => {
    setSalesOrderEditOpen(false)
    setSalesOrderEditRow(null)
    setSalesOrderEditError(null)
  }, [])

  const loadSalesOrderLatestDate = useCallback(async (): Promise<string | null> => {
    try {
      const res = await fetchWithTimeout(`${API_BASE}/api/config/sales-order/latest-date`)
      if (!res.ok) throw new Error(await res.text())
      const data: { date?: string } = await res.json()
      if (data?.date) setSalesOrderMonth(data.date)
      return data?.date ? String(data.date) : null
    } catch (e) {
      // latest-date 失败不阻断页面，仅保留现有筛选值
      return null
    }
  }, [])

  const loadSalesOrderDetail = useCallback(async (monthOverride?: string) => {
    setSalesOrderDetailLoading(true)
    setSalesOrderError(null)
    try {
      const params = new URLSearchParams()
      const base = (monthOverride ?? salesOrderDetailMonthFilter ?? salesOrderMonth ?? '').trim()
      const m = base ? (base.length >= 7 ? base.slice(0, 7) : base) : ''
      if (m) {
        params.set('month', m)
        // 同步到浮框内的月份筛选状态，保证输入框显示的是本次查询的月份
        setSalesOrderDetailMonthFilter(m)
      }
      const res = await fetchWithTimeout(`${API_BASE}/api/config/sales-order/detail?${params}`, {}, 60000)
      if (!res.ok) throw new Error(await res.text())
      const list: SalesOrderDetailItem[] = await res.json()
      setSalesOrderDetailItems(Array.isArray(list) ? list : [])
      setSalesOrderDetailOpen(true)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载明细失败'
      setSalesOrderError(msg)
      setSalesOrderDetailItems([])
    } finally {
      setSalesOrderDetailLoading(false)
    }
  }, [salesOrderMonth, salesOrderDetailMonthFilter])

  const loadSalesOrderSummary = useCallback(async (monthOverride?: string) => {
    setSalesOrderSummaryLoading(true)
    setSalesOrderError(null)
    try {
      const params = new URLSearchParams()
      const base = (monthOverride ?? salesOrderSummaryMonthFilter ?? salesOrderMonth ?? '').trim()
      const m = base ? (base.length >= 7 ? base.slice(0, 7) : base) : ''
      if (m) {
        params.set('month', m)
        setSalesOrderSummaryMonthFilter(m)
      }
      const res = await fetchWithTimeout(`${API_BASE}/api/config/sales-order/summary?${params}`, {}, 60000)
      if (!res.ok) throw new Error(await res.text())
      const list: SalesOrderSummaryItem[] = await res.json()
      setSalesOrderSummaryItems(Array.isArray(list) ? list : [])
      setSalesOrderSummaryOpen(true)
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载汇总失败'
      setSalesOrderError(msg)
      setSalesOrderSummaryItems([])
    } finally {
      setSalesOrderSummaryLoading(false)
    }
  }, [salesOrderMonth, salesOrderSummaryMonthFilter])

  const loadSignCustomerGroup = useCallback(async (page: number = 1) => {
    const reqToken = ++signCustomerGroupReqTokenRef.current
    setSignCustomerGroupLoading(true)
    setSignCustomerGroupError(null)
    try {
      const params = new URLSearchParams()
      if (signCustomerGroupMonth.trim()) params.set('month', signCustomerGroupMonth.trim())
      params.set('page', String(page))
      params.set('page_size', String(SIGN_CUSTOMER_GROUP_PAGE_SIZE))
      const res = await fetchWithTimeout(`${API_BASE}/api/config/sign-customer-group?${params}`)
      if (!res.ok) throw new Error(await res.text())
      const ct = (res.headers.get('content-type') || '').toLowerCase()
      if (!ct.includes('application/json')) {
        const raw = await res.text()
        const snippet = raw.replace(/\s+/g, ' ').slice(0, 120)
        throw new Error(`接口未返回 JSON（可能跳转到登录/门户页）：${snippet}`)
      }
      const data: SignCustomerGroupListResp = await res.json()
      // 避免并发请求：只处理“最后一次触发”的响应
      if (reqToken !== signCustomerGroupReqTokenRef.current) return
      setSignCustomerGroupItems(Array.isArray(data.items) ? data.items : [])
      setSignCustomerGroupTotal(Number(data.total || 0))
      setSignCustomerGroupPage(page)
    } catch (e) {
      if (reqToken !== signCustomerGroupReqTokenRef.current) return
      const msg = e instanceof Error ? e.message : '加载签约客户群管理配置失败'
      setSignCustomerGroupError(msg === 'The operation was aborted.' ? '请求超时' : msg)
      setSignCustomerGroupItems([])
      setSignCustomerGroupTotal(0)
    } finally {
      if (reqToken === signCustomerGroupReqTokenRef.current) setSignCustomerGroupLoading(false)
    }
  }, [signCustomerGroupMonth])

  const saveSignCustomerGroupRow = useCallback(async (row: SignCustomerGroupItem, inGroupValue: number) => {
    const sole = String(row.sole_code || '').trim()
    const acct = String(row.customer_account || '').trim()
    if (!sole || !acct) return
    const key = `${sole}__${acct}`
    setSignRowSaving((prev) => ({ ...prev, [key]: true }))
    try {
      const res = await fetchWithTimeout(
        `${API_BASE}/api/config/sign-customer-group/${encodeURIComponent(sole)}/${encodeURIComponent(acct)}`,
        {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ in_group: inGroupValue }),
        },
      )
      if (!res.ok) throw new Error(await res.text())
      setSignCustomerGroupItems((prev) =>
        prev.map((it) => (String(it.sole_code) === sole && String(it.customer_account || '') === acct ? { ...it, in_group: inGroupValue } : it)),
      )
    } catch (e) {
      const msg = e instanceof Error ? e.message : '保存失败'
      setSignCustomerGroupError(msg)
    } finally {
      setSignRowSaving((prev) => ({ ...prev, [key]: false }))
    }
  }, [])

  const refreshCurrentConfig = () => {
    if (configTab === 'open_channel_tag') {
      void loadOpenChannelTags()
    } else if (configTab === 'sales_order') {
      void (async () => {
        const latest = salesOrderMonth.trim() ? salesOrderMonth.trim() : await loadSalesOrderLatestDate()
        await loadSalesOrderConfig(1, latest || undefined)
      })()
    } else if (configTab === 'sign_customer_group') {
      void loadSignCustomerGroup(signCustomerGroupPage)
    } else if (configTab === 'opportunity_lead') {
      void loadOpportunityLeads()
    } else if (configTab === 'morning_hot_stock_track') {
      void loadMorningHotStockTrack()
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
    } else if (configTab === 'sales_order') {
      void (async () => {
        const latest = salesOrderMonth.trim() ? salesOrderMonth.trim() : await loadSalesOrderLatestDate()
        await loadSalesOrderConfig(1, latest || undefined)
      })()
    } else if (configTab === 'sign_customer_group') {
      void loadSignCustomerGroup(1)
    } else if (configTab === 'opportunity_lead') {
      void loadOpportunityLeads()
    } else if (configTab === 'morning_hot_stock_track') {
      void loadMorningHotStockTrackTgNames()
      void loadMorningHotStockTrack()
    } else {
      void loadChannelStaff()
    }
  }, [isConfigMode, configTab, loadOpenChannelTags, loadSalesOrderConfig, loadSalesOrderLatestDate, loadSignCustomerGroup, loadOpportunityLeads, loadMorningHotStockTrackTgNames, loadMorningHotStockTrack, loadChannelStaff])

  useEffect(() => {
    if (!isConfigMode) return
    if (configTab !== 'morning_hot_stock_track') return
    void loadMorningHotStockTrack()
  }, [isConfigMode, configTab, morningHotStockTrackTgName, loadMorningHotStockTrack])

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

  const loadStockPositionPreviewPositionDetail = useCallback(async () => {
    const productName = getFirstProductName(stockPositionFilter)
    setStockPositionPreviewPositionLoading(true)
    setStockPositionPreviewPositionError(null)
    try {
      const res = await fetchWithTimeout(
        `${API_BASE}/api/config/stock-position/export?product_name=${encodeURIComponent(productName)}`,
        {},
        60000,
      )
      if (!res.ok) throw new Error(await res.text())
      const list: StockPositionItem[] = await res.json()
      setStockPositionPreviewPositionItems(Array.isArray(list) ? list : [])
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载失败'
      setStockPositionPreviewPositionError(msg === 'The operation was aborted.' ? '请求超时' : msg)
      setStockPositionPreviewPositionItems([])
    } finally {
      setStockPositionPreviewPositionLoading(false)
    }
  }, [getFirstProductName, stockPositionFilter])

  const loadStockPositionPreviewNav = useCallback(async () => {
    const productName = getFirstProductName(stockPositionFilter)
    setStockPositionPreviewNavLoading(true)
    setStockPositionPreviewNavError(null)
    try {
      const params = new URLSearchParams()
      params.set('product_name', productName)
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/nav-chart?${params}`, {}, 60000)
      if (!res.ok) throw new Error(await res.text())
      const list: NavChartPoint[] = await res.json()
      setStockPositionPreviewNavItems(Array.isArray(list) ? list : [])
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载失败'
      setStockPositionPreviewNavError(msg === 'The operation was aborted.' ? '请求超时' : msg)
      setStockPositionPreviewNavItems([])
    } finally {
      setStockPositionPreviewNavLoading(false)
    }
  }, [getFirstProductName, stockPositionFilter])

  const loadNavDetail = useCallback(async () => {
    const productName = getFirstProductName(stockPositionFilter)
    setNavDetailLoading(true)
    setNavDetailError(null)
    try {
      const params = new URLSearchParams()
      params.set('product_name', productName)
      const res = await fetchWithTimeout(`${API_BASE}/api/config/stock-position/nav-detail?${params}`, {}, 60000)
      if (!res.ok) throw new Error(await res.text())
      const data: NavDetailResp = await res.json()
      setNavDetailItems(Array.isArray(data.items) ? data.items : [])
    } catch (e) {
      const msg = e instanceof Error ? e.message : '加载失败'
      setNavDetailError(msg === 'The operation was aborted.' ? '请求超时' : msg)
      setNavDetailItems([])
    } finally {
      setNavDetailLoading(false)
    }
  }, [getFirstProductName, stockPositionFilter])

  const openStockPositionPreview = useCallback(async () => {
    setStockPositionPreviewOpen(true)
    setStockPositionPreviewTab('position')
    // 默认加载全部 Tab 数据（位置明细/净值/净值明细），保证切 Tab 不卡
    await Promise.all([loadStockPositionPreviewPositionDetail(), loadStockPositionPreviewNav(), loadNavDetail()])
  }, [loadStockPositionPreviewPositionDetail, loadStockPositionPreviewNav, loadNavDetail])

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

  // -------------------- 客户中心：商机线索配置 --------------------

  const openEditOpportunityLead = (item: OpportunityLeadItem) => {
    setEditingOpportunityLead(item)
    setOpportunityLeadForm({
      biz_category_big: String(item.biz_category_big ?? ''),
      biz_category_small: String(item.biz_category_small ?? ''),
      clue_name: String(item.clue_name ?? ''),
      is_important: item.is_important ? '1' : '0',
      remark: String(item.remark ?? ''),
      table_name: String(item.table_name ?? ''),
    })
    setOpportunityLeadModal('edit')
  }

  const saveOpportunityLead = async () => {
    if (isConfigReadOnly) return
    setConfigError(null)
    try {
      const isEdit = opportunityLeadModal === 'edit' && !!editingOpportunityLead
      const url = isEdit
        ? `${API_BASE}/api/config/opportunity-leads/${editingOpportunityLead!.id}`
        : `${API_BASE}/api/config/opportunity-leads`
      const method = isEdit ? 'PUT' : 'POST'
      const payload = {
        biz_category_big: opportunityLeadForm.biz_category_big.trim() || null,
        biz_category_small: opportunityLeadForm.biz_category_small.trim() || null,
        clue_name: opportunityLeadForm.clue_name.trim() || null,
        is_important: opportunityLeadForm.is_important === '1',
        remark: opportunityLeadForm.remark.trim() || null,
        table_name: opportunityLeadForm.table_name.trim() || null,
      }
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!res.ok) throw new Error(await res.text())
      await refreshCurrentConfig()
      setEditingOpportunityLead(null)
      setOpportunityLeadModal(null)
    } catch (e) {
      setConfigError(e instanceof Error ? e.message : '保存失败')
    }
  }

  const deleteOpportunityLead = async (item: OpportunityLeadItem) => {
    if (isConfigReadOnly) return
    if (!window.confirm(`确定删除【${item.clue_name ?? '-'}】这条线索配置吗？`)) return
    setConfigError(null)
    try {
      const res = await fetch(`${API_BASE}/api/config/opportunity-leads/${item.id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error(await res.text())
      window.location.reload()
    } catch {
      window.location.reload()
    }
  }

  // -------------------- 客户中心：早盘人气股战绩追踪配置 --------------------

  const openEditMorningHotStockTrack = (item: MorningHotStockTrackItem) => {
    setEditingMorningHotStockTrack(item)
    setMorningHotStockTrackForm({
      tg_name: String(item.tg_name ?? morningHotStockTrackTgName ?? ''),
      biz_date: String(item.biz_date ?? ''),
      stock_name: String(item.stock_name ?? ''),
      stock_code: String(item.stock_code ?? ''),
      remark: String(item.remark ?? ''),
    })
    setMorningHotStockTrackModal('edit')
  }

  const saveMorningHotStockTrack = async () => {
    if (isConfigReadOnly) return
    setConfigError(null)
    try {
      const isEdit = morningHotStockTrackModal === 'edit' && !!editingMorningHotStockTrack
      const url = isEdit
        ? `${API_BASE}/api/config/morning-hot-stock-track/${editingMorningHotStockTrack!.id}`
        : `${API_BASE}/api/config/morning-hot-stock-track`
      const method = isEdit ? 'PUT' : 'POST'
      const payload = {
        tg_name: morningHotStockTrackForm.tg_name.trim() || null,
        biz_date: morningHotStockTrackForm.biz_date.trim() || null,
        stock_name: morningHotStockTrackForm.stock_name.trim() || null,
        stock_code: morningHotStockTrackForm.stock_code.trim() || null,
        remark: morningHotStockTrackForm.remark.trim() || null,
      }
      const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!res.ok) throw new Error(await res.text())

      // 新增后立即置顶（后端也会按 created_at DESC 排序兜底）
      if (!isEdit) {
        try {
          const created: MorningHotStockTrackItem = await res.json()
          // 只要是当前老师的数据，就置顶；否则刷新让筛选生效
          if ((created.tg_name ?? '').trim() === morningHotStockTrackTgName.trim()) {
            setMorningHotStockTrackItems((prev) => [created, ...prev])
          } else {
            await refreshCurrentConfig()
          }
          setMorningHotStockTrackModal(null)
          setEditingMorningHotStockTrack(null)
          return
        } catch {
          // fallback：走刷新
        }
      }

      await refreshCurrentConfig()
      setEditingMorningHotStockTrack(null)
      setMorningHotStockTrackModal(null)
    } catch (e) {
      setConfigError(e instanceof Error ? e.message : '保存失败')
    }
  }

  const deleteMorningHotStockTrack = async (item: MorningHotStockTrackItem) => {
    if (isConfigReadOnly) return
    if (!window.confirm(`确定删除【${item.stock_name ?? '-'} ${item.stock_code ?? ''}】这条记录吗？`)) return
    setConfigError(null)
    try {
      const res = await fetch(`${API_BASE}/api/config/morning-hot-stock-track/${item.id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error(await res.text())
      window.location.reload()
    } catch {
      window.location.reload()
    }
  }

  // -------------------- 渲染：顶部导航 --------------------

  const pageTitle = (() => {
    if (!isConfigMode) {
      if (view === 'realtime') return '实时加微名单'
      if (view === 'open_channel_daily') return '自营渠道加微统计'
      return 'StarRocks 业务应用'
    }
    // 配置视图下按 tab 区分
    if (configTab === 'code_mapping') return '抖音投流账号配置'
    if (configTab === 'open_channel_tag') return '渠道字典配置'
    if (configTab === 'channel_staff') return '承接人员配置'
    if (configTab === 'stock_position') return '产品净值'
    if (configTab === 'sales_order') return '销售订单配置'
    if (configTab === 'sign_customer_group') return '签约客户群管理配置'
    if (configTab === 'opportunity_lead') return '商机线索配置'
    if (configTab === 'morning_hot_stock_track') return '早盘人气股战绩追踪配置'
    return 'StarRocks 业务应用'
  })()

  // 行底色：按“当前列表里实际出现的日期块”交替，而不是按自然日奇偶。
  // 例如 12/12 有数据、12/13 没数据、12/14 有数据：12/12=浅蓝，12/14=白色。
  const stockPositionStripeByTradeDate = useMemo(() => {
    const normalize = (v?: string | null) => (v ?? '').trim().slice(0, 10)
    const m = new Map<string, boolean>()
    let lastDate = ''
    let isBlue = true // 第一块日期默认浅蓝
    for (const it of stockPositionItems) {
      const d = normalize(it.trade_date)
      if (!d) continue
      if (d !== lastDate) {
        if (!m.has(d)) m.set(d, isBlue)
        isBlue = !isBlue
        lastDate = d
      }
    }
    return m
  }, [stockPositionItems])

  const getTradeDateStripeClass = (tradeDate?: string | null) => {
    const d = (tradeDate ?? '').trim().slice(0, 10)
    if (!d) return ''
    return stockPositionStripeByTradeDate.get(d) ? 'bg-sky-100/70' : ''
  }

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
                  {isDailyStatsMode ? '实时数据' : '实时数据'} · 每 5 分钟自动刷新 · 上次刷新时间:{' '}
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
                {CONFIG_TAB_LABEL[configTab]}
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

            {configTab !== 'code_mapping' && configTab !== 'stock_position' && configTab !== 'sales_order' && configTab !== 'sign_customer_group' && (
              <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => {
                      if (isConfigReadOnly) return
                      if (configTab === 'open_channel_tag') {
                        setEditingOpenItem(null)
                        setOpenForm({ open_channel: '', wechat_customer_tag: '' })
                        setOpenModal('add')
                      } else if (configTab === 'channel_staff') {
                        setEditingStaffItem(null)
                        setStaffForm({ branch_name: '', staff_name: '' })
                        setStaffModalError(null)
                        setStaffModal('add')
                      } else if (configTab === 'opportunity_lead') {
                        setEditingOpportunityLead(null)
                        setOpportunityLeadForm({
                          biz_category_big: '',
                          biz_category_small: '',
                          clue_name: '',
                          is_important: '0',
                          remark: '',
                          table_name: '',
                        })
                        setOpportunityLeadModal('add')
                      } else if (configTab === 'morning_hot_stock_track') {
                        setEditingMorningHotStockTrack(null)
                        setMorningHotStockTrackForm({
                          tg_name: morningHotStockTrackTgName.trim() || '胡晶翔',
                          biz_date: '',
                          stock_name: '',
                          stock_code: '',
                          remark: '',
                        })
                        setMorningHotStockTrackModal('add')
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
            ) : configTab === 'opportunity_lead' ? (
              <div>
                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    {configLoading && opportunityLeadItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">加载中...</div>
                    ) : opportunityLeadItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                    ) : (
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-slate-200 bg-slate-100">
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">ID</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">业务大类</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">业务小类</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">线索名称</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">是否重要</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">备注</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">表名</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">创建时间</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">更新时间</th>
                            <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          {opportunityLeadItems.map((item) => (
                            <tr
                              key={item.id}
                              className="border-b border-slate-200 hover:bg-slate-100/80 transition-colors"
                            >
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.id}</td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.biz_category_big ?? '-'}</td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.biz_category_small ?? '-'}</td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.clue_name ?? '-'}</td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.is_important ? '是' : '否'}</td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap max-w-xs truncate" title={String(item.remark ?? '')}>
                                {item.remark ?? '-'}
                              </td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.table_name ?? '-'}</td>
                              <td className="px-4 py-3 text-slate-500 whitespace-nowrap">{item.created_at ?? '-'}</td>
                              <td className="px-4 py-3 text-slate-500 whitespace-nowrap">{item.updated_at ?? '-'}</td>
                              <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                <button
                                  onClick={() => openEditOpportunityLead(item)}
                                  disabled={isConfigReadOnly}
                                  className={`mr-2 px-3 py-1 text-xs font-medium ${
                                    isConfigReadOnly ? 'text-slate-400 cursor-not-allowed' : 'text-sky-600 hover:text-sky-700'
                                  }`}
                                >
                                  修改
                                </button>
                                <button
                                  onClick={() => void deleteOpportunityLead(item)}
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
            ) : configTab === 'morning_hot_stock_track' ? (
              <div>
                <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                  <div className="flex flex-wrap items-center gap-3">
                    <label className="text-sm text-slate-600 whitespace-nowrap">老师：</label>
                    <select
                      value={morningHotStockTrackTgName}
                      onChange={(e) => {
                        setMorningHotStockTrackTgName(e.target.value)
                        setMorningHotStockTrackPage(1)
                      }}
                      className="px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm bg-white w-56"
                      title="选择老师"
                    >
                      {(morningHotStockTrackTgNames.length ? morningHotStockTrackTgNames : ['胡晶翔']).map((n) => (
                        <option key={n} value={n}>
                          {n}
                        </option>
                      ))}
                    </select>
                    <button
                      onClick={() => void loadMorningHotStockTrack()}
                      disabled={configLoading}
                      className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-800 font-medium transition-colors"
                    >
                      {configLoading ? '查询中...' : '查询'}
                    </button>
                    <button
                      onClick={() => {
                        setMorningHotStockPerfOpen(true)
                        void loadMorningHotStockPerf()
                      }}
                      className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white font-medium transition-colors"
                      title="查看战绩（默认上个月）"
                    >
                      战绩
                    </button>
                  </div>
                </div>
                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    {configLoading && morningHotStockTrackItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">加载中...</div>
                    ) : morningHotStockTrackItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                    ) : (
                      <>
                        {(() => {
                          const total = morningHotStockTrackItems.length
                          const totalPages = Math.max(1, Math.ceil(total / MORNING_HOT_STOCK_TRACK_PAGE_SIZE))
                          const page = Math.min(Math.max(1, morningHotStockTrackPage), totalPages)
                          const start = (page - 1) * MORNING_HOT_STOCK_TRACK_PAGE_SIZE
                          const end = Math.min(start + MORNING_HOT_STOCK_TRACK_PAGE_SIZE, total)
                          const pageItems = morningHotStockTrackItems.slice(start, end)
                          return (
                            <>
                              <table className="w-full text-sm">
                                <thead>
                                  <tr className="border-b border-slate-200 bg-slate-100">
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">ID</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">老师</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">日期</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">人气股</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">代码</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">备注</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">创建时间</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">更新时间</th>
                                    <th className="px-4 py-3 text-left font-medium text-slate-700 whitespace-nowrap">操作</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {pageItems.map((item) => (
                                    <tr
                                      key={item.id}
                                      className="border-b border-slate-200 hover:bg-slate-100/80 transition-colors"
                                    >
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.id}</td>
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.tg_name ?? '-'}</td>
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.biz_date ?? '-'}</td>
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.stock_name ?? '-'}</td>
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap">{item.stock_code ?? '-'}</td>
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap max-w-xs truncate" title={String(item.remark ?? '')}>
                                        {item.remark ?? '-'}
                                      </td>
                                      <td className="px-4 py-3 text-slate-500 whitespace-nowrap">{item.created_at ?? '-'}</td>
                                      <td className="px-4 py-3 text-slate-500 whitespace-nowrap">{item.updated_at ?? '-'}</td>
                                      <td className="px-4 py-3 text-slate-700 whitespace-nowrap">
                                        <button
                                          onClick={() => openEditMorningHotStockTrack(item)}
                                          disabled={isConfigReadOnly}
                                          className={`mr-2 px-3 py-1 text-xs font-medium ${
                                            isConfigReadOnly ? 'text-slate-400 cursor-not-allowed' : 'text-sky-600 hover:text-sky-700'
                                          }`}
                                        >
                                          修改
                                        </button>
                                        <button
                                          onClick={() => void deleteMorningHotStockTrack(item)}
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

                              <div className="px-4 py-3 border-t border-slate-200 flex flex-wrap items-center justify-between gap-2 bg-white">
                                <span className="text-slate-600 text-sm">
                                  第 {total ? start + 1 : 0}-{end} 条 / 共 {total} 条
                                </span>
                                <div className="flex items-center gap-2">
                                  <button
                                    onClick={() => setMorningHotStockTrackPage((p) => Math.max(1, p - 1))}
                                    disabled={page <= 1}
                                    className="px-3 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-700 text-sm"
                                  >
                                    上一页
                                  </button>
                                  <span className="text-slate-500 text-sm">
                                    {page} / {totalPages}
                                  </span>
                                  <button
                                    onClick={() => setMorningHotStockTrackPage((p) => Math.min(totalPages, p + 1))}
                                    disabled={page >= totalPages}
                                    className="px-3 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 disabled:cursor-not-allowed text-slate-700 text-sm"
                                  >
                                    下一页
                                  </button>
                                </div>
                              </div>
                            </>
                          )
                        })()}
                      </>
                    )}
                  </div>
                </div>

                {/* 战绩弹窗 */}
                {morningHotStockPerfOpen && (
                  <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                    onClick={() => setMorningHotStockPerfOpen(false)}
                  >
                    <div
                      className="w-full max-w-4xl bg-white rounded-xl shadow-lg border border-slate-200 p-4 mx-4 max-h-[85vh] overflow-y-auto"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="flex items-start justify-end">
                        <button
                          onClick={() => setMorningHotStockPerfOpen(false)}
                          className="text-slate-500 hover:text-slate-700"
                          aria-label="关闭"
                        >
                          ✕
                        </button>
                      </div>

                      <div className="mt-3 flex flex-wrap items-end justify-between gap-3">
                        <div className="flex flex-col gap-2">
                          <div className="text-xs text-slate-500 -mt-1">默认展示上个月，可按日期筛选</div>
                          <div className="flex flex-wrap items-end gap-3">
                            <div>
                              <div className="text-xs text-slate-600 mb-1">开始日期</div>
                              <input
                                type="date"
                                value={morningHotStockPerfStart}
                                onChange={(e) => setMorningHotStockPerfStart(e.target.value)}
                                className="px-2 py-1.5 rounded-lg border border-slate-300 text-slate-700 text-xs bg-white"
                              />
                            </div>
                            <div>
                              <div className="text-xs text-slate-600 mb-1">结束日期</div>
                              <input
                                type="date"
                                value={morningHotStockPerfEnd}
                                onChange={(e) => setMorningHotStockPerfEnd(e.target.value)}
                                className="px-2 py-1.5 rounded-lg border border-slate-300 text-slate-700 text-xs bg-white"
                              />
                            </div>
                            <button
                              type="button"
                              onClick={() => void loadMorningHotStockPerf()}
                              disabled={morningHotStockPerfLoading}
                              className="px-3 py-1.5 rounded-lg bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-800 font-medium text-xs"
                            >
                              {morningHotStockPerfLoading ? '加载中...' : '查询'}
                            </button>
                            <button
                              type="button"
                              onClick={() => void downloadMorningHotStockPerfCsv()}
                              className="px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-xs"
                              title="下载当前展示明细（CSV）"
                            >
                              下载
                            </button>
                          </div>
                        </div>
                        <div className="hidden sm:block max-w-[460px] border border-red-300 bg-red-50/60 rounded-lg px-3 py-2 text-[11px] text-slate-600 leading-snug self-end">
                          <div className="text-slate-700 font-medium mb-0.5">说明</div>
                          <div>T+1最高涨幅=([T+1当日最高价-推送当日开盘价)/推送当日开盘价</div>
                          <div>T+3日内最高涨幅=([T+3日内最高价-推送当日开盘价)/推送当日开盘价</div>
                        </div>
                      </div>

                      {morningHotStockPerfError && (
                        <div className="mt-4 p-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
                          {morningHotStockPerfError}
                        </div>
                      )}

                      <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                        <div className="p-3 bg-yellow-100 border-b border-slate-200">
                          <div className="text-center mb-2">
                            <div className="text-lg font-semibold text-red-600">
                              {(morningHotStockTrackTgName || '').trim() || '老师'}老师早评人气股
                            </div>
                          </div>
                          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 text-xs">
                            <div>
                              <div className="text-xs text-slate-500">时间</div>
                              <div className="text-red-600 font-medium">
                                {(morningHotStockPerfData?.start_date || morningHotStockPerfStart) +
                                  ' ~ ' +
                                  (morningHotStockPerfData?.end_date || morningHotStockPerfEnd)}
                              </div>
                            </div>
                            <div>
                              <div className="text-xs text-slate-500">推送个股</div>
                              <div className="text-red-600 font-medium">{morningHotStockPerfData?.push_count ?? 0}</div>
                            </div>
                            <div>
                              <div className="text-xs text-slate-500">次日涨跌幅胜率</div>
                              <div className="text-red-600 font-medium">{morningHotStockPerfData?.next_win_rate ?? '-'}</div>
                            </div>
                            <div>
                              <div className="text-xs text-slate-500">T+3日涨跌幅胜率</div>
                              <div className="text-red-600 font-medium">{morningHotStockPerfData?.t3_win_rate ?? '-'}</div>
                            </div>
                          </div>
                        </div>
                        <div className="overflow-x-auto bg-sky-50">
                          <table className="w-full text-[11px]">
                            <thead>
                              <tr className="border-b border-slate-200 bg-sky-100">
                                <th className="px-2 py-1.5 text-left font-medium text-slate-700 whitespace-nowrap">日期（买入）</th>
                                <th className="px-2 py-1.5 text-left font-medium text-slate-700 whitespace-nowrap">人气股</th>
                                <th className="px-2 py-1.5 text-left font-medium text-slate-700 whitespace-nowrap">开盘价</th>
                                <th className="px-2 py-1.5 text-left font-medium text-slate-700 whitespace-nowrap">T+1涨幅</th>
                                <th className="px-2 py-1.5 text-left font-medium text-slate-700 whitespace-nowrap">T+3最高涨幅</th>
                              </tr>
                            </thead>
                            <tbody>
                              {morningHotStockPerfLoading && (!morningHotStockPerfData || morningHotStockPerfData.items.length === 0) ? (
                                <tr>
                                  <td colSpan={5} className="px-2 py-8 text-center text-slate-500">
                                    加载中...
                                  </td>
                                </tr>
                              ) : !morningHotStockPerfData || morningHotStockPerfData.items.length === 0 ? (
                                <tr>
                                  <td colSpan={5} className="px-2 py-8 text-center text-slate-500">
                                    暂无数据
                                  </td>
                                </tr>
                              ) : (
                                morningHotStockPerfData.items.map((it, idx) => (
                                  <tr key={`${it.biz_date || 'd'}-${idx}`} className="border-b border-slate-200 hover:bg-sky-100/70 transition-colors">
                                    {(() => {
                                      const t1 = (it.t1_pct ?? '').toString()
                                      const t3 = (it.t3_pct ?? '').toString()
                                      const t1Cls = t1.startsWith('-') ? 'text-blue-600' : 'text-red-600'
                                      const t3Cls = t3.startsWith('-') ? 'text-blue-600' : 'text-red-600'
                                      return (
                                        <>
                                          <td className="px-2 py-1.5 text-slate-700 whitespace-nowrap">{it.biz_date ?? '-'}</td>
                                          <td className="px-2 py-1.5 text-slate-700 whitespace-nowrap">{it.stock_name ?? '-'}</td>
                                          <td className="px-2 py-1.5 text-slate-700 whitespace-nowrap">{it.openprice ?? '-'}</td>
                                          <td className={`px-2 py-1.5 whitespace-nowrap ${it.t1_pct ? t1Cls : 'text-slate-700'}`}>{it.t1_pct ?? '-'}</td>
                                          <td className={`px-2 py-1.5 whitespace-nowrap ${it.t3_pct ? t3Cls : 'text-slate-700'}`}>{it.t3_pct ?? '-'}</td>
                                        </>
                                      )
                                    })()}
                                  </tr>
                                ))
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ) : configTab === 'stock_position' ? (
              <div className="mt-4">
                <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                  <div className="flex items-center gap-3 flex-wrap">
                    <label className="text-sm text-slate-600">产品名称筛选（默认 短线王）：</label>
                    <select
                      value={stockPositionFilter}
                      onChange={(e) => {
                        setStockPositionFilter(e.target.value)
                        setStockPositionPage(1)
                      }}
                      onBlur={() => void loadStockPosition()}
                      className="px-3 py-2 rounded-lg border border-slate-300 text-slate-800 text-sm w-56 bg-white"
                      title="选择产品名称"
                    >
                      {/* 兜底：如果当前值不在列表里，也要能展示出来 */}
                      {stockPositionFilter.trim() &&
                        stockPositionProducts.indexOf(stockPositionFilter.trim()) < 0 && (
                          <option value={stockPositionFilter.trim()}>{stockPositionFilter.trim()}</option>
                        )}
                      {(stockPositionProducts.length ? stockPositionProducts : ['短线王']).map((p) => (
                        <option key={p} value={p}>
                          {p}
                        </option>
                      ))}
                    </select>
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
                      onClick={() => void openStockPositionPreview()}
                      className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium transition-colors"
                      title="预览：仓位明细 / 净值 / 净值明细"
                    >
                      预览
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
                          <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">序号</th>
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
                          <td className="px-3 py-2 text-xs text-slate-400">
                            自动
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
                          <tr><td colSpan={9} className="py-8 text-center text-slate-500">加载中...</td></tr>
                        ) : stockPositionItems.length === 0 ? (
                          <tr><td colSpan={9} className="py-8 text-center text-slate-500">暂无数据（默认展示产品「短线王」）</td></tr>
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
                                    value={editingStockPositionRow?.row_id ?? ''}
                                    readOnly
                                    className="w-full min-w-[40px] px-2 py-1.5 rounded border border-slate-200 text-xs bg-slate-100 text-slate-500 cursor-not-allowed text-center"
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
                              <tr
                                key={item.id}
                                className={`${getTradeDateStripeClass(item.trade_date)} border-b border-slate-200 hover:bg-slate-100/80`}
                              >
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.product_name ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{item.trade_date ?? '-'}</td>
                                <td className="px-3 py-2 text-slate-700 text-center whitespace-nowrap">{item.row_id ?? '-'}</td>
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
                        <div className="flex flex-wrap items-center justify-end gap-4 text-sm">
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
                              // 右侧留白要足够：最后一个点的数值标签向右展开时避免被 SVG 裁剪
                              const padR = 72
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

                              const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v))
                              const findPrevFinite = (arr: number[], idx: number) => {
                                for (let i = idx - 1; i >= 0; i--) if (Number.isFinite(arr[i])) return i
                                return -1
                              }
                              const findNextFinite = (arr: number[], idx: number) => {
                                for (let i = idx + 1; i < arr.length; i++) if (Number.isFinite(arr[i])) return i
                                return -1
                              }
                              // 标签必须在点上方：根据局部走向做“额外上移 + 水平错位”，尽量不挡线段
                              const labelPosFor = (arr: number[], idx: number, x: number, y: number) => {
                                const prev = findPrevFinite(arr, idx)
                                const next = findNextFinite(arr, idx)
                                const v = arr[idx]
                                if (!Number.isFinite(v)) return { x, y: clamp(y - 12, padT + 14, h - padB - 6) }
                                const prevV = prev >= 0 ? arr[prev] : NaN
                                const nextV = next >= 0 ? arr[next] : NaN
                                const inUp = Number.isFinite(prevV) ? v > prevV : false
                                const outUp = Number.isFinite(nextV) ? nextV > v : false

                                // 基础：向上抬起一点，保证在点上方
                                let dy = -12
                                // 若右侧线段向上（从该点出发上升），上方更容易被线段“擦到”，则多抬一点
                                if (outUp) dy -= 10
                                // 若两侧都向上（深 V 底部），再额外抬一点
                                if (inUp && outUp) dy -= 6

                                // 水平错位：右侧向上时向左挪，左侧向上时向右挪，减少与斜线重叠概率
                                let dx = 0
                                if (outUp && !inUp) dx = -10
                                else if (inUp && !outUp) dx = 10
                                else if (inUp && outUp) dx = -8

                                const yy = clamp(y + dy, padT + 14, h - padB - 6)
                                const xx = clamp(x + dx, padL + 6, w - padR - 6)
                                return { x: xx, y: yy }
                              }

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
                                        const anchor = 'middle'
                                        const edgeDx = 0
                                        // 最右端：放到点的右上方，利用右侧留白，避免压到左侧线段
                                        const navPos = isLast
                                          ? { x, y: clamp(y - 24, padT + 14, h - padB - 6) }
                                          : labelPosFor(navNums, idx, x, y)
                                        return (
                                          <g key={`nav-label-${idx}`}>
                                            <circle cx={x} cy={y} r={3} fill="#ef4444" stroke="#ffffff" strokeWidth={1.5} />
                                            <text
                                              x={navPos.x}
                                              y={navPos.y}
                                              dx={edgeDx}
                                              textAnchor={anchor}
                                              fontSize="12"
                                              fill="#111827"
                                              style={{ paintOrder: 'stroke', stroke: '#ffffff', strokeWidth: 3 }}
                                            >
                                              {fmt4(v)}
                                            </text>
                                            {/* 最后一天：同时标注沪深300，且避免与组合净值标签互相遮挡/挡线 */}
                                            {isLast && lastHs != null ? (() => {
                                              const hv = lastHs
                                              if (!Number.isFinite(hv)) return null
                                              const hy = yOf(hv)
                                              let hsPos = { x, y: clamp(hy - 24, padT + 14, h - padB - 6) }
                                              // 与组合净值标签过近时，强制上下错开（都在上方，所以只能“再上移”一条）
                                              if (Math.abs(hsPos.y - navPos.y) < 14) {
                                                hsPos = { ...hsPos, y: clamp(hsPos.y - 16, padT + 14, h - padB - 6) }
                                              }
                                              return (
                                                <g>
                                                  <circle cx={x} cy={hy} r={3} fill="#f59e0b" stroke="#ffffff" strokeWidth={1.5} />
                                                  <text
                                                    x={hsPos.x}
                                                    y={hsPos.y}
                                                    dx={edgeDx}
                                                    textAnchor={anchor}
                                                    fontSize="12"
                                                    fill="#111827"
                                                    style={{ paintOrder: 'stroke', stroke: '#ffffff', strokeWidth: 3 }}
                                                  >
                                                    {fmt4(hv)}
                                                  </text>
                                                </g>
                                              )
                                            })() : null}
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
                                  </div>
                                  {/* 右下角（不遮挡折线）：图例 + 最新/浮动数据（浮动放在最新日期下面） */}
                                  <div className="mt-2 flex justify-end">
                                    <div className="bg-white/90 backdrop-blur rounded-lg border border-slate-200 shadow-sm px-3 py-2 text-xs text-slate-700 max-w-[560px] min-w-[520px]">
                                      <div className="flex items-center justify-end gap-3">
                                        <div className="flex items-center gap-1.5">
                                          <span className="inline-block w-2.5 h-2.5 rounded bg-red-500" />
                                          <span>组合净值</span>
                                        </div>
                                        <div className="flex items-center gap-1.5">
                                          <span className="inline-block w-2.5 h-2.5 rounded bg-amber-500" />
                                          <span>沪深300净值</span>
                                        </div>
                                      </div>
                                      {/* 固定两列，避免 hover 时卡片尺寸变化 */}
                                      <div className="mt-1 grid grid-cols-2 gap-x-4 gap-y-1">
                                        {/* 左列：当前点 */}
                                        <div className="text-right pr-3">
                                          <div className="text-slate-500 text-[11px] whitespace-nowrap">当前日期：{hover?.date ?? '-'}</div>
                                          <div className="mt-0.5 whitespace-nowrap">
                                            <span className="text-slate-500">组合：</span>
                                            <span className="font-medium text-slate-800">{hover ? fmt4(Number(hover.nav)) : '-'}</span>
                                            <span className="ml-2 text-slate-500">沪深300：</span>
                                            <span className="font-medium text-slate-800">
                                              {hover && hover.hs300_nav != null && Number.isFinite(Number(hover.hs300_nav)) ? fmt4(Number(hover.hs300_nav)) : '-'}
                                            </span>
                                          </div>
                                        </div>
                                        {/* 右列：最新点（固定展示） */}
                                        <div className="text-right border-l border-slate-200/70 pl-3">
                                          <div className="text-slate-500 text-[11px] whitespace-nowrap">最新日期：{last.date}</div>
                                          <div className="mt-0.5 whitespace-nowrap">
                                            <span className="text-slate-500">组合：</span>
                                            <span className="font-medium text-slate-800">{lastNav == null ? '-' : fmt4(lastNav)}</span>
                                            <span className="ml-2 text-slate-500">沪深300：</span>
                                            <span className="font-medium text-slate-800">{lastHs == null ? '-' : fmt4(lastHs)}</span>
                                          </div>
                                        </div>
                                      </div>
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

            {/* 产品净值预览浮框（仓位明细 / 净值 / 净值明细） */}
            {stockPositionPreviewOpen && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                onClick={() => setStockPositionPreviewOpen(false)}
              >
                <div
                  className="w-full max-w-6xl bg-white rounded-xl shadow-lg border border-slate-200 p-5 mx-4 max-h-[85vh] overflow-y-auto"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-base font-semibold text-slate-800">
                        产品净值预览 · {getFirstProductName(stockPositionFilter)}
                      </div>
                      <div className="text-sm text-slate-500 mt-0.5">
                        共
                        {stockPositionPreviewTab === 'position'
                          ? stockPositionPreviewPositionItems.length
                          : stockPositionPreviewTab === 'nav'
                            ? stockPositionPreviewNavItems.length
                            : navDetailItems.length}
                        条
                      </div>
                    </div>
                    <button
                      onClick={() => setStockPositionPreviewOpen(false)}
                      className="text-slate-500 hover:text-slate-700"
                      aria-label="关闭"
                    >
                      ✕
                    </button>
                  </div>

                  <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => setStockPositionPreviewTab('position')}
                        className={`px-3 py-1.5 rounded-lg border text-sm font-medium transition-colors ${
                          stockPositionPreviewTab === 'position'
                            ? 'bg-sky-600 border-sky-600 text-white'
                            : 'bg-white border-slate-300 text-slate-700 hover:bg-slate-50'
                        }`}
                      >
                        仓位明细
                      </button>
                      <button
                        type="button"
                        onClick={() => setStockPositionPreviewTab('nav')}
                        className={`px-3 py-1.5 rounded-lg border text-sm font-medium transition-colors ${
                          stockPositionPreviewTab === 'nav'
                            ? 'bg-sky-600 border-sky-600 text-white'
                            : 'bg-white border-slate-300 text-slate-700 hover:bg-slate-50'
                        }`}
                      >
                        净值
                      </button>
                      <button
                        type="button"
                        onClick={() => setStockPositionPreviewTab('nav_detail')}
                        className={`px-3 py-1.5 rounded-lg border text-sm font-medium transition-colors ${
                          stockPositionPreviewTab === 'nav_detail'
                            ? 'bg-sky-600 border-sky-600 text-white'
                            : 'bg-white border-slate-300 text-slate-700 hover:bg-slate-50'
                        }`}
                        title="取 product_nav_daily_detail row_type=2 的最新 biz_date 明细"
                      >
                        净值明细
                      </button>
                    </div>

                    <button
                      type="button"
                      onClick={() => void downloadStockPositionExcel()}
                      className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium text-sm transition-colors"
                    >
                      下载 Excel
                    </button>
                  </div>

                  {stockPositionPreviewTab === 'position' && (
                    <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                      <div className="overflow-x-auto">
                        {stockPositionPreviewPositionLoading && stockPositionPreviewPositionItems.length === 0 ? (
                          <div className="py-12 text-center text-slate-500">加载中...</div>
                        ) : stockPositionPreviewPositionError ? (
                          <div className="py-12 text-center text-red-600 text-sm">{stockPositionPreviewPositionError}</div>
                        ) : stockPositionPreviewPositionItems.length === 0 ? (
                          <div className="py-12 text-center text-slate-500">暂无数据</div>
                        ) : (
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b border-slate-200 bg-slate-100">
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">日期</th>
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">序号</th>
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">股票代码</th>
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">个股</th>
                                <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">仓位(%)</th>
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">买入/卖出</th>
                                <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">成交价</th>
                              </tr>
                            </thead>
                            <tbody>
                              {stockPositionPreviewPositionItems.map((it, idx) => (
                                <tr key={`${it.id}-${idx}`} className="border-b border-slate-100 hover:bg-slate-100/70">
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.trade_date ?? '-'}</td>
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.row_id ?? '-'}</td>
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.stock_code ?? '-'}</td>
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.stock_name ?? '-'}</td>
                                  <td className="px-3 py-2 text-right text-slate-700 whitespace-nowrap">
                                    {it.position_pct != null ? `${it.position_pct}%` : '-'}
                                  </td>
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.side ?? '-'}</td>
                                  <td className="px-3 py-2 text-right text-slate-700 whitespace-nowrap">
                                    {it.price == null || !Number.isFinite(Number(it.price)) ? '-' : Number(it.price).toFixed(2)}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        )}
                      </div>
                    </div>
                  )}

                  {stockPositionPreviewTab === 'nav' && (
                    <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                      <div className="overflow-x-auto">
                        {stockPositionPreviewNavLoading && stockPositionPreviewNavItems.length === 0 ? (
                          <div className="py-12 text-center text-slate-500">加载中...</div>
                        ) : stockPositionPreviewNavError ? (
                          <div className="py-12 text-center text-red-600 text-sm">{stockPositionPreviewNavError}</div>
                        ) : stockPositionPreviewNavItems.length === 0 ? (
                          <div className="py-12 text-center text-slate-500">暂无数据</div>
                        ) : (
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b border-slate-200 bg-slate-100">
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">日期</th>
                                <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">组合净值</th>
                                <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">沪深300净值</th>
                              </tr>
                            </thead>
                            <tbody>
                              {stockPositionPreviewNavItems.map((it, idx) => (
                                <tr key={`${it.date}-${idx}`} className="border-b border-slate-100 hover:bg-slate-100/70">
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.date ?? '-'}</td>
                                  <td className="px-3 py-2 text-right text-slate-700 whitespace-nowrap">
                                    {Number.isFinite(Number(it.nav)) ? Number(it.nav).toFixed(4) : '-'}
                                  </td>
                                  <td className="px-3 py-2 text-right text-slate-700 whitespace-nowrap">
                                    {it.hs300_nav == null || !Number.isFinite(Number(it.hs300_nav)) ? '-' : Number(it.hs300_nav).toFixed(4)}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        )}
                      </div>
                    </div>
                  )}

                  {stockPositionPreviewTab === 'nav_detail' && (
                    <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                      <div className="overflow-x-auto">
                        {navDetailError ? (
                          <div className="py-12 text-center text-red-600 text-sm">{navDetailError}</div>
                        ) : navDetailLoading && navDetailItems.length === 0 ? (
                          <div className="py-12 text-center text-slate-500">加载中...</div>
                        ) : navDetailItems.length === 0 ? (
                          <div className="py-12 text-center text-slate-500">暂无数据</div>
                        ) : (
                          <table className="w-full text-sm">
                            <thead>
                              <tr className="border-b border-slate-200 bg-slate-100">
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">日期</th>
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">个股</th>
                                <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">股票代码</th>
                                <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">持仓数量</th>
                              </tr>
                            </thead>
                            <tbody>
                              {navDetailItems.map((it, idx) => (
                                <tr key={`${it.biz_date ?? 'd'}-${it.stock_code ?? 'c'}-${idx}`} className="border-b border-slate-100 hover:bg-slate-100/70">
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.biz_date ?? '-'}</td>
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.stock_name ?? '-'}</td>
                                  <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.stock_code ?? '-'}</td>
                                  <td className="px-3 py-2 text-right text-slate-700 whitespace-nowrap">
                                    {it.position_after == null ? '-' : Number(it.position_after).toFixed(4)}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {configTab === 'sales_order' && (
              <div className="mt-4">
                <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-sm text-slate-600">成交日期</span>
                    <input
                      type="date"
                      value={salesOrderMonth}
                      onChange={(e) => setSalesOrderMonth(e.target.value)}
                      className="px-3 py-2 border border-slate-300 rounded-lg text-sm bg-white"
                    />
                    <button
                      onClick={() => void loadSalesOrderConfig(1)}
                      className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-sm"
                    >
                      查询
                    </button>
                    <button onClick={() => void loadSalesOrderConfig(1, '')} className="hidden" aria-hidden="true" />
                  </div>

                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => {
                        const m = salesOrderMonth.trim() ? salesOrderMonth.trim().slice(0, 7) : ''
                        void loadSalesOrderDetail(m)
                      }}
                      className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-sm"
                    >
                      明细
                    </button>
                    <button
                      onClick={() => {
                        const m = salesOrderMonth.trim() ? salesOrderMonth.trim().slice(0, 7) : ''
                        void loadSalesOrderSummary(m)
                      }}
                      className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm"
                    >
                      汇总
                    </button>
                    <button onClick={() => refreshCurrentConfig()} className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-sm">
                      刷新
                    </button>
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    {salesOrderError ? (
                      <div className="py-10 text-center text-red-600">{salesOrderError}</div>
                    ) : salesOrderLoading && salesOrderItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">加载中...</div>
                    ) : salesOrderItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                    ) : (
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-slate-200 bg-slate-100">
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">订单编号</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">资金账号</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">产品名称</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">成交时间</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">进线月份</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">渠道</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">微信昵称</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">销售归属</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          {salesOrderItems.map((it, idx) => {
                            const key = `${it.sole_code}__${it.customer_account}__${it.product_name}`
                            return (
                              <tr key={`sales-${key}-${idx}`} className="border-b border-slate-100 hover:bg-slate-100/70">
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.sole_code}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.customer_account}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.product_name}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.pay_time || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.in_month || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.channel || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.wechat_nick || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.sales_owner || '-'}</td>
                                <td className="px-3 py-2">
                                  <button
                                    onClick={() => openSalesOrderEdit(it)}
                                    disabled={!!salesOrderSaving[key]}
                                    className="px-3 py-1 rounded bg-sky-600 hover:bg-sky-500 disabled:opacity-50 text-white text-xs"
                                  >
                                    修改
                                  </button>
                                </td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
                <div className="mt-2 px-4 text-sm text-slate-600">共 {salesOrderTotal} 条</div>

                {salesOrderEditOpen && salesOrderEditRow && (
                  <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                    onClick={() => closeSalesOrderEdit()}
                  >
                    <div
                      className="w-full max-w-3xl bg-white rounded-xl shadow-lg border border-slate-200 p-4 mx-4 max-h-[85vh] overflow-y-auto"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="mb-3 flex items-center justify-between gap-3">
                        <div className="font-semibold text-slate-800">销售订单配置 · 修改</div>
                        <button onClick={() => closeSalesOrderEdit()} className="text-slate-500 hover:text-slate-700">
                          ✕
                        </button>
                      </div>

                      {salesOrderEditError && (
                        <div className="mb-3 p-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
                          {salesOrderEditError}
                        </div>
                      )}

                      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                        <div>
                          <div className="text-xs text-slate-600 mb-1">订单编号</div>
                          <input disabled value={salesOrderEditRow.sole_code} className="w-full px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-slate-500 cursor-not-allowed" />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">资金账号</div>
                          <input disabled value={salesOrderEditRow.customer_account} className="w-full px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-slate-500 cursor-not-allowed" />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">产品名称</div>
                          <input disabled value={salesOrderEditRow.product_name} className="w-full px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-slate-500 cursor-not-allowed" />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">成交时间</div>
                          <input disabled value={salesOrderEditRow.pay_time || ''} className="w-full px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-slate-500 cursor-not-allowed" />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">进线月份</div>
                          <MonthDropdownWheelPicker
                            value={salesOrderEditForm.in_month}
                            onChange={(v) => setSalesOrderEditForm((p) => ({ ...p, in_month: v }))}
                          />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">渠道</div>
                          <input
                            value={salesOrderEditForm.channel}
                            onChange={(e) => setSalesOrderEditForm((p) => ({ ...p, channel: e.target.value }))}
                            className="w-full px-3 py-2 rounded-lg border border-slate-300 bg-white text-slate-800"
                          />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">微信昵称</div>
                          <input
                            value={salesOrderEditForm.wechat_nick}
                            onChange={(e) => setSalesOrderEditForm((p) => ({ ...p, wechat_nick: e.target.value }))}
                            className="w-full px-3 py-2 rounded-lg border border-slate-300 bg-white text-slate-800"
                          />
                        </div>
                        <div>
                          <div className="text-xs text-slate-600 mb-1">销售归属</div>
                          <input
                            value={salesOrderEditForm.sales_owner}
                            onChange={(e) => setSalesOrderEditForm((p) => ({ ...p, sales_owner: e.target.value }))}
                            className="w-full px-3 py-2 rounded-lg border border-slate-300 bg-white text-slate-800"
                          />
                        </div>
                      </div>

                      <div className="mt-4 flex items-center justify-end gap-2">
                        <button onClick={() => closeSalesOrderEdit()} className="px-3 py-2 rounded-lg border border-slate-300 text-slate-700 hover:bg-slate-50">
                          取消
                        </button>
                        <button
                          onClick={async () => {
                            if (!salesOrderEditRow) return
                            setSalesOrderEditError(null)
                            const payload: SalesOrderConfigItem = {
                              ...salesOrderEditRow,
                              in_month: salesOrderEditForm.in_month,
                              channel: salesOrderEditForm.channel,
                              wechat_nick: salesOrderEditForm.wechat_nick,
                              sales_owner: salesOrderEditForm.sales_owner,
                            }
                            const ok = await saveSalesOrderRow(payload)
                            if (ok) closeSalesOrderEdit()
                            else setSalesOrderEditError(salesOrderError || '保存失败')
                          }}
                          disabled={salesOrderLoading}
                          className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 disabled:opacity-50 text-white"
                        >
                          保存
                        </button>
                      </div>
                    </div>
                  </div>
                )}

                {salesOrderDetailOpen && (
                  <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={() => setSalesOrderDetailOpen(false)}>
                    <div className="w-full max-w-6xl bg-white rounded-xl shadow-lg border border-slate-200 p-3 mx-4 max-h-[85vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
                      <div className="mb-3 flex items-center justify-between">
                        <div className="font-semibold text-slate-800">
                          销售订单明细（
                          {salesOrderDetailMonthFilter.trim()
                            ? salesOrderDetailMonthFilter.trim()
                            : salesOrderMonth.trim()
                              ? salesOrderMonth.trim().slice(0, 7)
                              : '-'}
                          ）
                        </div>
                        <div className="flex items-center gap-2">
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-slate-600 whitespace-nowrap">月份</span>
                            <input
                              type="month"
                              value={salesOrderDetailMonthFilter}
                              onChange={(e) => setSalesOrderDetailMonthFilter(e.target.value)}
                              className="px-2 py-1 border border-slate-300 rounded-lg text-xs bg-white"
                            />
                            <button
                              onClick={() => void loadSalesOrderDetail(salesOrderDetailMonthFilter)}
                              disabled={!salesOrderDetailMonthFilter.trim()}
                              className="px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-800 text-xs"
                            >
                              查询
                            </button>
                          </div>
                          <button
                            onClick={() => {
                              const m = salesOrderDetailMonthFilter.trim()
                                ? salesOrderDetailMonthFilter.trim()
                                : salesOrderMonth.trim()
                                  ? salesOrderMonth.trim().slice(0, 7)
                                  : ''
                              void window.open(`${API_BASE}/api/config/sales-order/detail/export.csv?month=${encodeURIComponent(m)}`, '_blank')
                            }}
                            className="px-3 py-1.5 rounded bg-emerald-600 hover:bg-emerald-500 text-white text-xs whitespace-nowrap"
                          >
                            下载 CSV
                          </button>
                          <button onClick={() => setSalesOrderDetailOpen(false)} className="text-slate-500 hover:text-slate-700">✕</button>
                        </div>
                      </div>
                      <div className="rounded-xl border border-slate-200 overflow-x-auto">
                        <table className="w-full text-[11px]">
                          <thead>
                            <tr className="border-b border-slate-200 bg-slate-100">
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">客户姓名</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">资金账号</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">手机号</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">订单编号</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">产品名称</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">产品类型</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">产品归类</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">签约方式</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">支付金额</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">退款金额</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">总资产</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">成交时间</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">支付结束时间</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">客户分层</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">进线月份</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">渠道</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">微信昵称</th>
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">销售归属</th>
                            </tr>
                          </thead>
                          <tbody>
                            {salesOrderDetailLoading && salesOrderDetailItems.length === 0 ? (
                              <tr>
                                <td colSpan={18} className="px-2 py-8 text-center text-slate-500">
                                  加载中...
                                </td>
                              </tr>
                            ) : salesOrderDetailItems.length === 0 ? (
                              <tr>
                                <td colSpan={18} className="px-2 py-8 text-center text-slate-500">
                                  暂无数据
                                </td>
                              </tr>
                            ) : (
                              salesOrderDetailItems.map((it, idx) => (
                              <tr key={`sod-${idx}`} className="border-b border-slate-100">
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.customer_name || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.customer_account || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.customer_phone || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.sole_code || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.product_name || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.product_type || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.product_class || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.sign_method || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">
                                  {it.pay_amount_display == null || !Number.isFinite(Number(it.pay_amount_display)) ? '-' : Number(it.pay_amount_display).toFixed(4)}
                                </td>
                                <td className="px-2 py-1.5 whitespace-nowrap">
                                  {it.refund_amount == null || !Number.isFinite(Number(it.refund_amount)) ? '-' : Number(it.refund_amount).toFixed(4)}
                                </td>
                                <td className="px-2 py-1.5 whitespace-nowrap">
                                  {it.curr_total_asset == null || !Number.isFinite(Number(it.curr_total_asset)) ? '-' : Number(it.curr_total_asset).toFixed(4)}
                                </td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.pay_time || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.pay_time_end || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.customer_layer || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.in_month || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.channel || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.wechat_nick || '-'}</td>
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.sales_owner || '-'}</td>
                              </tr>
                              ))
                            )}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                )}

                {salesOrderSummaryOpen && (
                  <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={() => setSalesOrderSummaryOpen(false)}>
                    <div className="w-full max-w-3xl bg-white rounded-xl shadow-lg border border-slate-200 p-3 mx-4 max-h-[85vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
                      <div className="mb-3 flex items-center justify-between">
                        <div className="font-semibold text-slate-800">
                          销售订单汇总（
                          {salesOrderSummaryMonthFilter.trim()
                            ? salesOrderSummaryMonthFilter.trim()
                            : salesOrderMonth.trim()
                              ? salesOrderMonth.trim().slice(0, 7)
                              : '-'}
                          ）
                        </div>
                        <div className="flex items-center gap-2">
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-slate-600 whitespace-nowrap">月份</span>
                            <input
                              type="month"
                              value={salesOrderSummaryMonthFilter}
                              onChange={(e) => setSalesOrderSummaryMonthFilter(e.target.value)}
                              className="px-2 py-1 border border-slate-300 rounded-lg text-xs bg-white"
                            />
                            <button
                              onClick={() => void loadSalesOrderSummary(salesOrderSummaryMonthFilter)}
                              disabled={!salesOrderSummaryMonthFilter.trim()}
                              className="px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 disabled:opacity-50 text-slate-800 text-xs"
                            >
                              查询
                            </button>
                          </div>
                          <button
                            onClick={() => {
                              const m = salesOrderSummaryMonthFilter.trim()
                                ? salesOrderSummaryMonthFilter.trim()
                                : salesOrderMonth.trim()
                                  ? salesOrderMonth.trim().slice(0, 7)
                                  : ''
                              void window.open(`${API_BASE}/api/config/sales-order/summary/export.csv?month=${encodeURIComponent(m)}`, '_blank')
                            }}
                            className="px-3 py-1.5 rounded bg-emerald-600 hover:bg-emerald-500 text-white text-xs whitespace-nowrap"
                          >
                            下载 CSV
                          </button>
                          <button onClick={() => setSalesOrderSummaryOpen(false)} className="text-slate-500 hover:text-slate-700">✕</button>
                        </div>
                      </div>
                      <div className="rounded-xl border border-slate-200 overflow-x-auto">
                        <table className="w-full text-[11px]">
                          <thead>
                            <tr className="border-b border-slate-200 bg-slate-100">
                              <th className="px-2 py-1.5 text-left whitespace-nowrap">销售归属</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">升佣订单数</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">现金订单数</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">总计订单数</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">现金订单额</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">新签订单数</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">续期订单数</th>
                              <th className="px-2 py-1.5 text-right whitespace-nowrap">复购订单数</th>
                            </tr>
                          </thead>
                          <tbody>
                            {salesOrderSummaryLoading && salesOrderSummaryItems.length === 0 ? <tr><td colSpan={8} className="px-2 py-8 text-center text-slate-500">加载中...</td></tr> : salesOrderSummaryItems.length === 0 ? <tr><td colSpan={8} className="px-2 py-8 text-center text-slate-500">暂无数据</td></tr> : salesOrderSummaryItems.map((it, idx) => (
                              <tr key={`sos-${idx}`} className="border-b border-slate-100">
                                <td className="px-2 py-1.5 whitespace-nowrap">{it.sales_owner || '-'}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{Number(it.commission_count || 0)}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{Number(it.cash_count || 0)}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{Number(it.total_count || 0)}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{it.cash_amount == null || !Number.isFinite(Number(it.cash_amount)) ? '-' : Number(it.cash_amount).toFixed(4)}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{Number(it.new_count || 0)}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{Number(it.renew_count || 0)}</td>
                                <td className="px-2 py-1.5 text-right whitespace-nowrap">{Number(it.repurchase_count || 0)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}

            {configTab === 'sign_customer_group' && (
              <div className="mt-4">
                <div className="mb-3 flex items-center gap-2">
                  <span className="text-sm text-slate-600">到期月份</span>
                  <input
                    type="month"
                    value={signCustomerGroupMonth}
                    onChange={(e) => setSignCustomerGroupMonth(e.target.value)}
                    className="px-3 py-2 border border-slate-300 rounded-lg text-sm"
                  />
                  <button
                    type="button"
                    onClick={() => void loadSignCustomerGroup(1)}
                    className="px-4 py-2 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-sm"
                  >
                    查询
                  </button>
                  <button
                    type="button"
                    onClick={() => void loadSignCustomerGroup(signCustomerGroupPage)}
                    className="px-4 py-2 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-sm"
                  >
                    刷新
                  </button>
                </div>
                <div className="rounded-xl border border-slate-200 bg-slate-50/50 overflow-hidden">
                  <div className="overflow-x-auto">
                    {signCustomerGroupError ? (
                      <div className="py-10 text-center text-red-600">{signCustomerGroupError}</div>
                    ) : signCustomerGroupLoading && signCustomerGroupItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">加载中...</div>
                    ) : signCustomerGroupItems.length === 0 ? (
                      <div className="py-10 text-center text-slate-500">暂无配置数据</div>
                    ) : (
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-slate-200 bg-slate-100">
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">订单支付日期</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">姓名</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">资金账号</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">手机号</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">微信名称</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">订单编号</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">升佣/现金</th>
                            <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">总资产</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">到期日期</th>
                            <th className="px-3 py-2 text-right font-medium text-slate-700 whitespace-nowrap">退订金额</th>
                            <th className="px-3 py-2 text-left font-medium text-slate-700 whitespace-nowrap">是否进群</th>
                          </tr>
                        </thead>
                        <tbody>
                          {signCustomerGroupItems.map((it, idx) => {
                            const key = `${String(it.sole_code || '')}__${String(it.customer_account || '')}`
                            return (
                              <tr key={`sign-${key}-${idx}`} className="border-b border-slate-100 hover:bg-slate-100/70">
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.pay_time || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.customer_name || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.customer_account || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{maskPhone(it.customer_phone)}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.wechat_nick || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.sole_code || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.sign_type || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap text-right">
                                  {it.curr_total_asset == null || !Number.isFinite(Number(it.curr_total_asset)) ? '-' : Number(it.curr_total_asset).toFixed(2)}
                                </td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">{it.pay_time_end || '-'}</td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap text-right">
                                  {it.refund_amount == null || !Number.isFinite(Number(it.refund_amount)) ? '-' : Number(it.refund_amount).toFixed(2)}
                                </td>
                                <td className="px-3 py-2 text-slate-700 whitespace-nowrap">
                                  <select
                                    className="px-2 py-1 border border-slate-300 rounded"
                                    value={String(it.in_group ?? 0)}
                                    disabled={!!signRowSaving[key]}
                                    onChange={(e) => {
                                      const v = Number(e.target.value)
                                      setSignCustomerGroupItems((prev) => prev.map((r, i) => (i === idx ? { ...r, in_group: v } : r)))
                                      void saveSignCustomerGroupRow({ ...it, in_group: v }, v)
                                    }}
                                  >
                                    <option value="0">否</option>
                                    <option value="1">是</option>
                                  </select>
                                </td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
                <div className="mt-3 flex items-center justify-between text-sm text-slate-600">
                  <div>共 {signCustomerGroupTotal} 条</div>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => void loadSignCustomerGroup(Math.max(1, signCustomerGroupPage - 1))}
                      disabled={signCustomerGroupPage <= 1 || signCustomerGroupLoading}
                      className="px-3 py-1 rounded border border-slate-300 disabled:opacity-50"
                    >
                      上一页
                    </button>
                    <span>第 {signCustomerGroupPage} / {Math.max(1, Math.ceil(signCustomerGroupTotal / SIGN_CUSTOMER_GROUP_PAGE_SIZE))} 页</span>
                    <button
                      type="button"
                      onClick={() => void loadSignCustomerGroup(signCustomerGroupPage + 1)}
                      disabled={signCustomerGroupLoading || signCustomerGroupPage * SIGN_CUSTOMER_GROUP_PAGE_SIZE >= signCustomerGroupTotal}
                      className="px-3 py-1 rounded border border-slate-300 disabled:opacity-50"
                    >
                      下一页
                    </button>
                  </div>
                </div>
              </div>
            )}

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
                      {/* {codeMappingModal === 'edit' ? '修改[市场中心]抖音投流账号' : '新增[市场中心]抖音投流账号'} */}
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
                    {/* {openModal === 'edit' ? '修改配置' : '新增配置'} · 开户渠道 & 企微客户标签 */}
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
                    {/* {staffModal === 'edit' ? '修改配置' : '新增配置'} · 投流渠道承接员工 */}
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

            {/* 客户中心-商机线索配置 弹窗 */}
            {opportunityLeadModal && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                onClick={() => {
                  setOpportunityLeadModal(null)
                  setEditingOpportunityLead(null)
                  setOpportunityLeadForm({
                    biz_category_big: '',
                    biz_category_small: '',
                    clue_name: '',
                    is_important: '0',
                    remark: '',
                    table_name: '',
                  })
                }}
              >
                <div
                  className="rounded-xl border border-slate-200 bg-white p-4 shadow-xl w-full max-w-lg mx-4"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h2 className="text-sm font-semibold text-slate-800 mb-3">商机线索配置</h2>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">业务大类</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={opportunityLeadForm.biz_category_big}
                        onChange={(e) => setOpportunityLeadForm((p) => ({ ...p, biz_category_big: e.target.value }))}
                        placeholder="例如：直销/市场/投顾"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">业务小类</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={opportunityLeadForm.biz_category_small}
                        onChange={(e) => setOpportunityLeadForm((p) => ({ ...p, biz_category_small: e.target.value }))}
                        placeholder="例如：渠道/活动"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5 sm:col-span-2">
                      <span className="text-[11px] text-slate-500">线索名称</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={opportunityLeadForm.clue_name}
                        onChange={(e) => setOpportunityLeadForm((p) => ({ ...p, clue_name: e.target.value }))}
                        placeholder="请输入线索名称"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">是否重要</span>
                      <select
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={opportunityLeadForm.is_important}
                        onChange={(e) => setOpportunityLeadForm((p) => ({ ...p, is_important: e.target.value as '1' | '0' }))}
                      >
                        <option value="0">否</option>
                        <option value="1">是</option>
                      </select>
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">表名</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={opportunityLeadForm.table_name}
                        onChange={(e) => setOpportunityLeadForm((p) => ({ ...p, table_name: e.target.value }))}
                        placeholder="例如：mv_xxx 或 ods_xxx"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5 sm:col-span-2">
                      <span className="text-[11px] text-slate-500">备注</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={opportunityLeadForm.remark}
                        onChange={(e) => setOpportunityLeadForm((p) => ({ ...p, remark: e.target.value }))}
                        placeholder="可选"
                      />
                    </div>
                  </div>
                  <div className="flex gap-2 mt-4 justify-end">
                    <button
                      onClick={() => {
                        setOpportunityLeadModal(null)
                        setEditingOpportunityLead(null)
                      }}
                      className="px-3 py-1.5 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-xs font-medium"
                    >
                      取消
                    </button>
                    <button
                      onClick={() => void saveOpportunityLead()}
                      disabled={isConfigReadOnly}
                      className="px-3 py-1.5 rounded-lg bg-sky-600 hover:bg-sky-500 text-white text-xs font-medium"
                    >
                      保存
                    </button>
                  </div>
                </div>
              </div>
            )}

            {/* 客户中心-早盘人气股战绩追踪配置 弹窗 */}
            {morningHotStockTrackModal && (
              <div
                className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
                onClick={() => {
                  setMorningHotStockTrackModal(null)
                  setEditingMorningHotStockTrack(null)
                  setMorningHotStockTrackForm({
                    tg_name: morningHotStockTrackTgName.trim() || '胡晶翔',
                    biz_date: '',
                    stock_name: '',
                    stock_code: '',
                    remark: '',
                  })
                }}
              >
                <div
                  className="rounded-xl border border-slate-200 bg-white p-4 shadow-xl w-full max-w-lg mx-4"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h2 className="text-sm font-semibold text-slate-800 mb-3">早盘人气股战绩追踪配置</h2>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">老师</span>
                      <input
                        disabled
                        className={`px-2.5 py-1.5 rounded-lg border text-xs focus:outline-none focus:ring-2 focus:ring-sky-500 ${
                          'bg-slate-100 border-slate-300 text-slate-400 cursor-not-allowed pointer-events-none'
                        }`}
                        value={morningHotStockTrackForm.tg_name}
                        onChange={(e) => setMorningHotStockTrackForm((p) => ({ ...p, tg_name: e.target.value }))}
                        placeholder="例如：胡晶翔"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">日期</span>
                      <input
                        type="date"
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={morningHotStockTrackForm.biz_date}
                        onChange={(e) => setMorningHotStockTrackForm((p) => ({ ...p, biz_date: e.target.value }))}
                        placeholder="YYYY-MM-DD"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5">
                      <span className="text-[11px] text-slate-500">代码</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={morningHotStockTrackForm.stock_code}
                        onChange={(e) => setMorningHotStockTrackForm((p) => ({ ...p, stock_code: e.target.value }))}
                        placeholder="例如：000001 / 600000"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5 sm:col-span-2">
                      <span className="text-[11px] text-slate-500">人气股</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={morningHotStockTrackForm.stock_name}
                        onChange={(e) => setMorningHotStockTrackForm((p) => ({ ...p, stock_name: e.target.value }))}
                        placeholder="请输入股票名称"
                      />
                    </div>
                    <div className="flex flex-col gap-0.5 sm:col-span-2">
                      <span className="text-[11px] text-slate-500">备注</span>
                      <input
                        className="px-2.5 py-1.5 rounded-lg bg-slate-50 border border-slate-300 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-sky-500"
                        value={morningHotStockTrackForm.remark}
                        onChange={(e) => setMorningHotStockTrackForm((p) => ({ ...p, remark: e.target.value }))}
                        placeholder="可选"
                      />
                    </div>
                  </div>
                  <div className="flex gap-2 mt-4 justify-end">
                    <button
                      onClick={() => {
                        setMorningHotStockTrackModal(null)
                        setEditingMorningHotStockTrack(null)
                      }}
                      className="px-3 py-1.5 rounded-lg bg-slate-200 hover:bg-slate-300 text-slate-800 text-xs font-medium"
                    >
                      取消
                    </button>
                    <button
                      onClick={() => void saveMorningHotStockTrack()}
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
