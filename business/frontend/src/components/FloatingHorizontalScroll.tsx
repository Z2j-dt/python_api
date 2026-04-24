import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

type Props = {
  children: ReactNode
  /** 合并到横向滚动层上的 class（如圆角、边框、背景） */
  trackClassName?: string
  /**
   * 视口底部同步滚动条的 z-index。
   * 普通页面默认 45（低于业务弹窗 z-50）；放在 fixed z-50 弹窗内时请传 60 以免被挡住。
   */
  mirrorZIndex?: number
}

/**
 * 宽表横向滚动：在视口底部增加一条与主区域同步的横向滚动条，
 * 避免用户必须先纵向滚到页面最底才能横向拖动。
 */
export function FloatingHorizontalScroll({ children, trackClassName = '', mirrorZIndex = 45 }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const trackRef = useRef<HTMLDivElement>(null)
  const thumbRef = useRef<HTMLDivElement>(null)
  const draggingRef = useRef(false)
  const dragStartXRef = useRef(0)
  const dragStartLeftRef = useRef(0)
  const [showMirror, setShowMirror] = useState(false)
  const [thumbLeft, setThumbLeft] = useState(0)
  const [thumbWidth, setThumbWidth] = useState(24)

  const updateMetrics = useCallback(() => {
    const el = scrollRef.current
    if (!el) return
    const need = el.scrollWidth > el.clientWidth + 2
    setShowMirror(need)

    const track = trackRef.current
    if (!track) return
    const trackW = track.clientWidth
    const clientW = el.clientWidth
    const scrollW = el.scrollWidth
    const maxScroll = Math.max(1, scrollW - clientW)
    const ratio = clientW / scrollW
    const w = Math.max(36, Math.min(trackW, Math.floor(trackW * ratio)))
    setThumbWidth(w)
    const maxLeft = Math.max(1, trackW - w)
    const left = Math.round((el.scrollLeft / maxScroll) * maxLeft)
    setThumbLeft(Number.isFinite(left) ? left : 0)
  }, [])

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    updateMetrics()
    const ro = new ResizeObserver(() => updateMetrics())
    ro.observe(el)
    const track = trackRef.current
    if (track) ro.observe(track)
    const mo = new MutationObserver(() => updateMetrics())
    mo.observe(el, { childList: true, subtree: true, attributes: true, characterData: true })
    const onWin = () => updateMetrics()
    window.addEventListener('resize', onWin)
    return () => {
      ro.disconnect()
      mo.disconnect()
      window.removeEventListener('resize', onWin)
    }
  }, [updateMetrics])

  const onMainScroll = useCallback(() => {
    if (draggingRef.current) return
    updateMetrics()
  }, [updateMetrics])

  const setScrollLeftByThumb = useCallback((nextLeft: number) => {
    const el = scrollRef.current
    const track = trackRef.current
    if (!el || !track) return
    const trackW = track.clientWidth
    const maxLeft = Math.max(1, trackW - thumbWidth)
    const clampedLeft = Math.max(0, Math.min(maxLeft, nextLeft))
    const clientW = el.clientWidth
    const scrollW = el.scrollWidth
    const maxScroll = Math.max(1, scrollW - clientW)
    const nextScrollLeft = (clampedLeft / maxLeft) * maxScroll
    el.scrollLeft = nextScrollLeft
    setThumbLeft(clampedLeft)
  }, [])

  const onThumbPointerDown = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    if (e.button !== 0) return
    draggingRef.current = true
    dragStartXRef.current = e.clientX
    dragStartLeftRef.current = thumbLeft
    try {
      ;(e.target as HTMLElement).setPointerCapture(e.pointerId)
    } catch {
      // ignore
    }
  }, [thumbLeft])

  const onThumbPointerMove = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    if (!draggingRef.current) return
    const dx = e.clientX - dragStartXRef.current
    setScrollLeftByThumb(dragStartLeftRef.current + dx)
  }, [setScrollLeftByThumb])

  const onThumbPointerUp = useCallback(() => {
    draggingRef.current = false
  }, [])

  const mirrorEl =
    showMirror && typeof document !== 'undefined' ? (
      <div
        className="fixed bottom-0 left-0 right-0 border-t border-slate-200/90 bg-white/95 backdrop-blur-sm shadow-[0_-6px_16px_rgba(15,23,42,0.07)] pb-[env(safe-area-inset-bottom,0px)] pointer-events-auto"
        style={{ zIndex: mirrorZIndex }}
        aria-hidden
      >
        <div className="px-3 py-2">
          <div className="flex items-center justify-between">
            <div className="text-[11px] text-slate-500 select-none">左右拖动查看隐藏列</div>
            <div className="text-[11px] text-slate-400 select-none">横向滚动</div>
          </div>
          <div
            ref={trackRef}
            className="mt-1 h-3 w-full rounded-full bg-slate-200/80 relative"
            onPointerDown={(e) => {
              // 点击轨道：直接跳转到对应位置
              const track = trackRef.current
              if (!track) return
              const rect = track.getBoundingClientRect()
              const x = e.clientX - rect.left
              const nextLeft = x - thumbWidth / 2
              setScrollLeftByThumb(nextLeft)
            }}
          >
            <div
              ref={thumbRef}
              className="absolute top-0 h-3 rounded-full bg-slate-500/70 hover:bg-slate-600/80 active:bg-slate-700/80 cursor-grab active:cursor-grabbing"
              style={{ left: thumbLeft, width: thumbWidth }}
              onPointerDown={onThumbPointerDown}
              onPointerMove={onThumbPointerMove}
              onPointerUp={onThumbPointerUp}
              onPointerCancel={onThumbPointerUp}
            />
          </div>
        </div>
      </div>
    ) : null

  return (
    <>
      <div
        ref={scrollRef}
        className={`overflow-x-auto min-w-0 ${trackClassName}`.trim()}
        onScroll={onMainScroll}
      >
        {children}
      </div>
      {showMirror ? <div className="h-8 shrink-0" aria-hidden /> : null}
      {mirrorEl && createPortal(mirrorEl, document.body)}
    </>
  )
}
