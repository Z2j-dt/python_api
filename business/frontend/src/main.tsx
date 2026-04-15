import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

const THEME_KEY = 'portal.theme.mode'
const normalizeTheme = (v?: string | null) => (v === 'dark' ? 'dark' : 'light')

function applyTheme(mode: 'light' | 'dark') {
  try {
    document.documentElement.setAttribute('data-theme', mode)
    document.body.setAttribute('data-theme', mode)
  } catch {
    // ignore
  }
}

function initThemeSync() {
  let mode: 'light' | 'dark' = 'light'
  try {
    const q = new URLSearchParams(window.location.search).get('theme')
    if (q === 'dark' || q === 'light') {
      mode = q
      localStorage.setItem(THEME_KEY, q)
    } else {
      const saved = localStorage.getItem(THEME_KEY)
      mode = normalizeTheme(saved)
    }
  } catch {
    // ignore
  }
  applyTheme(mode)

  window.addEventListener('message', (evt) => {
    try {
      if (evt.source !== window.parent) return
      const payload = evt.data as { type?: string; theme?: string } | null
      if (!payload || payload.type !== 'portal-theme-change') return
      const next = normalizeTheme(payload.theme)
      applyTheme(next)
      localStorage.setItem(THEME_KEY, next)
    } catch {
      // ignore
    }
  })
}

initThemeSync()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
