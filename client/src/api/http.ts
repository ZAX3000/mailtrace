// src/api/http.ts
import axios from 'axios'
import type { AxiosError, AxiosRequestConfig, AxiosResponse } from 'axios'
import { log, getReq } from '@/utils/logger' // ← tiny logger we added

const baseURL = import.meta.env.VITE_API_BASE || '/api'

export const http = axios.create({
  baseURL,
  withCredentials: true,
  timeout: 60_000,
  headers: { 'X-Requested-With': 'XMLHttpRequest' },
})

/**
 * REQUEST interceptor
 * - Injects X-Request-ID (set by useRun/newReqId)
 * - Logs outgoing request + payload (non-GET)
 */
http.interceptors.request.use((cfg) => {
  const reqId = getReq()
  if (reqId) {
    if (cfg.headers && typeof (cfg.headers as any).set === 'function') {
      // AxiosHeaders instance
      ;(cfg.headers as any).set('X-Request-ID', reqId)
    } else {
      // Plain object case
      cfg.headers = { ...(cfg.headers as any), 'X-Request-ID': reqId } as any
    }
  }

  const url = (cfg.baseURL || '') + (cfg.url || '')
  log.info('HTTP → request', { method: cfg.method, url, reqId })
  if (cfg.method?.toUpperCase() !== 'GET' && cfg.data !== undefined) {
    log.debug('HTTP → payload', { data: cfg.data })
  }
  return cfg
})

/**
 * RESPONSE interceptor
 * - Logs successes and normalizes errors to Error with .status and .data
 */
http.interceptors.response.use(
  (res: AxiosResponse) => {
    log.info('HTTP ← response', { url: res.config?.url, status: res.status })
    return res
  },
  (err: AxiosError) => {
    const status = err.response?.status ?? 0
    const data: any = err.response?.data
    log.error('HTTP ← error', {
      url: err.config?.url,
      status,
      data,
      message: err.message,
    })

    let msg = 'Network error'
    if (status) msg = `${status} ${err.response?.statusText || ''}`.trim()
    if (data?.error) msg = `${msg}: ${data.error}`
    if (data?.message) msg = `${msg} — ${data.message}`

    const e = new Error(msg) as Error & { status?: number; data?: any }
    e.status = status
    e.data = data
    throw e
  }
)

// ---- data-only helpers (return response.data)
export async function api<T = any>(
  path: string,
  cfg?: AxiosRequestConfig
): Promise<T> {
  const res = await http.request<T>({ url: path, method: 'GET', ...cfg })
  return res.data as T
}

export const get =   <T = any>(path: string, cfg?: AxiosRequestConfig) => api<T>(path, { ...cfg, method: 'GET' })
export const postJson = <T = any>(path: string, body?: any, cfg?: AxiosRequestConfig) =>
  api<T>(path, { method: 'POST', data: body ?? {}, headers: { 'Content-Type': 'application/json', ...(cfg?.headers || {}) }, ...cfg })
export const postForm = <T = any>(path: string, form: FormData, cfg?: AxiosRequestConfig) =>
  api<T>(path, { method: 'POST', data: form, ...cfg })
export const putJson  = <T = any>(path: string, body?: any, cfg?: AxiosRequestConfig) =>
  api<T>(path, { method: 'PUT', data: body ?? {}, headers: { 'Content-Type': 'application/json', ...(cfg?.headers || {}) }, ...cfg })
export const del_     = <T = any>(path: string, cfg?: AxiosRequestConfig) => api<T>(path, { ...cfg, method: 'DELETE' })

// ---- raw-response helpers (return AxiosResponse so you can check .status)
export const getRaw   = <T = any>(path: string, cfg?: AxiosRequestConfig) => http.get<T>(path, cfg)
export const post     = <T = any>(path: string, body?: any, cfg?: AxiosRequestConfig) => http.post<T>(path, body ?? {}, cfg)
export const putRaw   = <T = any>(path: string, body?: any, cfg?: AxiosRequestConfig) => http.put<T>(path, body ?? {}, cfg)
export const deleteRaw= <T = any>(path: string, cfg?: AxiosRequestConfig) => http.delete<T>(path, cfg)
