// src/api/http.ts
import axios from 'axios'
import type { AxiosError, AxiosRequestConfig, AxiosResponse } from 'axios'

const baseURL = import.meta.env.VITE_API_BASE || '/api'

export const http = axios.create({
  baseURL,
  withCredentials: true,
  timeout: 60_000,
  headers: { 'X-Requested-With': 'XMLHttpRequest' },
})

// --- Interceptors (keep lightweight; loader handled by composables)
http.interceptors.request.use((cfg) => {
  // place to inject headers (e.g., run_id) if needed later
  return cfg
})

http.interceptors.response.use(
  (res: AxiosResponse) => res,
  (err: AxiosError) => {
    // Normalize all errors to a thrown Error with .status and .data
    const status = err.response?.status ?? 0
    const data: any = err.response?.data
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

// ---- Generic API (returns response.data like before)
export async function api<T = any>(
  path: string,
  cfg?: AxiosRequestConfig
): Promise<T> {
  const res = await http.request<T>({ url: path, method: 'GET', ...cfg })
  return res.data as T
}

// ---- “data-only” helpers (keep existing call-sites working)
export const get = <T = any>(path: string, cfg?: AxiosRequestConfig) =>
  api<T>(path, { ...cfg, method: 'GET' })

export const postJson = <T = any>(
  path: string,
  body?: any,
  cfg?: AxiosRequestConfig
) =>
  api<T>(path, {
    method: 'POST',
    data: body ?? {},
    headers: { 'Content-Type': 'application/json', ...(cfg?.headers || {}) },
    ...cfg,
  })

export const postForm = <T = any>(
  path: string,
  form: FormData,
  cfg?: AxiosRequestConfig
) => api<T>(path, { method: 'POST', data: form, ...cfg })

export const putJson = <T = any>(
  path: string,
  body?: any,
  cfg?: AxiosRequestConfig
) =>
  api<T>(path, {
    method: 'PUT',
    data: body ?? {},
    headers: { 'Content-Type': 'application/json', ...(cfg?.headers || {}) },
    ...cfg,
  })

export const del_ = <T = any>(path: string, cfg?: AxiosRequestConfig) =>
  api<T>(path, { ...cfg, method: 'DELETE' })

// ---- “raw response” helpers (return AxiosResponse so you can read .status)
export const getRaw = <T = any>(path: string, cfg?: AxiosRequestConfig) =>
  http.get<T>(path, cfg)

export const post = <T = any>(
  path: string,
  body?: any,
  cfg?: AxiosRequestConfig
) => http.post<T>(path, body ?? {}, cfg)

export const putRaw = <T = any>(
  path: string,
  body?: any,
  cfg?: AxiosRequestConfig
) => http.put<T>(path, body ?? {}, cfg)

export const deleteRaw = <T = any>(path: string, cfg?: AxiosRequestConfig) =>
  http.delete<T>(path, cfg)