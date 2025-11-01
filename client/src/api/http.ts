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

// --- Interceptors (loader is opt-in; disabled by default)
http.interceptors.request.use((cfg) => {
  return cfg
})

http.interceptors.response.use(
  (res: AxiosResponse) => {
    return res
  },
  (err: AxiosError) => {

    let msg = 'Network error'
    const status = err.response?.status
    const data: any = err.response?.data

    if (status) msg = `${status} ${err.response?.statusText || ''}`.trim()
    if (data?.error) msg = `${msg}: ${data.error}`
    if (data?.message) msg = `${msg} — ${data.message}`

    const e = new Error(msg)
    ;(e as any).status = status
    ;(e as any).data = data
    throw e
  }
)

// ---- Generic API (keeps your old call-site)
export async function api<T = any>(path: string, cfg?: AxiosRequestConfig): Promise<T> {
  const res = await http.request<T>({ url: path, method: 'GET', ...cfg })
  return res.data as T
}

// ---- Helpers
export const get = <T = any>(path: string, cfg?: AxiosRequestConfig) =>
  api<T>(path, { ...cfg, method: 'GET' })

export const postJson = <T = any>(path: string, body?: any, cfg?: AxiosRequestConfig) =>
  api<T>(path, {
    method: 'POST',
    data: body ?? {},
    headers: { 'Content-Type': 'application/json', ...(cfg?.headers || {}) },
    ...cfg,
  })

export const postForm = <T = any>(path: string, form: FormData, cfg?: AxiosRequestConfig) =>
  api<T>(path, { method: 'POST', data: form, ...cfg })

export const putJson = <T = any>(path: string, body?: any, cfg?: AxiosRequestConfig) =>
  api<T>(path, {
    method: 'PUT',
    data: body ?? {},
    headers: { 'Content-Type': 'application/json', ...(cfg?.headers || {}) },
    ...cfg,
  })

export const del_ = <T = any>(path: string, cfg?: AxiosRequestConfig) =>
  api<T>(path, { ...cfg, method: 'DELETE' })
