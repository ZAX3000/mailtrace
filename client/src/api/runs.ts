// src/api/runs.ts
export type StartRunResponse =
  | { kind: 'ok' }
  | { kind: 'needs-mapping'; missing: Record<string, string[]> }
  | { kind: 'error'; status?: number; message?: string }

export type RunStatus = {
  run_id?: string
  status?: 'queued' | 'matching' | 'aggregating' | 'done' | 'failed'
  step?: string
  message?: string
  pct?: number
}

export type RunItem = {
  id: string
  started_at: string
  status?: 'queued' | 'matching' | 'aggregating' | 'done' | 'failed'
  summary?: string | null
}

const API = (import.meta as any)?.env?.VITE_API_BASE?.toString?.() || ''

async function safeParse<T>(res: Response): Promise<T | string> {
  const text = await res.text()
  try { return JSON.parse(text) as T } catch { return text }
}

function url(path: string) {
  return API ? `${API}${path}` : path
}

// ---------------------------
// Existing functions (kept)
// ---------------------------
export async function startRun(runId: string): Promise<StartRunResponse> {
  const res = await fetch(url(`/api/runs/${encodeURIComponent(runId)}/start`), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
  })

  const data = await safeParse<{ message?: string; missing?: Record<string, string[]> }>(res)

  if (res.status === 409 && typeof data !== 'string' && data?.missing) {
    return { kind: 'needs-mapping', missing: data.missing }
  }

  if (!res.ok) {
    return {
      kind: 'error',
      status: res.status,
      message:
        typeof data === 'string'
          ? (data || 'Start failed')
          : (data?.message || 'Start failed'),
    }
  }

  return { kind: 'ok' }
}

export async function getRunStatus(runId: string): Promise<RunStatus> {
  const res = await fetch(url(`/api/runs/${encodeURIComponent(runId)}/status`), {
    method: 'GET',
    headers: { Accept: 'application/json' },
    cache: 'no-store',
    credentials: 'same-origin',
  })

  if (!res.ok) {
    return {
      status: 'failed',
      message: `Status ${res.status} while polling`,
      pct: 100,
    }
  }

  const data = await safeParse<RunStatus>(res)
  return (typeof data === 'string'
    ? { status: 'failed', message: data, pct: 100 }
    : data) as RunStatus
}

export async function createRun(): Promise<{ run_id: string }> {
  const res = await fetch(url(`/api/runs`), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
  })
  const data = await safeParse<{ run_id: string }>(res)
  if (!res.ok || typeof data === 'string' || !data?.run_id) {
    throw new Error(typeof data === 'string' ? data : 'Failed to create run')
  }
  return data
}

// ---------------------------
// History helpers
// ---------------------------
export async function fetchRuns(limit = 25, before?: string): Promise<RunItem[]> {
  const q = new URLSearchParams({ limit: String(limit) })
  if (before) q.set('before', before)
  const res = await fetch(url(`/api/runs?${q.toString()}`), {
    method: 'GET',
    credentials: 'same-origin',
  })
  const data = await safeParse<{ items?: RunItem[] }>(res)
  if (!res.ok || typeof data === 'string') {
    throw new Error(typeof data === 'string' ? data : `Runs list failed: ${res.status}`)
  }
  return Array.isArray(data.items) ? data.items : []
}

export async function fetchLatestRun(onlyDone = false): Promise<RunItem | null> {
  const q = onlyDone ? '?require=done' : ''
  const res = await fetch(url(`/api/runs/latest${q}`), {
    method: 'GET',
    credentials: 'same-origin',
  })
  if (res.status === 204) return null
  const data = await safeParse<RunItem>(res)
  if (!res.ok || typeof data === 'string') {
    throw new Error(typeof data === 'string' ? data : `Latest run failed: ${res.status}`)
  }
  return data
}

export async function activateRun(runId: string): Promise<boolean> {
  const res = await fetch(url(`/api/runs/${encodeURIComponent(runId)}/activate`), {
    method: 'POST',
    credentials: 'same-origin',
  })
  const data = await safeParse<{ ok?: boolean }>(res)
  if (!res.ok || typeof data === 'string') {
    throw new Error(typeof data === 'string' ? data : `Activate run failed: ${res.status}`)
  }
  return !!data.ok
}