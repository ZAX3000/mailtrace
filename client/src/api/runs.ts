// src/api/runs.ts
export type StartRunResponse =
  | { kind: 'ok' }
  | { kind: 'needs-mapping'; missing: Record<string, string[]> }
  | { kind: 'error'; status?: number; message?: string }

export type RunStatus = {
  status?: 'queued' | 'running' | 'done' | 'failed'
  step?: string
  message?: string
  pct?: number
}

const API = (import.meta as any)?.env?.VITE_API_BASE?.toString?.() || '' // e.g. http://localhost:5000

async function safeParse<T>(res: Response): Promise<T | string> {
  const text = await res.text()
  try { return JSON.parse(text) as T } catch { return text } // don’t throw on HTML/error pages
}

function url(path: string) {
  // If API is set, prefix it; otherwise assume Vite proxy handles /api
  return API ? `${API}${path}` : path
}

export async function startRun(runId: string): Promise<StartRunResponse> {
  const res = await fetch(url(`/api/runs/${encodeURIComponent(runId)}/start`), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  })
  const data = await safeParse<StartRunResponse>(res)

  if (!res.ok) {
    return {
      kind: 'error',
      status: res.status,
      message:
        typeof data === 'string'
          ? data || 'Start failed'
          : (data as any)?.message || 'Start failed',
    }
  }
  return (typeof data === 'string' ? { kind: 'error', status: res.status, message: data } : data) as StartRunResponse
}

export async function getRunStatus(runId: string): Promise<RunStatus> {
  const res = await fetch(url(`/api/runs/${encodeURIComponent(runId)}/status`), {
    method: 'GET',
    headers: { Accept: 'application/json' },
    cache: 'no-store',
  })

  if (!res.ok) {
    // Surface a readable message but don’t crash the poller
    return {
      status: 'failed',
      message: `Status ${res.status} while polling`,
      pct: 100,
    }
  }

  const data = await safeParse<RunStatus>(res)
  return (typeof data === 'string' ? { status: 'failed', message: data, pct: 100 } : data) as RunStatus
}