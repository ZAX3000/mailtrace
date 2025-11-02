import { get, post } from './http'

// ----- Status -----
export type RunStatus = {
  run_id: string
  status?: string
  pct?: number
  step?: string
  message?: string
}
export const getRunStatus = (runId: string) => get<RunStatus>(`/runs/${runId}/status`)
export const getRunResult = (runId: string) => get(`/runs/${runId}/result`)

// ----- Start run (discriminated union) -----
export type StartRunOk = { kind: 'ok'; status: 200 | 202 }
export type StartRunNeedsMapping = {
  kind: 'needs-mapping'
  status: 409
  missing: Record<string, string[]> // e.g. { mail:['zip'], crm:['job_date'] }
}
export type StartRunError = { kind: 'error'; status: number; message: string }

export type StartRunResponse = StartRunOk | StartRunNeedsMapping | StartRunError

export async function startRun(runId: string): Promise<StartRunResponse> {
  try {
    const res = await post(`/runs/${runId}/run`, {})
    // Successful kick-off should be 202
    if (res.status === 202) return { kind: 'ok', status: 202 }
    // Fallback: some backends 200 on accept; treat as ok
    if (res.status === 200) return { kind: 'ok', status: 200 }
    // Unexpected success code
    return { kind: 'error', status: res.status ?? 0, message: 'Unexpected status' }
  } catch (e: any) {
    const status = e?.status ?? 0
    const data = e?.data
    if (status === 409 && data && data.missing) {
      return { kind: 'needs-mapping', status: 409, missing: data.missing as Record<string, string[]> }
    }
    return { kind: 'error', status, message: e?.message || 'Failed to start run' }
  }
}
