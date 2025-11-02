// client/src/api/runStart.ts
import { http } from './http'

export type RunStartOK = { ok: true; status: number }
export type RunStartNeedsMapping = {
  ok: false
  status: 409
  missing: { mail?: string[]; crm?: string[] }
  message?: string
}

export async function startRun(runId: string): Promise<RunStartOK | RunStartNeedsMapping> {
  try {
    const res = await http.post(`/runs/${runId}/run`)
    return { ok: true, status: res.status }
  } catch (err: any) {
    const status = err?.response?.status
    const data   = err?.response?.data || {}
    if (status === 409) {
      return { ok: false, status, missing: data.missing || {}, message: data.message }
    }
    throw err
  }
}