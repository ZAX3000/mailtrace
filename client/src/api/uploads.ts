// src/api/uploads.ts
import { http } from './http'

export type Side = 'mail' | 'crm'
export type UploadState = 'ready' | 'raw_only'

export interface CreateRunRes {
  run_id: string
}

export interface UploadRes {
  run_id: string
  side: Side
  state: UploadState                 // 'ready' => normalized; 'raw_only' => needs mapper
  // Optional hints the server may return (esp. when state === 'raw_only')
  missing?: string[]
  sample_headers?: string[]
  sample_rows?: any[]
  message?: string
}

/** Create (or get) an active run for the current user. */
export async function createRun(): Promise<CreateRunRes> {
  const res = await http.post<CreateRunRes>('/runs')
  return res.data
}

/**
 * Upload one side (mail|crm). Server ingests RAW and may immediately normalize.
 * - 201 → { state: 'ready' | 'raw_only' }
 * - 409 → mapping required (handled here; returns { state: 'raw_only', ...hints })
 */
export async function uploadSide(runId: string, kind: Side, file: File): Promise<UploadRes> {
  const fd = new FormData()
  fd.append('file', file)

  const res = await http.post<UploadRes>(
    `/runs/${runId}/uploads/${kind}`,
    fd,
    {
      headers: { 'Content-Type': 'multipart/form-data' },
      // Treat 409 as a controlled outcome instead of throwing
      validateStatus: (s) => (s >= 200 && s < 300) || s === 409,
      withCredentials: true,
    }
  )

  if (res.status === 409) {
    const data: any = res.data || {}
    return {
      run_id: runId,
      side: kind,
      state: 'raw_only',
      missing: data.missing,
      sample_headers: data.sample_headers,
      sample_rows: data.sample_rows,
      message: data.message || 'Mapping required',
    }
  }

  return res.data
}