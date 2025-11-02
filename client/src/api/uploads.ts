// src/api/uploads.ts
import { http } from './http'

export type Source = 'mail' | 'crm'
export type UploadState = 'ready' | 'raw_only'

export interface CreateRunRes {
  run_id: string
}

export interface UploadRes {
  run_id: string
  source: Source
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
 * Upload one source (mail|crm). Server ingests RAW and may immediately normalize.
 * - 201 → { state: 'ready' | 'raw_only' }
 * - 409 → mapping required (handled here; returns { state: 'raw_only', ...hints })
 */
export async function uploadSource(runId: string, source: Source, file: File): Promise<UploadRes> {
  const fd = new FormData()
  fd.append('file', file)

  const res = await http.post<UploadRes>(
    `/runs/${runId}/uploads/${source}`,
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
      source: source,
      state: 'raw_only',
      missing: data.missing,
      sample_headers: data.sample_headers,
      sample_rows: data.sample_rows,
      message: data.message || 'Mapping required',
    }
  }

  return res.data
}