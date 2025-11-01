// src/api/runs.ts
import { get } from './http'

export type RunStatus = { status: string; pct?: number; message?: string }
export const getRunStatus = (runId: string) => get<RunStatus>(`/runs/${runId}/status`)
export const getRunResult = (runId: string) => get(`/runs/${runId}/result`)