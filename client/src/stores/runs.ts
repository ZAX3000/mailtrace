// src/stores/runs.ts
import { defineStore } from 'pinia'
import { fetchRuns, fetchLatestRun, activateRun, type RunItem } from '@/api/runs'
import { useKpiStore } from '@/stores/useKpiStore'

export const useRuns = defineStore('runs', {
  state: () => ({
    items: [] as RunItem[],
    loading: false as boolean,
    error: null as string | null,
    activeRunId: null as string | null,
  }),
  getters: {
    activeRun(state) {
      return state.items.find(r => r.id === state.activeRunId) || null
    },
  },
  actions: {
    async load(limit = 25) {
      this.loading = true
      this.error = null
      try {
        this.items = await fetchRuns(limit)
        // default to the newest run if nothing is selected yet
        if (!this.activeRunId && this.items[0]) {
          this.activeRunId = this.items[0].id
        }
      } catch (e: any) {
        this.error = e?.message || 'Failed to load runs'
      } finally {
        this.loading = false
      }
    },

    async pickLatest(onlyDone = true) {
      const r = await fetchLatestRun(onlyDone)
      if (r?.id) {
        await this.setActive(r.id)
      }
    },

    async setActive(runId: string) {
      // optional: persist “active run” on the server
      try { await activateRun(runId) } catch { /* ignore if not implemented */ }

      this.activeRunId = runId

      // Tell KPI store to fetch for this run
      const kpi = useKpiStore()
      await kpi.fetch(runId)
    },
  },
})