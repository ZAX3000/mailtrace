// client/src/stores/loader.ts  (Pinia version with identical API)
import { defineStore } from 'pinia'

function clamp(n: number) {
  n = Math.round(n || 0)
  return Math.max(0, Math.min(100, n))
}

export const useLoader = defineStore('loader', {
  state: () => ({
    open: false,
    progress: 0,
    etaSeconds: null as number | null,
  }),
  actions: {
    show(opts?: { progress?: number; etaSeconds?: number | null }) {
      this.open = true
      if (opts?.progress != null) this.progress = clamp(opts.progress)
      if (opts?.etaSeconds !== undefined) this.etaSeconds = opts.etaSeconds ?? null
    },
    hide() { this.open = false },
    setProgress(n: number) { this.progress = clamp(n) },
    setETA(sec: number | null | undefined) {
      this.etaSeconds = sec == null ? null : Math.max(0, Math.round(sec))
    },
  },
})
