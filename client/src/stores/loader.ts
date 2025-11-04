// src/stores/loader.ts
import { defineStore } from 'pinia'

type ShowOpts = {
  progress?: number
  etaSeconds?: number | null
  message?: string
}

export const useLoader = defineStore('loader', {
  state: () => ({
    open: false as boolean,
    locked: false as boolean,
    progress: 0 as number,
    etaSeconds: null as number | null,
    message: '' as string,
  }),
  actions: {
    lock() { this.locked = true },
    unlock() { this.locked = false },

    show(opts: ShowOpts = {}) {
      if (typeof opts.progress === 'number') this.progress = clamp(opts.progress, 0, 100)
      if (typeof opts.etaSeconds === 'number' || opts.etaSeconds === null) this.etaSeconds = opts.etaSeconds ?? null
      if (typeof opts.message === 'string') this.message = opts.message
      this.open = true
    },

    hide(force = false) {
      if (this.locked && !force) return
      this.open = false
    },

    setMessage(msg: string) { this.message = msg },
    setProgress(p: number) { this.progress = clamp(p, 0, 100) },
    setEtaSeconds(s: number | null) { this.etaSeconds = s },
    reset() {
      this.open = false
      this.locked = false
      this.progress = 0
      this.etaSeconds = null
      this.message = ''
    },
  },
})

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n))
}