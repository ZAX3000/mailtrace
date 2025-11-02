// client\src\stores\loader.ts
import { defineStore } from 'pinia'

type ShowOpts = {
  progress?: number
  etaSeconds?: number | null
  message?: string          // <-- add
}

export const useLoader = defineStore('loader', {
  state: () => ({
    open: false,
    progress: 0,
    etaSeconds: null as number | null,
    message: '' as string,   // <-- add
  }),
  actions: {
    show(opts: ShowOpts = {}) {
      this.open = true
      if (typeof opts.progress === 'number') this.progress = opts.progress
      this.etaSeconds = typeof opts.etaSeconds === 'number' ? opts.etaSeconds : null
      if (typeof opts.message === 'string') this.message = opts.message // <-- add
    },
    hide() {
      this.open = false
      this.progress = 0
      this.etaSeconds = null
      this.message = '' // <-- add
    },
    setProgress(n: number) { this.progress = n },
    setETA(sec?: number | null) { this.etaSeconds = (typeof sec === 'number' ? sec : null) },
    setMessage(msg: string) { this.message = msg }, // <-- add
  },
})