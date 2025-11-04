// client/src/stores/mapper.ts
import { defineStore } from 'pinia'

export const useMapper = defineStore('mapper', {
  state: () => ({
    open: false,
    runId: '' as string,
    missing: {} as Record<string, string[]>,
  }),
  actions: {
    show(runId: string, missing: Record<string, string[]>) {
      this.runId = runId
      this.missing = missing
      this.open = true
    },
    hide() {
      this.open = false
      this.runId = ''
      this.missing = {}
    },
  },
})