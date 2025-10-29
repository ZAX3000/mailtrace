import { reactive, readonly } from 'vue'

type State = {
  open: boolean
  progress: number // 0..100
  etaSeconds: number | null
}

const state = reactive<State>({
  open: false,
  progress: 0,
  etaSeconds: null,
})

function clamp(n: number) {
  n = Math.round(n || 0)
  return Math.max(0, Math.min(100, n))
}

export function useLoader() {
  function show(opts?: { progress?: number; etaSeconds?: number | null }) {
    state.open = true
    if (opts?.progress != null) state.progress = clamp(opts.progress)
    if (opts?.etaSeconds !== undefined) state.etaSeconds = opts.etaSeconds
  }
  function hide() { state.open = false }
  function setProgress(n: number) { state.progress = clamp(n) }
  function setETA(sec: number | null | undefined) {
    state.etaSeconds = sec == null ? null : Math.max(0, Math.round(sec))
  }

  return { state: readonly(state), show, hide, setProgress, setETA }
}