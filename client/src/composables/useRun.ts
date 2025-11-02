import { ref } from 'vue'
import { useLoader } from '@/stores/loader'
import { getRunStatus, startRun, type RunStatus, type StartRunResponse } from '@/api/runs'

type RunOutcome =
  | { kind: 'needs-mapping'; missing: Record<string, string[]> }
  | { kind: 'done' }
  | { kind: 'error'; message: string }

export function useRun() {
  const loader = useLoader()
  const running = ref(false)

  async function kickOffAndPoll(
    runId: string,
    onNeedsMapping?: (missing: Record<string, string[]>) => void
  ): Promise<RunOutcome> {
    if (!runId) return { kind: 'error', message: 'No run_id' }

    // Turn on UI state immediately so double-clicks don’t queue runs
    running.value = true
    // Show the overlay BEFORE the first network await so failures still show UI
    loader.show({ progress: 5 })

    try {
      // 1) Start the run
      const res: StartRunResponse = await startRun(runId)

      if (res.kind === 'needs-mapping') {
        // Close the loader, notify caller, stop here
        loader.hide()
        running.value = false
        onNeedsMapping?.(res.missing)
        return { kind: 'needs-mapping', missing: res.missing }
      }

      if (res.kind === 'error') {
        throw new Error(res.message || `Start failed (${res.status})`)
      }

      // Optional signal that polling begins
      window.dispatchEvent(new CustomEvent('mt:run-started', { detail: { run_id: runId } }))

      // 2) Poll status
      for (let i = 0; i < 240; i++) { // ~4 min @ 1s
        const s: RunStatus = await getRunStatus(runId)
        if (typeof s.pct === 'number') loader.setProgress(s.pct)

        const status = String(s.status || '').toLowerCase()
        if (status === 'done' || status === 'completed') {
          window.dispatchEvent(new CustomEvent('mt:run-completed', { detail: { run_id: runId } }))
          return { kind: 'done' }
        }
        if (status === 'failed') {
          throw new Error(s.message || 'Matching failed')
        }
        await new Promise(r => setTimeout(r, 1000))
      }

      // Timed out polling
      throw new Error('Run timed out while polling status')

    } catch (e: any) {
      const msg = e?.message || 'Run error'
      alert(msg) // or route through a toast
      return { kind: 'error', message: msg }
    } finally {
      // Always clean up UI
      loader.hide()
      running.value = false
    }
  }

  return { running, kickOffAndPoll }
}