// src/composables/useRun.ts
import { nextTick, ref } from 'vue'
import { useLoader } from '@/stores/loader'
import { getRunStatus, startRun, type RunStatus, type StartRunResponse } from '@/api/runs'

type WireStatus = NonNullable<RunStatus['status']>

export function useRun() {
  const loader = useLoader()
  const running = ref(false)

  async function kickOffAndPoll(
    runId: string,
    onNeedsMapping: (missing: Record<string, string[]>) => void
  ) {
    running.value = true

    loader.show({ progress: 3, message: 'Starting…' })
    await nextTick()

    let started = false
    try {
      console.debug('[run] ▶ START', { runId })
      const res: StartRunResponse = await startRun(runId)
      console.debug('[run] startRun ⇦', res)

      if (res.kind === 'needs-mapping') {
        console.debug('[run] needs-mapping → handoff to mapper (hide loader)')
        loader.hide(true)
        running.value = false
        onNeedsMapping(res.missing)
        return
      }

      if (res.kind === 'error') {
        console.error('[run] startRun error', res)
        throw new Error(res.message || `Start failed (${res.status})`)
      }

      started = true
      loader.setMessage('Analyzing…')

      console.debug('[run] polling status…')
      for (let i = 0; i < 240; i++) {
        const s: RunStatus = await getRunStatus(runId)
        console.debug('[run] status tick', s)

        if (typeof s.pct === 'number') loader.setProgress(Math.max(0, Math.min(100, s.pct)))

        if (s.step || s.message) loader.setMessage(String(s.step || s.message))

        const status = (s.status || '').toLowerCase() as WireStatus
        const step   = (s.step   || '').toLowerCase()

        if (status === 'failed' || step === 'failed') {
          throw new Error(s.message || 'Matching failed')
        }

        if (status === 'done' || step === 'done' || s.pct === 100) {
          break
        }

        await new Promise(r => setTimeout(r, 1000))
      }

      loader.setProgress(100)
      loader.setMessage('Run complete')
      window.dispatchEvent(new CustomEvent('mt:run-completed', { detail: { run_id: runId } }))
      console.debug('[run] ✓ COMPLETED')

    } catch (e: any) {
      console.error('[run] ✗ FAILED', e)
      loader.setProgress(100)
      loader.setMessage(e?.message || 'Run failed')

    } finally {
      running.value = false
      console.debug('[run] ⎋ cleanup: running=false; started=%s (loader left open)', started)
    }
  }

  return { running, kickOffAndPoll }
}