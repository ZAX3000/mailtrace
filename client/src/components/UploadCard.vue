<!-- src/components/UploadCard.vue -->
<template>
  <section class="section p-4 space-y-4">
    <header class="flex items-center justify-between">
      <h2 class="font-semibold text-sm">Upload &amp; Match</h2>
    </header>

    <!-- Inputs -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <!-- Mail -->
      <div class="cardlike p-4">
        <label for="mailCsv" class="small">Mail CSV</label>
        <input
          id="mailCsv"
          ref="mailInput"
          type="file"
          accept=".csv,text/csv"
          class="w-full"
          @change="onPick('mail')"
        />
      </div>

      <!-- CRM -->
      <div class="cardlike p-4">
        <label for="crmCsv" class="small">CRM CSV</label>
        <input
          id="crmCsv"
          ref="crmInput"
          type="file"
          accept=".csv,text/csv"
          class="w-full"
          @change="onPick('crm')"
        />
      </div>
    </div>

    <!-- Actions -->
    <footer class="actions actions--stack">
      <button class="btn" @click="onEditMapping">
        Edit mapping
      </button>
      <button class="btn" @click="onRun">
        <span v-if="!running">Run</span><span v-else>Running…</span>
      </button>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useLoader } from '@/stores/loader'
import { createRun, uploadSide, type Side } from '@/api/uploads'
import { getRunStatus } from '@/api/runs'

const loader = useLoader()
const runId = ref<string>('')

const mailInput = ref<HTMLInputElement | null>(null)
const crmInput  = ref<HTMLInputElement | null>(null)

const running = ref(false)

async function ensureRun() {
  if (!runId.value) {
    const { run_id } = await createRun()
    runId.value = run_id
    ;(window as any).MT_CONTEXT = { ...(window as any).MT_CONTEXT, run_id }
  }
}

function csvGuard(file: File): boolean {
  const ok = file.type?.includes('csv') || file.name.toLowerCase().endsWith('.csv')
  if (!ok) alert('Please select a CSV file.')
  return ok
}

async function onPick(kind: Side) {
  const el = kind === 'mail' ? mailInput.value : crmInput.value
  const file = el?.files?.[0]
  if (!file) return
  if (!csvGuard(file)) { if (el) el.value = ''; return }

  await ensureRun()

  try {
    // NOTE: we DO NOT show the full-screen loader on file pick.
    const res = await uploadSide(runId.value, kind, file)

    // If mapping is needed, prompt the modal consumer
    if (res.state === 'raw_only') {
      window.dispatchEvent(new CustomEvent('mt:open-mapper', {
        detail: { run_id: runId.value, kind }
      }))
    }
  } catch (e: any) {
    alert(`Upload failed: ${e?.message || 'unknown error'}`)
    if (el) el.value = ''
  }
}

function onEditMapping() {
  if (!runId.value) {
    alert('Please upload both CSV files first.')
    return
  }
  window.dispatchEvent(new CustomEvent('mt:open-mapper', {
    detail: { run_id: runId.value }
  }))
}

async function onRun(ev?: Event) {
  if (!runId.value) {
    alert('Please upload both CSV files first.')
    return
  }
  ev?.preventDefault?.()

  running.value = true
  loader.show({ progress: 10 })

  try {
    for (let i = 0; i < 240; i++) {   // ~4 min if 1s interval
      const s = await getRunStatus(runId.value)
      if (typeof s.pct === 'number') loader.setProgress(s.pct)
      const status = String(s.status || '').toLowerCase()
      if (status === 'done' || status === 'completed') break
      if (status === 'failed') throw new Error(s.message || 'Matching failed')
      await new Promise(r => setTimeout(r, 1000))
    }

    window.dispatchEvent(new CustomEvent('mt:run-completed', {
      detail: { run_id: runId.value }
    }))
  } catch (e: any) {
    alert(`Run failed: ${e?.message || 'unknown error'}`)
  } finally {
    loader.hide()
    running.value = false
  }
}
</script>