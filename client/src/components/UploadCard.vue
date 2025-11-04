<!-- src/components/UploadCard.vue -->
<template>
  <section class="section p-4 space-y-4" v-bind="$attrs">
    <header class="flex items-center justify-between">
      <h2 class="font-semibold text-sm">Upload &amp; Match</h2>
    </header>

    <!-- Inputs -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
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
      <button class="btn" type="button" @click="openMapper('mail')">
        Edit mapping
      </button>

      <button class="btn" :disabled="running" type="button" @click="onRun">
        <span v-if="!running">Run</span>
        <span v-else>Running…</span>
      </button>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRun } from '@/composables/useRun'
import { uploadSource, type Source } from '@/api/uploads'
import { createRun } from '@/api/runs'
import { log } from '@/utils/logger'

const emit = defineEmits<{
  (e: 'mapping-required', missing: Record<string, string[]>): void
  (e: 'need-both-files'): void
}>()

const runId = ref<string>('')
const mailInput = ref<HTMLInputElement | null>(null)
const crmInput  = ref<HTMLInputElement | null>(null)

async function ensureRun() {
  if (!runId.value) {
    log.debug('UI ▶ ensureRun: creating run…')
    const { run_id } = await createRun()
    runId.value = run_id
    ;(window as any).MT_CONTEXT = { ...(window as any).MT_CONTEXT, run_id }
    log.info('UI ▶ ensureRun: run created', { runId: run_id })
  }
}

function csvGuard(file: File): boolean {
  const ok = file.type?.includes('csv') || file.name.toLowerCase().endsWith('.csv')
  if (!ok) alert('Please select a CSV file.')
  return ok
}

async function onPick(source: Source) {
  const el = source === 'mail' ? mailInput.value : crmInput.value
  const file = el?.files?.[0]
  if (!file) return
  if (!csvGuard(file)) { if (el) el.value = ''; return }

  await ensureRun()
  try {
    log.info('UI ▶ upload start', { runId: runId.value, source, name: file.name, size: file.size })
    const res = await uploadSource(runId.value, source, file)
    log.info('UI ▶ upload done', { source, res })

    if (res.state === 'raw_only') {
      window.dispatchEvent(
        new CustomEvent('mt:open-mapper', { detail: { run_id: runId.value, source } })
      )
    }
  } catch (e: any) {
    log.error('UI ▶ upload failed', e)
    alert(`Upload failed: ${e?.message || 'unknown error'}`)
    if (el) el.value = ''
  }
}

function openMapper(source: Source) {
  if (!runId.value) {
    alert('Create a run by uploading at least one CSV first.')
    return
  }
  log.info('UI ▶ open mapper clicked', { runId: runId.value, source })
  window.dispatchEvent(new CustomEvent('mt:open-mapper', { detail: { run_id: runId.value, source } }))
}

const { running, kickOffAndPoll } = useRun()

async function onRun(ev?: Event) {
  ev?.preventDefault?.()

  const hasMail = !!mailInput.value?.files?.[0]
  const hasCrm  = !!crmInput.value?.files?.[0]
  if (!hasMail || !hasCrm) {
    emit('need-both-files')
    return
  }

  await ensureRun()
  log.info('UI ▶ Run clicked', { runId: runId.value })

  await kickOffAndPoll(runId.value, (missing) => {
    log.warn('UI ▶ needs mapping (409)', { runId: runId.value, missing })
    emit('mapping-required', missing)
    window.dispatchEvent(
      new CustomEvent('mt:open-mapper', { detail: { run_id: runId.value, missing } })
    )
  })
}
</script>