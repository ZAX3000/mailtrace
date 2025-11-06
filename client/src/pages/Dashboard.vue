<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ModalUploadGuard from '@/components/ModalUploadGuard.vue'
import ModalMappingRequired from '@/components/ModalMappingRequired.vue'
import UploadCard from '@/components/UploadCard.vue'
import KpiSummaryCard from '@/components/KpiSummaryCard.vue'
import { useUploadGuard } from '@/composables/useUploadGuard'
import { useLoader } from '@/stores/loader'

declare global {
  interface Window {
    MT_CONTEXT?: any
    initDashboard?: (runId?: string) => void
  }
}

const route = useRoute()
const router = useRouter()
const loader = useLoader()

// Upload guard modal
const showUploadGuard = ref(false)
useUploadGuard(() => (showUploadGuard.value = true))

// Mapping modal state
const showMapping = ref(false)
const missing = ref<Record<string, string[]>>({})

// expose run id for KPI component
const runId = ref<string>('')

// ---- UI handlers ------------------------------------------------------------

function onMappingRequired(payload: { mail?: string[]; crm?: string[] }) {
  // Hand off UI to mapper: unlock + hide loader, then open modal
  loader.unlock()
  loader.hide(true)
  missing.value = payload || {}
  showMapping.value = true
}

function openMapper() {
  // If someone clicks "Edit mapping" from the modal’s button:
  loader.unlock()
  loader.hide(true)
  const run_id = window.MT_CONTEXT?.run_id || ''
  window.dispatchEvent(new CustomEvent('mt:open-mapper', { detail: { run_id } }))
}

// ---- Lifecycle / events -----------------------------------------------------

onMounted(async () => {
  // Optional: react to app-wide events (useRun already shows/updates loader;
  // these are helpful for sanity/diagnostics and manual testing)
  const onRunStarted = (e: any) => {
    console.info('EVT mt:run-started', e?.detail)
    loader.lock()
    loader.show({ progress: 3, message: 'Starting…' })
  }

  const onRunCompleted = (e: any) => {
    console.info('EVT mt:run-completed', e?.detail)
    // Update loader
    loader.setProgress(100)
    loader.setMessage('Run complete')
    // Push the finished run id so KPI card can fetch /result
    const id = e?.detail?.run_id as string | undefined
    if (id) {
      runId.value = id
      // reflect in global ctx for legacy consumers
      window.MT_CONTEXT = { ...(window.MT_CONTEXT || {}), run_id: id }
      // make the URL shareable/bookmarkable
      const q = { ...route.query, run_id: id }
      router.replace({ path: route.path, query: q })
    }
  }

  const onOpenMapper = (e: any) => {
    console.warn('EVT mt:open-mapper', e?.detail)
    const miss = (e?.detail?.missing ?? {}) as { mail?: string[]; crm?: string[] }
    if (Object.keys(miss).length) missing.value = miss
    // Hand off to mapper overlay: unlock + hide loader, then show modal
    loader.unlock()
    loader.hide(true)
    showMapping.value = true
  }

  window.addEventListener('mt:run-started', onRunStarted)
  window.addEventListener('mt:run-completed', onRunCompleted)
  window.addEventListener('mt:open-mapper', onOpenMapper)
  ;(window as any).__onRunStarted = onRunStarted
  ;(window as any).__onRunCompleted = onRunCompleted
  ;(window as any).__onOpenMapper = onOpenMapper

  const qRunId = (route.query.run_id as string) || ''
  if (qRunId) {
    window.MT_CONTEXT = { ...(window.MT_CONTEXT || {}), run_id: qRunId }
    runId.value = qRunId
  }

  // DEBUG console hooks
  ;(window as any).MT_DEBUG = {
    showLoader: (p = 15) => loader.show({ progress: p }),
    hideLoader: () => loader.hide(),
    lock: () => loader.lock(),
    unlock: () => loader.unlock(),
    openMapper: (miss = {}) =>
      window.dispatchEvent(new CustomEvent('mt:open-mapper', { detail: { run_id: qRunId, missing: miss } })),
  }
})

onBeforeUnmount(() => {
  const rs = (window as any).__onRunStarted
  const rc = (window as any).__onRunCompleted
  const om = (window as any).__onOpenMapper
  if (rs) window.removeEventListener('mt:run-started', rs)
  if (rc) window.removeEventListener('mt:run-completed', rc)
  if (om) window.removeEventListener('mt:open-mapper', om)
})
</script>

<template>
  <div class="wrap">
    <div class="err" id="err"><b>JavaScript error:</b> <span id="errmsg"></span></div>

    <div class="row">
      <!-- Upload panel -->
      <UploadCard
        class="card"
        style="flex:1 1 380px; min-width:340px"
        @mapping-required="onMappingRequired"
      />

      <!-- KPI bar (Vue component, preserves legacy look/spacing via id/class/style) -->
      <KpiSummaryCard
        id="cmp-kpis"
        class="card row"
        style="flex:3 1 520px; min-width:320px; align-items:center; justify-content:space-around"
        :run-id="runId"
      />
    </div>

    <!-- Chart (legacy for now) -->
    <div class="section card" id="cmp-graph">
      <div class="yoy yoy-btn">
        <label><input id="yoyToggle" type="checkbox" checked> Show YoY overlay</label>
      </div>
      <canvas id="chart" width="1000" height="260"></canvas>
    </div>

    <!-- Top lists (legacy for now) -->
    <div class="row section" id="cmp-top">
      <div class="card" style="flex:1 1 360px">
        <div style="font-weight:600;margin-bottom:8px">Top Cities</div>
        <div class="table-wrap topwrap">
          <table data-kind="cities">
            <thead>
              <tr><th>City</th><th style="text-align:center">Total Matches</th><th style="text-align:center">Match Rate</th></tr>
            </thead>
            <tbody data-top="cities"></tbody>
          </table>
        </div>
      </div>

      <div class="card" style="flex:1 1 360px">
        <div style="font-weight:600;margin-bottom:8px">Top ZIPs</div>
        <div class="table-wrap topwrap">
          <table data-kind="zips">
            <thead>
              <tr><th>ZIP</th><th style="text-align:center">Total Matches</th><th style="text-align:center">Match Rate</th></tr>
            </thead>
            <tbody data-top="zips"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Summary table (legacy for now) -->
    <div class="section card" id="cmp-summary">
      <div style="font-weight:600;margin-bottom:8px">Summary</div>
      <div class="table-wrap">
        <table>
          <colgroup id="sum-cols"></colgroup>
          <thead>
            <tr>
              <th>Mail Address 1</th><th>Mail Unit</th><th>CRM Address 1</th><th>CRM Unit</th>
              <th>City</th><th>State</th><th>ZIP</th><th>Mail Dates</th>
              <th>CRM Date</th><th>Amount</th><th>Confidence</th><th>Notes</th>
            </tr>
          </thead>
          <tbody data-summary="rows"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Mapper modal (legacy shell) -->
  <div aria-hidden="true" id="mtMapperOverlay">
    <div aria-labelledby="mtMapperTitle" aria-modal="true" id="mtMapperModal" role="dialog">
      <header>
        <h3 id="mtMapperTitle">Map your columns</h3>
        <button class="btn ghost" id="mtMapperClose">Close</button>
      </header>
      <div class="body">
        <p id="mtMapperHelp">
          We couldn't automatically detect some columns. Map the fields below and we'll remember them for next time.
        </p>
        <div class="grid">
          <div class="col" id="mtMapMail"><h4>Mail CSV</h4><div class="rows"></div></div>
          <div class="col" id="mtMapCRM"><h4>CRM CSV</h4><div class="rows"></div></div>
        </div>
      </div>
      <footer>
        <button class="btn ghost" id="mtMapperAuto">Auto-map</button>
        <div style="flex:1"></div>
        <button class="btn" id="mtMapperCancel">Cancel</button>
        <button class="btn primary" id="mtMapperApply">Use mapping</button>
      </footer>
    </div>
  </div>

  <!-- Missing-field popup (Vue modal) -->
  <ModalMappingRequired
    v-model="showMapping"
    v-model:missing="missing"
    @edit-mapping="openMapper"
  />

  <!-- Simple alert modal (IDs kept for legacy) -->
  <ModalUploadGuard
    v-model="showUploadGuard"
    title="Action needed"
    message="Please pick <strong>both CSV files</strong> first."
    focusSelector="#mailCsv"
    :triggerFileDialog="true"
  />
</template>