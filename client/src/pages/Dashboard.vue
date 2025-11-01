<!-- client/src/pages/Dashboard.vue -->
<script setup lang="ts">
import { ref, onMounted, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ModalUploadGuard from '@/components/ModalUploadGuard.vue'
import { useUploadGuard } from '@/composables/useUploadGuard'
import UploadCard from '@/components/UploadCard.vue'

declare global {
  interface Window {
    MT_CONTEXT?: any
    initDashboard?: (runId?: string) => void
  }
}

const route = useRoute()
const router = useRouter()
const showUploadGuard = ref(false)

// create the guard; pass a function that opens our modal
useUploadGuard(() => (showUploadGuard.value = true))

onMounted(async () => {
  const runId = (route.query.run_id as string) || ''
  if (runId && route.query.run_id !== runId) {
    router.replace({ path: route.path, query: { ...route.query, run_id: runId } })
  }
  window.MT_CONTEXT = { ...(window.MT_CONTEXT || {}), run_id: runId }
  await nextTick()
  window.initDashboard?.(runId)
})

</script>

<template>
  <!-- Paste/keep your dashboard BODY content here so legacy JS finds the same selectors. -->
  <div class="wrap">
    <div class="err" id="err"><b>JavaScript error:</b> <span id="errmsg"></span></div>

    <div class="row">
      <!-- Upload panel -->
      <UploadCard class="card" style="flex:1 1 380px; min-width:340px" />

      <!-- KPI bar -->
      <div
        class="card row"
        id="cmp-kpis"
        style="flex:3 1 520px; min-width:320px; align-items:center; justify-content:space-around"
      >
        <div class="kpi"><div class="v" data-kpi="mail"></div><div class="l">Total Mail</div></div>
        <div class="kpi"><div class="v" data-kpi="uniqmail"></div><div class="l">Unique Mail Addresses</div></div>
        <div class="kpi"><div class="v" data-kpi="crm"></div><div class="l">Total Jobs</div></div>
        <div class="kpi"><div class="v" data-kpi="matches"></div><div class="l">Matches</div></div>
        <div class="kpi"><div class="v" data-kpi="rate"></div><div class="l">Match Rate</div></div>
        <div class="kpi"><div class="v" data-kpi="revenue"></div><div class="l">Match Revenue</div></div>
      </div>
    </div>

    <!-- Chart -->
    <div class="section card" id="cmp-graph">
      <div class="yoy yoy-btn">
        <label><input id="yoyToggle" type="checkbox" checked> Show YoY overlay</label>
      </div>
      <canvas id="chart" width="1000" height="260"></canvas>
    </div>

    <!-- Top lists -->
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

    <!-- Summary table -->
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

  <!-- Mapper modal (IDs kept for legacy) -->
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

  <!-- Missing-field popup (IDs kept for legacy) -->
  <div aria-hidden="true" id="mtPopupOverlay">
    <div aria-labelledby="mtPopupTitle" aria-modal="true" id="mtPopup" role="alertdialog">
      <header>
        <h3 id="mtPopupTitle">Missing required mappings</h3>
        <button class="btn" id="mtPopupClose" title="Close">Close</button>
      </header>
      <div class="body">
        <p>We couldn’t proceed because the following required fields aren’t mapped yet:</p>
        <ul id="mtPopupList"></ul>
      </div>
      <footer>
        <button class="btn primary" id="mtPopupOk">OK—take me back</button>
      </footer>
    </div>
  </div>

  <!-- Simple alert modal (IDs kept for legacy) -->
  <ModalUploadGuard
    v-model="showUploadGuard"
    title="Action needed"
    message="Please pick <strong>both CSV files</strong> first."
    focusSelector="#mailCsv"
    :triggerFileDialog="true"
  />
</template>