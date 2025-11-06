<template>
  <section class="section p-4 space-y-4" role="region" aria-label="KPI Summary">
    <header class="flex items-center justify-between gap-3">
      <h2 class="font-semibold text-base leading-tight flex-1 text-left">Summary</h2>
      <div class="flex items-center gap-2 shrink-0">
        <button class="btn" @click="reload" :disabled="loading">Refresh</button>
      </div>
    </header>

    <div v-if="error" class="text-amber-600 text-sm">
      <p class="mb-1">
        <strong>Heads up:</strong> {{ niceError }}
      </p>
      <button class="btn" @click="reload" :disabled="loading">Try again</button>
    </div>

    <div v-else-if="loading" class="text-sm opacity-80">
      Loading KPIs…
    </div>

    <!-- 1 / 2 / 3 columns, bigger gaps -->
    <div v-else class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 md:gap-6">
      <div class="cardlike p-4 md:p-5 h-full">
        <div class="small text-muted">Total Mail</div>
        <div class="text-xl font-semibold" data-kpi="mail">{{ n(k.total_mail) }}</div>
      </div>
      <div class="cardlike p-4 md:p-5 h-full">
        <div class="small text-muted">Unique Mail Addresses</div>
        <div class="text-xl font-semibold" data-kpi="uniqmail">{{ n(k.unique_mail_addresses) }}</div>
      </div>
      <div class="cardlike p-4 md:p-5 h-full">
        <div class="small text-muted">Total Jobs</div>
        <div class="text-xl font-semibold" data-kpi="crm">{{ n(k.total_jobs) }}</div>
      </div>
      <div class="cardlike p-4 md:p-5 h-full">
        <div class="small text-muted">Matches</div>
        <div class="text-xl font-semibold" data-kpi="matches">{{ n(k.matches) }}</div>
      </div>
      <div class="cardlike p-4 md:p-5 h-full">
        <div class="small text-muted">Match Rate</div>
        <div class="text-xl font-semibold" data-kpi="match_rate">{{ pct(k.match_rate) }}</div>
      </div>
      <div class="cardlike p-4 md:p-5 h-full">
        <div class="small text-muted">Match Revenue</div>
        <div class="text-xl font-semibold" data-kpi="match_revenue">{{ money(k.match_revenue) }}</div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, onBeforeUnmount } from "vue";
import { useKpiStore } from "@/stores/useKpiStore";

const props = defineProps<{
  runId?: string;
}>();

const kpis = useKpiStore();

const k = computed(() => kpis.kpis ?? {});
const loading = computed(() => kpis.loading);
const error = computed(() => kpis.error);

const niceError = computed(() => {
  if (!error.value) return "";
  if (error.value === "not_ready") return "This run isn’t finished yet — kick off matching or refresh after it completes.";
  return error.value;
});

function n(v?: number) { return (Number(v || 0)).toLocaleString(); }
function pct(v?: number) {
  if (v == null) return "0.00%";
  // DAO returns float; if backend is already percentage (e.g., 42.1) keep it;
  // if it’s a 0..1 ratio, multiply. We auto-detect.
  const raw = Number(v);
  const asPct = raw <= 1 ? raw * 100 : raw;
  return `${asPct.toFixed(2)}%`;
}
function money(v?: number) {
  const num = Number(v || 0);
  try { return num.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 }); }
  catch { return `$${num.toFixed(2)}`; }
}

async function reload() {
  // Prefer the prop if present; otherwise use the last run id the store fetched.
  const id = props.runId || kpis.runId;
  if (id) await kpis.fetch(id);
}

// 1) Autofetch once on mount if a runId was provided.
// 2) Then rely on the global 'mt:run-completed' event to fetch after future runs finish.
function onRunCompleted(e: any) {
  const id = e?.detail?.run_id as string | undefined;
  if (id) kpis.fetch(id);
}

onMounted(() => {
  if (props.runId) kpis.fetch(props.runId);
  window.addEventListener('mt:run-completed', onRunCompleted as EventListener);
});

onBeforeUnmount(() => {
  window.removeEventListener('mt:run-completed', onRunCompleted as EventListener);
});
</script>

<style scoped>

.small { font-size: .8rem; }
.text-muted { opacity: .7; }
.cardlike { background: var(--surface-color); border: 1px solid var(--line-color); border-radius: 12px; box-shadow: var(--elev-1); }
.section { background: var(--surface-color); border: 1px solid var(--line-color); border-radius: 12px; box-shadow: var(--elev-1); }
</style>
