<!-- src/components/MatchUpload.vue -->
<template>
  <section class="section p-4 space-y-4">
    <!-- Header -->
    <header class="flex items-center justify-between">
      <h2 class="font-semibold">Upload & Match</h2>
    </header>

    <!-- Inputs -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="cardlike p-4">
        <label for="mailCsv" class="small">Mail CSV</label>
        <input id="mailCsv" ref="mailInput" type="file" accept=".csv,text/csv" class="w-full" />
      </div>
      <div class="cardlike p-4">
        <label for="crmCsv" class="small">CRM CSV</label>
        <input id="crmCsv" ref="crmInput" type="file" accept=".csv,text/csv" class="w-full" />
      </div>
    </div>

    <!-- Actions -->
     <button id="editMappingBtn" class="btn" @click="onEditMapping">Edit mapping</button>
    <footer class="flex items-center justify-end gap-3">
        
        <button id="runBtn" class="btn" :disabled="busy" @click="onRun">
            <span v-if="!busy">Run</span>
            <span v-else>Running…</span>
        </button>
    </footer>
  </section>
</template>

<script setup>
import { ref } from "vue";

const mailInput = ref(null);
const crmInput  = ref(null);
const busy = ref(false);

// ---------------- helpers ----------------
async function upload(kind, file, mapping, reuseRunId) {
  const fd = new FormData();
  fd.append("file", file);
  fd.append("kind", kind); // "mail" | "crm"
  if (mapping) fd.append("mapping", JSON.stringify(mapping));
  if (reuseRunId) fd.append("run_id", reuseRunId);

  const res = await fetch("/api/uploads", { method: "POST", body: fd });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data?.run_id) {
    throw new Error(data?.error || `Failed to upload ${kind}`);
  }
  return data.run_id;
}

async function poll(runId) {
  while (true) {
    const r = await fetch(`/api/runs/${encodeURIComponent(runId)}/status`);
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d?.error || "status failed");

    if (d.status === "done") {
      if (window.MailTraceLoader?.setProgress) window.MailTraceLoader.setProgress(100, "Finishing…");
      return;
    }
    if (d.status === "error") {
      throw new Error(d?.message || "Server error");
    }
    if (window.MailTraceLoader?.setProgress) {
      const pct = typeof d.pct === "number" ? d.pct : 0;
      const step = d.step || "processing";
      window.MailTraceLoader.setProgress(pct, step);
    }
    await new Promise((s) => setTimeout(s, 1200));
  }
}

async function fetchResult(runId) {
  const r = await fetch(`/api/runs/${encodeURIComponent(runId)}/result`);
  if (r.status === 202) throw new Error("Result not ready");
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d?.error || "result failed");
  return d;
}

// ---------------- UI actions ----------------
function onEditMapping() {
  if (window.openMapperModal) return window.openMapperModal();
}

async function onRun(ev) {
  ev?.preventDefault?.();

  const mail = mailInput.value?.files?.[0];
  const crm  = crmInput.value?.files?.[0];
  if (!mail || !crm) {
    alert("Please select both Mail and CRM CSV files.");
    return;
  }

  // Require minimal mapping presence until server-side mapping validation is fully authoritative
  const mappingState = (window.mapperStorage && window.mapperStorage.read && window.mapperStorage.read()) || {};
  const mailMap = mappingState?.mail || {};
  const crmMap  = mappingState?.crm  || {};
  const mappingLooksOk = !!(mailMap.address1 && crmMap.address1);
  if (!mappingLooksOk) {
    alert("Please configure column mapping first (click “Edit mapping”).");
    document.getElementById("editMappingBtn")?.focus();
    return;
  }

  busy.value = true;
  try {
    if (window.MailTraceLoader?.show) window.MailTraceLoader.show({ progress: 0, text: "Uploading…" });

    // 1) Upload mail → get run_id
    if (window.MailTraceLoader?.setProgress) window.MailTraceLoader.setProgress(3, "Uploading Mail…");
    const runId = await upload("mail", mail, mailMap);

    // 2) Upload CRM using same run_id
    if (window.MailTraceLoader?.setProgress) window.MailTraceLoader.setProgress(4, "Uploading CRM…");
    await upload("crm", crm, crmMap, runId);

    // Expose runId globally for any other modules
    window.MT_CONTEXT = Object.assign(window.MT_CONTEXT || {}, { run_id: runId });

    // 3) Poll for completion
    if (window.MailTraceLoader?.setProgress) window.MailTraceLoader.setProgress(6, "Processing…");
    await poll(runId);

    // 4) Fetch results and render with existing functions
    const data = await fetchResult(runId);

    // These are your preexisting render targets—unchanged.
    window.renderKpis?.(data.kpis || {});
    window.renderGraph?.(data.graph || { months: [], matches: [], prev_year: [] });
    window.renderSummary?.(data.summary || []);
    window.renderTop?.("cities", data.top_cities || []);
    window.renderTop?.("zips",   data.top_zips   || []);

    // Optional: if some downstream consumers listen for completion:
    if (typeof window.onMatchCompleted === "function") window.onMatchCompleted(runId, data);
  } catch (e) {
    console.error(e);
    alert("Run failed: " + (e.message || "unknown error"));
  } finally {
    if (window.MailTraceLoader?.hide) window.MailTraceLoader.hide();
    busy.value = false;
  }
}
</script>