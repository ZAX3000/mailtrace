<template>
  <section class="section p-4">
    <div class="card p-4">
      <div class="kpi-grid">
        <div class="kpi"><div class="l">Total Mail</div>            <div class="v">{{ fmt(kpis.total_mail) }}</div></div>
        <div class="kpi"><div class="l">Unique Mail Addresses</div> <div class="v">{{ fmt(kpis.unique_addresses) }}</div></div>
        <div class="kpi"><div class="l">Total Jobs</div>            <div class="v">{{ fmt(kpis.total_jobs) }}</div></div>
        <div class="kpi"><div class="l">Matches</div>               <div class="v">{{ fmt(kpis.matches) }}</div></div>
        <div class="kpi"><div class="l">Match Rate</div>            <div class="v">{{ fmt(kpis.match_rate) }}</div></div>
        <div class="kpi"><div class="l">Match Revenue</div>         <div class="v">{{ fmt(kpis.match_revenue) }}</div></div>
      </div>
    </div>
  </section>
</template>

<script setup>
import { storeToRefs } from "pinia";
import { useMatchRun } from "@/stores/matchRun";
const { kpis } = storeToRefs(useMatchRun());

function fmt(v) {
  if (v == null || v === "") return "—";
  if (typeof v === "number" && v >= 1_000_000) return (v/1_000_000).toFixed(1) + "M";
  if (typeof v === "number" && v >= 1_000)     return (v/1_000).toFixed(1) + "k";
  if (typeof v === "number") return v.toLocaleString();
  return String(v);
}
</script>