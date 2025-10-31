<template>
  <section class="section p-4">
    <div id="cmp-kpis" class="card p-4">
      <!-- Empty shell: legacy JS populates this at runtime -->
      <!-- Optional placeholders for initial layout -->
      <div class="kpi-grid">
        <div class="kpi"><div class="l">Total Mail</div><div class="v">&nbsp;</div></div>
        <div class="kpi"><div class="l">Unique Mail Addresses</div><div class="v">&nbsp;</div></div>
        <div class="kpi"><div class="l">Total Jobs</div><div class="v">&nbsp;</div></div>
        <div class="kpi"><div class="l">Matches</div><div class="v">&nbsp;</div></div>
        <div class="kpi"><div class="l">Match Rate</div><div class="v">&nbsp;</div></div>
        <div class="kpi"><div class="l">Match Revenue</div><div class="v">&nbsp;</div></div>
      </div>
    </div>
  </section>
</template>

<script setup>
// no script needed; exists for layout + legacy hook (#cmp-kpis)
</script>