<!-- src/components/Loader.vue -->
<script setup lang="ts">
import { onMounted, onBeforeUnmount, watch, computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useLoader } from '@/stores/loader'

const loader = useLoader()
const { open, locked, progress, message, etaSeconds } = storeToRefs(loader)
const isDone = computed(() => progress.value >= 100)

// expose a force-close that ignores locked (you wanted "always closable")
function closeNow() { loader.hide(true) }

// body lock for scroll-stop + quick visual check
const BODY_ATTR = '__mt_loader_lock'
watch(locked, (val) => {
  document.body.setAttribute(`data-${BODY_ATTR}`, val ? '1' : '0')
  document.body.style.overflow = val ? 'hidden' : ''
}, { immediate: true })

onMounted(() => {
  if (locked.value) {
    document.body.setAttribute(`data-${BODY_ATTR}`, '1')
    document.body.style.overflow = 'hidden'
  }
})
onBeforeUnmount(() => {
  document.body.removeAttribute(`data-${BODY_ATTR}`)
  document.body.style.overflow = ''
})

const pctStr = computed(() => `${Math.round(progress.value || 0)}%`)
const etaStr = computed(() => {
  const sec = etaSeconds.value
  if (sec == null) return '≈ --:-- left'
  const m = Math.floor(sec / 60)
  const s = sec % 60
  return m > 0 ? `≈ ${m}m ${s}s left` : `≈ ${s}s left`
})
</script>

<template>
  <teleport to="body">
    <div v-show="open" class="mt-overlay" role="dialog" aria-label="Analyzing run" aria-modal="true">
      <div class="mt-card" role="progressbar" :aria-valuenow="progress" aria-valuemin="0" aria-valuemax="100">
        <!-- Close -->
        <button class="mt-close" title="Close" aria-label="Close" @click.stop="closeNow">✕</button>

        <!-- Decorative scene -->
        <div class="scene" aria-hidden="true">
          <!-- envelope -->
          <svg class="env" viewBox="0 0 240 200" width="280" height="230">
            <rect x="20" y="36" rx="14" ry="14" width="200" height="128" fill="#ffffff"/>
            <rect x="24" y="40" rx="12" ry="12" width="192" height="120" fill="#f5f9fc"/>
            <path d="M20,52 L120,116 L220,52 L220,36 C220,28.268 213.732,22 206,22 L34,22 C26.268,22 20,28.268 20,36 Z" fill="#dde8f2"/>
            <path d="M20,52 L120,116 L220,52" fill="none" stroke="#c8d9e7" stroke-width="2"/>
          </svg>

          <!-- faint chart grid + sparkline -->
          <svg class="chart" viewBox="0 0 240 200" width="280" height="230">
            <defs>
              <pattern id="gridM" width="20" height="20" patternUnits="userSpaceOnUse">
                <path d="M20 0 H0 V20" fill="none" stroke="rgba(0,0,0,0.06)" stroke-width="1"/>
              </pattern>
              <linearGradient id="fillM" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="rgba(110,231,183,0.35)"/>
                <stop offset="100%" stop-color="rgba(110,231,183,0)"/>
              </linearGradient>
            </defs>
            <g transform="translate(0,-12)">
              <rect x="24" y="40" width="192" height="120" fill="url(#gridM)"/>
              <path d="M32 150 H208" stroke="rgba(0,0,0,0.12)" stroke-width="1"/>
              <path d="M32 150 V56" stroke="rgba(0,0,0,0.12)" stroke-width="1"/>
              <path d="M32 150 L48 138 C56 132,64 126,72 128 S88 138,96 132 S112 108,120 112 S136 140,144 132 S160 98,168 100 S184 126,192 118 L208 118 L208 150 Z"
                    fill="url(#fillM)"/>
              <path d="M32 150 L48 138 C56 132,64 126,72 128 S88 138,96 132 S112 108,120 112 S136 140,144 132 S160 98,168 100 S184 126,192 118 L208 118"
                    fill="none" stroke="#23c268" stroke-width="3" stroke-linecap="round"/>
            </g>
          </svg>

<!-- magnifying glass: gentle bob (not circular) -->
<div class="scope-center" :class="{ paused: isDone }" aria-hidden="true">
  <div class="drift">
    <svg class="scope" viewBox="0 0 120 120">
      <defs>
        <linearGradient id="rimM" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stop-color="#1b6a3a"/><stop offset="100%" stop-color="#1db954"/>
        </linearGradient>
        <linearGradient id="handleM" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stop-color="#16a34a"/><stop offset="100%" stop-color="#12843e"/>
        </linearGradient>
      </defs>
      <circle cx="54" cy="52" r="33" fill="none" stroke="url(#rimM)" stroke-width="6"/>
      <g transform="translate(54,52) rotate(40)">
        <rect x="31" y="-9" width="10" height="18" rx="5" fill="#0f7a3a"/>
        <rect x="41" y="-7" width="42" height="14" rx="7" fill="url(#handleM)"/>
        <rect x="43" y="-5" width="24" height="10" rx="5" fill="rgba(255,255,255,0.18)"/>
      </g>
    </svg>
  </div>
</div>

        </div>

        <!-- Copy -->
        <div class="title">{{ message || 'Analyzing run' }}</div>
        <div class="subtitle">This may take a moment</div>

        <!-- Progress -->
        <div class="bar">
          <div class="fill" :style="{ width: (progress || 0) + '%' }" />
        </div>
        <div class="bar-meta">
          <span>Completed</span>
          <span class="pct">{{ pctStr }}</span>
        </div>
        <div class="eta">{{ etaStr }}</div>
      </div>
    </div>
  </teleport>
</template>

<style scoped>
/* Overlay + card shell (matches your working one, keeps z-index huge) */
.mt-overlay{
  position: fixed; inset: 0; z-index: 2147483646;
  display: grid; place-items: center;
  background: rgba(7,13,23,.42);
  backdrop-filter: blur(2px);
}
.mt-card{
  position: relative;
  width: min(560px, 92vw);
  padding: 24px 28px 26px 28px;
  border-radius: 26px;
  background: radial-gradient(120% 120% at 50% 10%, rgba(255,255,255,.07), rgba(255,255,255,.02));
  box-shadow: 0 14px 40px rgba(0,0,0,.45), inset 0 0 0 1px rgba(255,255,255,.08);
}

/* Close button */
.mt-close{
  position: absolute; top: 10px; right: 10px;
  border: 0; background: rgba(255,255,255,.14);
  width: 34px; height: 34px; border-radius: 10px;
  color: #e6faf0; font-size: 18px; line-height: 1;
  display: grid; place-items: center; cursor: pointer;
  backdrop-filter: blur(1px);
}

/* Scene layout: everything is absolutely positioned inside this box
   so global CSS can't nudge it. */
.scene{
  position: relative;
  width: 360px; height: 260px;
  margin: 22px auto 30px auto;
  pointer-events: none; user-select: none;
  opacity: .98;
}
.scene .env{
  position: absolute; left: 50%; top: -6px;
  transform: translate(-50%, 0);
  filter: drop-shadow(0 10px 18px rgba(0,0,0,.25));
}
.scene .chart{
  position: absolute; left: 50%; bottom: 22px;
  transform: translateX(-50%);
  opacity: .45;
}
.scope-wrap{
  /* orbit center */
  position: absolute; left: 50%; top: 56%;
  width: 1px; height: 1px; transform: translate(-50%, -50%);
  animation: orbit 6.4s linear infinite;
}
.scope{
  /* radius of orbit = translate distance below in keyframes */
  width: 120px; height: 120px;
  filter: drop-shadow(0 10px 16px rgba(0,0,0,.28));
}

/* Orbit animation: rotate the wrapper while translating out from center.
   The inner rotate negates spin so the glass appears to face "upright". */
@keyframes orbit {
  0%   { transform: translate(-50%, -50%) rotate(0deg);   }
  100% { transform: translate(-50%, -50%) rotate(360deg); }
}
.scope-wrap .scope{
  animation: face 6.4s linear infinite, bob 2.6s ease-in-out infinite;
  transform-origin: 0 0;
}
@keyframes face {
  from { transform: rotate(0deg) translate(64px) rotate(0deg); }
  to   { transform: rotate(-360deg) translate(64px) rotate(360deg); }
}
@keyframes bob {
  0%,100% { filter: drop-shadow(0 8px 14px rgba(0,0,0,.22)); }
  50%     { filter: drop-shadow(0 14px 22px rgba(0,0,0,.30)); }
}

/* Copy + progress */
.title{
  position: relative; text-align: center;
  font-weight: 700; color: #e8f1f8; margin-top: 2px;
}
.subtitle{
  text-align: center; font-size: 14px; color: #a8c3d9; margin-top: 4px;
}
.bar{
  margin: 16px 6px 4px 6px; height: 10px; border-radius: 999px; overflow: hidden;
  background: rgba(255,255,255,.08);
  box-shadow: inset 0 0 0 1px rgba(255,255,255,.06);
}
.fill{
  height: 100%;
  background: linear-gradient(90deg,#16a34a,#22c55e);
  border-radius: 999px;
  transition: width .25s linear;
}
.bar-meta{
  display: flex; justify-content: space-between; align-items: center;
  font-size: 12px; color: #a8c3d9; margin: 2px 6px 0 6px;
}
.bar-meta .pct{ font-weight: 700; color: #e8f1f8; }
.eta{ text-align: center; color: #a8c3d9; margin-top: 6px; font-size: 12px; }
</style>