<script setup lang="ts">
import { computed } from 'vue'
import { useLoader } from '@/stores/loader'

const { state, hide } = useLoader()

const pct = computed(() => `${Math.round(state.progress)}%`)
const eta = computed(() => {
  if (state.etaSeconds == null) return '≈ --:-- left'
  const m = Math.floor(state.etaSeconds / 60)
  const s = state.etaSeconds % 60
  return m > 0 ? `≈ ${m}m ${s}s left` : `≈ ${s}s left`
})
</script>

<template>
  <div
    v-show="state.open"
    class="fixed inset-0 z-[2147483646] m-0 flex items-center justify-center"
    style="background: rgba(0,0,0,.30)"
    aria-modal="true"
    role="dialog"
  >
    <div
      class="relative flex h-[510px] w-[420px] items-center justify-center overflow-hidden rounded-[28px]"
      style="background: radial-gradient(120% 120% at 50% 10%, rgba(255,255,255,.07), rgba(255,255,255,.02));
             box-shadow: 0 14px 40px rgba(0,0,0,.45), inset 0 0 0 1px rgba(255,255,255,.08);"
    >
      <button
        aria-label="Close"
        @click="hide()"
        class="absolute right-[10px] top-[10px] cursor-pointer rounded-[10px] p-[6px] text-[18px]"
        style="background: transparent; border: none; color: #cbd5e1;"
      >✕</button>

      <!-- Decorative scene (matches legacy visuals) -->
      <div class="scene">
        <svg viewBox="0 0 240 200" width="280" height="230" aria-label="Envelope back">
          <rect x="20" y="36" rx="14" ry="14" width="200" height="128" fill="#ffffff"/>
          <rect x="24" y="40" rx="12" ry="12" width="192" height="120" fill="#f5f9fc"/>
          <path d="M20,52 L120,116 L220,52 L220,36 C220,28.268 213.732,22 206,22 L34,22 C26.268,22 20,28.268 20,36 Z" fill="#dde8f2"/>
          <path d="M20,52 L120,116 L220,52" fill="none" stroke="#c8d9e7" stroke-width="2"/>
        </svg>
        <div class="chart">
          <svg viewBox="0 0 240 200" width="280" height="230">
            <defs>
              <pattern id="gridM" width="20" height="20" patternUnits="userSpaceOnUse">
                <path d="M20 0 H0 V20" fill="none" stroke="rgba(0,0,0,0.06)" stroke-width="1"/>
              </pattern>
              <linearGradient id="fillM" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="rgba(110,231,183,0.35)"/>
                <stop offset="100%" stop-color="rgba(110,231,183,0)"/>
              </linearGradient>
            </defs>
            <g id="mag"><g transform="translate(0,-12)">
              <rect x="24" y="40" width="192" height="120" fill="url(#gridM)"/>
              <path d="M32 150 H208" stroke="rgba(0,0,0,0.12)" stroke-width="1"/>
              <path d="M32 150 V56" stroke="rgba(0,0,0,0.12)" stroke-width="1"/>
              <path d="M32 150 L48 138 C56 132,64 126,72 128 S88 138,96 132 S112 108,120 112 S136 140,144 132 S160 98,168 100 S184 126,192 118 L208 118 L208 150 Z" fill="url(#fillM)"/>
              <path d="M32 150 L48 138 C56 132,64 126,72 128 S88 138,96 132 S112 108,120 112 S136 140,144 132 S160 98,168 100 S184 126,192 118 L208 118"
                    fill="none" stroke="#6ee7b7" stroke-width="3" stroke-linecap="round"/>
            </g></g>
          </svg>
        </div>
        <svg class="scope" viewBox="0 0 120 120" aria-hidden="true">
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

      <div class="absolute left-8 right-8 bottom-[144px] text-center font-semibold" style="color:#e8f1f8">
        Analyzing run
      </div>
      <div class="absolute left-8 right-8 bottom-[118px] text-center text-[14px]" style="color:#a8c3d9">
        <span>This may take a moment</span>
      </div>

      <div class="absolute left-8 right-8 bottom-12">
        <div class="h-[10px] w-full overflow-hidden rounded-full" style="background:rgba(255,255,255,0.08); box-shadow: inset 0 0 0 1px rgba(255,255,255,.06);">
          <div :style="{ width: pct }"
               class="h-full rounded-full transition-[width] duration-200 ease-linear"
               style="background:linear-gradient(90deg,#16a34a,#22c55e)"></div>
        </div>
        <div class="mt-2 flex items-center justify-between text-[12px]" style="color:#a8c3d9">
          <span>Completed</span><span class="font-semibold">{{ pct }}</span>
        </div>
      </div>

      <div class="absolute left-8 right-8 bottom-5 text-center" style="color:#a8c3d9">
        {{ eta }}
      </div>
    </div>
  </div>
</template>