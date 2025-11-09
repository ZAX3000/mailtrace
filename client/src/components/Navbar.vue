<!-- client/src/components/Navbar.vue -->
<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useRoute, RouterLink } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useRuns } from '@/stores/runs'

const route = useRoute()

// active-state helpers (mirrors the old request.endpoint checks)
const isDash = computed(() => route.path === '/' || route.name === 'dashboard')
const isMap  = computed(() => route.path.startsWith('/map') || route.name === 'map')

// History dropdown (keep your existing behavior)
const open = ref(false)
function toggle() {
  open.value = !open.value
  // lazy-load on first open
  if (open.value && !runs.items.length && !runs.loading) runs.load(25)
}
function close() { open.value = false }

function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') close()
}
function onClickAway(e: MouseEvent) {
  const root = document.getElementById('mt-hist')
  if (root && !root.contains(e.target as Node)) close()
}

// ---- Runs store wiring (NEW) ----
const runs = useRuns()
const { items, loading, error, activeRunId } = storeToRefs(runs)

async function selectRun(id: string) {
  await runs.setActive(id)  // will also refresh KPI (per store action)
  close()
}

onMounted(() => {
  document.addEventListener('keydown', onKey)
  document.addEventListener('click', onClickAway)
  // optional eager load (comment out if you prefer lazy in toggle):
  // runs.load(25)
})
onBeforeUnmount(() => {
  document.removeEventListener('keydown', onKey)
  document.removeEventListener('click', onClickAway)
})
</script>

<template>
  <!-- keep classes/structure so existing CSS applies -->
  <nav class="mt-nav" role="navigation" aria-label="Main">
    <div class="wrap">
      <div class="left logo">
        <RouterLink to="/" aria-label="MailTrace home">
          <img class="mt-logo" src="/img/logo.svg" alt="MailTrace logo" />
        </RouterLink>
      </div>

      <div class="right">
        <RouterLink id="mt-link-dash" to="/" :class="{ active: isDash }">
          Dashboard
        </RouterLink>

        <RouterLink id="mt-link-map" to="/map" :class="{ active: isMap }">
          Map
        </RouterLink>

        <!-- history dropdown (IDs preserved for any legacy wiring) -->
        <div class="hist" id="mt-hist">
          <button
            class="hist-btn"
            type="button"
            aria-haspopup="true"
            :aria-expanded="open ? 'true' : 'false'"
            @click="toggle"
          >
            History ▾
          </button>

          <div class="hist-panel" role="menu" v-show="open">
            <!-- Replaced legacy placeholders with actual data rendering -->
            <div v-if="loading" class="hist-empty" id="mt-hist-empty">Loading…</div>
            <div v-else-if="error" class="hist-empty text-amber-600">
              {{ error }}
            </div>
            <div v-else-if="!items.length" class="hist-empty" id="mt-hist-empty">
              No runs yet.
            </div>
            <div v-else id="mt-hist-list" class="hist-list">
              <button
                v-for="r in items"
                :key="r.id"
                class="hist-item"
                :class="{ active: r.id === activeRunId }"
                @click="selectRun(r.id)"
              >
                <div class="hist-line-1">
                  {{ new Date(r.started_at).toLocaleString() }}
                </div>
                <div v-if="r.summary" class="hist-line-2">
                  {{ r.summary }}
                </div>
              </button>
            </div>

            <!-- optional refresh button inside panel -->
            <div class="hist-footer">
              <button class="hist-refresh" @click="runs.load(25)" :disabled="loading">
                Refresh
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </nav>
</template>

<style scoped>
/* Optional minimal styles to match your existing classes;
   remove if you already style .hist-*, .hist-item, etc. */
.hist-list { display: flex; flex-direction: column; max-height: 20rem; overflow: auto; }
.hist-item { text-align: left; padding: 0.5rem 0.75rem; border-radius: 0.5rem; }
.hist-item:hover { background: rgba(0,0,0,0.04); }
.hist-item.active { background: rgba(0,0,0,0.06); }
.hist-line-1 { font-size: 0.875rem; font-weight: 600; }
.hist-line-2 { font-size: 0.75rem; color: #6b7280; }
.hist-footer { padding: 0.5rem; border-top: 1px solid rgba(0,0,0,0.06); display: flex; justify-content: flex-end; }
.hist-refresh { font-size: 0.75rem; padding: 0.25rem 0.5rem; border-radius: 0.375rem; }
</style>