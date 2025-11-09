<!-- src/components/HistoryDropdown.vue -->
<script setup lang="ts">
import { ref, onMounted } from 'vue'

type RunItem = {
  id: string
  started_at: string   // ISO timestamp
  summary?: string     // optional short text (e.g., "412 matches")
}

const props = defineProps<{
  modelValue?: boolean        // controls open state (if you want external control)
  limit?: number              // how many runs to fetch
}>()

const emit = defineEmits<{
  (e: 'update:modelValue', open: boolean): void
  (e: 'select', run: RunItem): void
}>()

const open = ref(false)
const loading = ref(false)
const error = ref<string | null>(null)
const runs = ref<RunItem[]>([])

function toggle() {
  open.value = !open.value
  emit('update:modelValue', open.value)
  if (open.value && !runs.value.length) load()
}

async function load() {
  loading.value = true
  error.value = null
  try {
    const q = props.limit ? `?limit=${props.limit}` : ''
    const res = await fetch(`/api/runs${q}`, { credentials: 'same-origin' })
    const j = await res.json()
    // Expect { items: [{ id, started_at, summary? }], ... }
    runs.value = Array.isArray(j?.items) ? j.items : []
  } catch (e: any) {
    error.value = e?.message || 'Failed to load runs'
  } finally {
    loading.value = false
  }
}

function choose(run: RunItem) {
  open.value = false
  emit('update:modelValue', false)
  emit('select', run)
}

// Optional: pull-to-refresh button inside the menu
async function refreshList() {
  await load()
}

// preload on mount (nice UX)
onMounted(() => { load() })
</script>

<template>
  <div class="relative inline-block text-left">
    <button
      class="inline-flex items-center gap-1 px-3 py-1.5 rounded-md hover:bg-gray-100"
      type="button"
      aria-haspopup="menu"
      :aria-expanded="open ? 'true' : 'false'"
      @click="toggle"
    >
      History
      <svg width="14" height="14" viewBox="0 0 20 20" aria-hidden="true">
        <path d="M5.5 7.5L10 12l4.5-4.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
      </svg>
    </button>

    <div
      v-show="open"
      class="absolute right-0 mt-2 w-72 origin-top-right rounded-xl bg-white shadow-lg ring-1 ring-black/5 focus:outline-none z-50"
      role="menu" tabindex="-1"
    >
      <div class="p-2 border-b flex items-center justify-between">
        <span class="text-sm font-medium">Recent Runs</span>
        <button class="btn btn-xs" @click.stop="refreshList" :disabled="loading">
          Refresh
        </button>
      </div>

      <div class="max-h-80 overflow-auto">
        <div v-if="loading" class="p-3 text-sm text-gray-500">Loading…</div>
        <div v-else-if="error" class="p-3 text-sm text-amber-600">{{ error }}</div>
        <div v-else-if="!runs.length" class="p-3 text-sm text-gray-500">No runs yet.</div>

        <ul v-else class="p-1">
          <li v-for="r in runs" :key="r.id">
            <button
              class="w-full text-left px-3 py-2 rounded-lg hover:bg-gray-50 focus:bg-gray-50"
              role="menuitem"
              @click="choose(r)"
            >
              <div class="text-sm font-medium">
                {{ new Date(r.started_at).toLocaleString() }}
              </div>
              <div v-if="r.summary" class="text-xs text-gray-500">
                {{ r.summary }}
              </div>
            </button>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<style scoped>
.btn { @apply inline-flex items-center px-2.5 py-1 rounded-md bg-gray-100 hover:bg-gray-200 text-sm; }
.btn[disabled] { @apply opacity-50 cursor-not-allowed; }
.btn-xs { @apply px-2 py-0.5 text-xs; }
</style>