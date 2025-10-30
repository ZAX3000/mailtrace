<!-- client/src/components/Navbar.vue -->
<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { useRoute, RouterLink } from 'vue-router'

const route = useRoute()

// active-state helpers (mirrors the old request.endpoint checks)
const isDash = computed(() => route.path === '/' || route.name === 'dashboard')
const isMap  = computed(() => route.path.startsWith('/map') || route.name === 'map')

// History dropdown
const open = ref(false)
function toggle() { open.value = !open.value }
function close() { open.value = false }

function onKey(e: KeyboardEvent) {
  if (e.key === 'Escape') close()
}
function onClickAway(e: MouseEvent) {
  const root = document.getElementById('mt-hist')
  if (root && !root.contains(e.target as Node)) close()
}

onMounted(() => {
  document.addEventListener('keydown', onKey)
  document.addEventListener('click', onClickAway)
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
        <!-- logo lives in client/public/img/logo.svg => /img/logo.svg -->
        <RouterLink to="/" aria-label="MailTrace home">
          <img class="mt-logo" src="/img/logo.svg" alt="MailTrace logo" />
        </RouterLink>
      </div>

      <div class="right">
        <RouterLink
          id="mt-link-dash"
          to="/"
          :class="{ active: isDash }"
        >
          Dashboard
        </RouterLink>

        <RouterLink
          id="mt-link-map"
          to="/map"
          :class="{ active: isMap }"
        >
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

          <div
            class="hist-panel"
            role="menu"
            v-show="open"
          >
            <!-- Keep these IDs so any legacy script can populate -->
            <div class="hist-empty" id="mt-hist-empty">Loading…</div>
            <div id="mt-hist-list"></div>
          </div>
        </div>
      </div>
    </div>
  </nav>
</template>