<!-- src/components/Sidebar.vue -->
<template>
  <aside
    class="bg-white rounded-[10px] shadow-[0_3px_15px_rgba(0,0,0,0.06)]
           min-h-[calc(100dvh-40px)] w-[318px] flex flex-col"
    role="navigation"
    aria-label="Primary"
  >
    <!-- Brand + optional toggle -->
    <div class="flex items-center justify-between px-5 pt-5 pb-4 border-b border-var(--line-color)/60">
      <RouterLink to="/" class="flex items-center min-w-0" aria-label="MailTrace" title="MailTrace">
        <LogoMark class="h-auto w-[180px]" aria-hidden="true" />
      </RouterLink>

      <!-- keep if you still want collapse -->
      <button
        class="p-2 rounded-lg hover:bg-black/5 dark:hover:bg-white/5 transition"
        :aria-label="collapsed ? 'Expand sidebar' : 'Collapse sidebar'"
        @click="$emit('toggle')"
      >
        <component :is="collapsed ? ChevronRight : ChevronLeft" class="size-5" aria-hidden="true" />
      </button>
    </div>

    <!-- Nav -->
    <nav class="flex-1 overflow-y-auto py-3">
      <ul class="space-y-2 px-5">
        <li v-for="item in items" :key="item.to">
          <RouterLink
            :to="item.to"
            class="c-pill"
            :class="isActive(item.to) ? 'pill-active' : 'pill-idle'"
            :title="collapsed ? item.label : undefined"
          >
            <component :is="item.icon" class="c-ico" aria-hidden="true" />
            <span class="truncate">{{ item.label }}</span>
            <span v-if="item.badge" class="ml-auto text-xs px-2 py-0.5 rounded-full bg-black/5 dark:bg-white/10">
              {{ item.badge }}
            </span>
          </RouterLink>
        </li>
      </ul>
    </nav>

    <!-- Footer -->
    <div class="border-t border-var(--line-color)/60 p-3 space-y-2">
      <RouterLink to="/settings" class="c-pill pill-idle">
        <Settings class="c-ico" aria-hidden="true" />
        <span class="truncate">Settings</span>
      </RouterLink>

      <button class="w-full c-pill pill-idle" @click="onSignOut" aria-label="Sign out">
        <LogOut class="c-ico" aria-hidden="true" />
        <span class="truncate">Sign out</span>
      </button>
    </div>
  </aside>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute, RouterLink } from 'vue-router'
import {
  LayoutDashboard, Upload, History, MapPinned, CreditCard,
  Settings, LogOut, ChevronLeft, ChevronRight
} from 'lucide-vue-next'
import LogoMark from '@/assets/logo_new.svg?component'

type Item = { label: string; to: string; icon: any; badge?: string | number }

withDefaults(defineProps<{ collapsed?: boolean }>(), { collapsed: false })

const emit = defineEmits<{ (e:'toggle'): void; (e:'signout'): void }>()
const route = useRoute()

const items = computed<Item[]>(() => [
  { label: 'Summary',        to: '/',         icon: LayoutDashboard },
  { label: 'Upload & Match', to: '/upload',   icon: Upload },
  { label: 'History',        to: '/history',  icon: History },
  { label: 'Map',            to: '/map',      icon: MapPinned },
  { label: 'Billing',        to: '/billing',  icon: CreditCard },
])

function isActive(path: string) {
  if (path === '/') return route.path === '/'
  return route.path.startsWith(path)
}
function onSignOut() { emit('signout') }
</script>

<style scoped>
/* shared pill style */
.c-pill {
  display:flex; align-items:center; gap:12px;
  padding: 10px 12px; border-radius: 10px; text-decoration:none;
  color: var(--ink-color);
}
.c-ico { width:20px; height:20px; color: var(--brand-color); flex:0 0 20px; }

/* idle/hover */
.pill-idle { color: var(--ink-color); }
.pill-idle:hover { background: color-mix(in srgb, var(--brand-color) 6%, transparent); }

/* active (Figma: #F4F5F7 + subtle inset feel) */
.pill-active {
  background: #F4F5F7;
  box-shadow: inset 0 0 9px rgba(0, 0, 0, 0.12);
  color: var(--ink-color);
}

/* logo fix: prevent clipping and baseline cut */
.logo-block { display:block; }
:deep(svg) { overflow: visible; }

/* custom thin scrollbar on the nav */
nav::-webkit-scrollbar { width: 8px; }
nav::-webkit-scrollbar-thumb { border-radius: 9999px; background: rgba(120,120,120,.25); }
</style>
