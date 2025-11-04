<template>
  <Teleport to="body">
    <div v-show="modelValue" class="fixed inset-0 z-2147483647 grid place-items-center">
      <div class="fixed inset-0 bg-black/40" @click="close" aria-hidden="true"></div>

      <div
        class="bg-white dark:bg-neutral-900 rounded-xl shadow-xl p-6 w-[min(680px,90vw)]"
        role="dialog"
        aria-modal="true"
        aria-labelledby="mapper-title"
      >
        <h3 id="mapper-title" class="text-lg font-semibold mb-2">Mapping required</h3>
        <p class="text-sm text-neutral-600 dark:text-neutral-300 mb-4">
          We couldn’t start the run because some required columns aren’t mapped yet.
        </p>

        <div v-if="missingLocal.mail?.length" class="mb-3">
          <div class="font-medium mb-1">Mail needs:</div>
          <ul class="list-disc ml-6 text-sm">
            <li v-for="m in missingLocal.mail" :key="'mail-'+m">{{ m }}</li>
          </ul>
        </div>

        <div v-if="missingLocal.crm?.length" class="mb-4">
          <div class="font-medium mb-1">CRM needs:</div>
          <ul class="list-disc ml-6 text-sm">
            <li v-for="m in missingLocal.crm" :key="'crm-'+m">{{ m }}</li>
          </ul>
        </div>

        <div class="flex gap-2 justify-end">
          <button class="btn btn-ghost" @click="close">Cancel</button>
          <button class="btn" @click="edit">Edit mapping</button>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { reactive, watch, onMounted, onBeforeUnmount } from 'vue'

const props = defineProps<{
  modelValue: boolean
  missing?: { mail?: string[]; crm?: string[] }
}>()

const emit = defineEmits<{
  (e:'update:modelValue', v:boolean): void
  (e:'update:missing', v:{ mail?: string[]; crm?: string[] }): void
  (e:'edit-mapping'): void
}>()

// Local mirror so we can show event-fed data even if parent hasn't set it yet
const missingLocal = reactive<{ mail?: string[]; crm?: string[] }>({})

// keep local mirror in sync with parent prop
watch(() => props.missing, (v) => {
  missingLocal.mail = v?.mail ?? []
  missingLocal.crm  = v?.crm  ?? []
}, { immediate: true })

function close(){ emit('update:modelValue', false) }
function edit(){ emit('edit-mapping'); close() }

// Handle global event from useRun / UploadCard
function onOpenMapper(evt: Event) {
  const e = evt as CustomEvent
  const detail = (e?.detail ?? {}) as { run_id?: string; source?: string; missing?: {mail?:string[]; crm?:string[]} }
  const miss = detail.missing || {}

  console.debug('[mapper-modal] open via event', detail)

  // push to parent via v-model:missing, and update our local copy
  emit('update:missing', miss)
  missingLocal.mail = miss.mail ?? []
  missingLocal.crm  = miss.crm  ?? []

  // show modal
  emit('update:modelValue', true)
}

onMounted(() => {
  window.addEventListener('mt:open-mapper', onOpenMapper as EventListener)
  console.debug('[mapper-modal] mounted; listening for mt:open-mapper')
})
onBeforeUnmount(() => {
  window.removeEventListener('mt:open-mapper', onOpenMapper as EventListener)
  console.debug('[mapper-modal] unmounted; listener removed')
})
</script>