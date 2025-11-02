<template>
  <div v-show="modelValue" class="fixed inset-0 z-50 grid place-items-center">
    <div class="fixed inset-0 bg-black/40" @click="close"></div>
    <div class="bg-white dark:bg-neutral-900 rounded-xl shadow-xl p-6 w-[min(680px,90vw)]">
      <h3 class="text-lg font-semibold mb-2">Mapping required</h3>
      <p class="text-sm text-neutral-600 dark:text-neutral-300 mb-4">
        We couldn’t start the run because some required columns aren’t mapped yet.
      </p>

      <div v-if="missing?.mail?.length" class="mb-3">
        <div class="font-medium mb-1">Mail needs:</div>
        <ul class="list-disc ml-6 text-sm">
          <li v-for="m in missing.mail" :key="'mail-'+m">{{ m }}</li>
        </ul>
      </div>

      <div v-if="missing?.crm?.length" class="mb-4">
        <div class="font-medium mb-1">CRM needs:</div>
        <ul class="list-disc ml-6 text-sm">
          <li v-for="m in missing.crm" :key="'crm-'+m">{{ m }}</li>
        </ul>
      </div>

      <div class="flex gap-2 justify-end">
        <button class="btn btn-ghost" @click="close">Cancel</button>
        <button class="btn" @click="edit">Edit mapping</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  modelValue: boolean
  missing?: { mail?: string[], crm?: string[] }
}>()
const emit = defineEmits<{
  (e:'update:modelValue', v:boolean): void
  (e:'edit-mapping'): void
}>()

function close(){ emit('update:modelValue', false) }
function edit(){ emit('edit-mapping'); close() }
</script>