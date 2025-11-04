<script setup lang="ts">
import { ref, watch, nextTick, onBeforeUnmount } from 'vue'

type Props = {
  modelValue: boolean
  title?: string
  message?: string
  /** CSS selector to focus/trigger when user clicks "Okay" */
  focusSelector?: string
  /** If true, "Okay" will .click() the input (opens file dialog) instead of just focusing */
  triggerFileDialog?: boolean
}
const props = withDefaults(defineProps<Props>(), {
  title: 'Action needed',
  message: 'Please pick both CSV files first.',
  focusSelector: '#mailCsv',
  triggerFileDialog: false,
})
const emit = defineEmits<{
  'update:modelValue': [boolean]
  confirm: []
  cancel: []
}>()

const bd = ref<HTMLElement | null>(null)
const dlg = ref<HTMLElement | null>(null)

function close() {
  dlg.value?.classList.remove('open')
  bd.value?.classList.remove('show')
  setTimeout(() => {
    if (bd.value) bd.value.style.display = 'none'
    emit('update:modelValue', false)
  }, 120)
  document.removeEventListener('keydown', onEsc)
}

function onEsc(e: KeyboardEvent) {
  if (e.key === 'Escape') close()
}

watch(
  () => props.modelValue,
  async (open) => {
    if (!bd.value || !dlg.value) return
    if (open) {
      bd.value.style.display = 'flex'
      await nextTick()
      bd.value.classList.add('show')
      dlg.value.classList.add('open')
      try { dlg.value.focus() } catch {}
      document.addEventListener('keydown', onEsc)
    } else {
      close()
    }
  },
  { immediate: true },
)

function onBackdrop(e: MouseEvent) {
  if (e.target === bd.value) close()
}

function onCancel() {
  emit('cancel')
  close()
}

function onConfirm() {
  emit('confirm')
  close()
  // Nudge the user toward the next step
  const el = document.querySelector(props.focusSelector) as HTMLInputElement | null
  if (el) {
    try {
      if (props.triggerFileDialog && typeof el.click === 'function') {
        el.click()        // Opens file picker
      } else {
        el.focus({ preventScroll: false })
        el.scrollIntoView?.({ block: 'center', behavior: 'smooth' })
      }
    } catch {}
  }
}

onBeforeUnmount(() => {
  document.removeEventListener('keydown', onEsc)
})
</script>

<template>
  <!-- IDs kept for existing CSS -->
  <div
    id="mt-modal-backdrop"
    role="dialog"
    aria-modal="true"
    aria-labelledby="mt-modal-title"
    ref="bd"
    @click="onBackdrop"
  >
    <div id="mt-modal" role="document" tabindex="-1" ref="dlg">
      <header>
        <span aria-hidden="true" class="dot"></span>
        <h3 id="mt-modal-title">{{ title }}</h3>
      </header>
      <div class="content">
        <p id="mt-modal-desc" v-html="message" />
        <p class="hint">
          Tip: select your <em>Mail CSV</em> and your <em>CRM CSV</em>, then click <strong>Run matching</strong>.
        </p>
      </div>
      <footer>
        <button class="btn btn-outline" id="mt-modal-cancel" type="button" @click="onCancel">Cancel</button>
        <button class="btn btn-outline" id="mt-modal-ok" type="button" @click="onConfirm">Okay</button>
      </footer>
    </div>
  </div>
</template>