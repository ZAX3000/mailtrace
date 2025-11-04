import { onMounted, onBeforeUnmount } from 'vue'

export function useUploadGuard(openModal: () => void) {
  const RE_RUN = /run\s*matching/i
  let observer1: MutationObserver | null = null

  function $all(sel: string, root?: Document | Element) {
    return Array.from((root || document).querySelectorAll(sel)) as HTMLElement[]
  }

  function countPickedFiles() {
    const inputs = $all('input[type="file"]')
    let total = 0
    for (const i of inputs) {
      const el = i as HTMLInputElement
      try { total += el.files?.length ?? 0 } catch {}
    }
    return total
  }

  function guardRun(ev?: Event) {
    if (countPickedFiles() < 2) {
      ev?.preventDefault?.()
      ev?.stopPropagation?.()
      openModal()
      return false
    }
    return true
  }

  function attachToRunButtons(root: Document | Element) {
    const btns = $all('button, a, [role="button"], input[type="button"], input[type="submit"]', root)
    for (const b of btns) {
      const txt =
        (b.textContent || (b as HTMLInputElement).value || b.getAttribute('aria-label') || b.getAttribute('title') || '')
          .replace(/\s+/g, ' ')
          .trim()
      if (RE_RUN.test(txt)) {
        // capture phase to block early
        b.addEventListener('click', guardRun, true)
      }
    }
  }

  function attachToForms() {
    for (const f of $all('form')) {
      f.addEventListener('submit', guardRun, true)
    }
  }

  onMounted(() => {
    attachToRunButtons(document)
    attachToForms()

    observer1 = new MutationObserver((muts) => {
      for (const m of muts) {
        for (const n of Array.from(m.addedNodes)) {
          if (n && (n as Node).nodeType === 1) attachToRunButtons(n as Element)
        }
      }
    })
    try { observer1.observe(document.body, { childList: true, subtree: true }) } catch {}
  })

  onBeforeUnmount(() => {
    observer1?.disconnect()
    observer1 = null
  })

  return { guardRun }
}
