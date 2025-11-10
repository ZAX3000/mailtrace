<template>
  <section
    class="mx-auto max-w-4xl p-6 space-y-4 text-slate-200"
    role="main"
    aria-label="Paywall"
  >
    <header>
      <h2 class="text-xl font-semibold">Unlock your dashboard</h2>
      <p class="text-sm text-slate-400">
        See your real matches, revenue, and map — pay to unlock now.
      </p>
    </header>

    <div class="bg-slate-900 border border-slate-700 rounded-xl p-5">
      <img
        src="https://dummyimage.com/900x360/1e293b/93c5fd&text=Dashboard+Preview"
        alt="Dashboard preview (blurred)"
        class="w-full rounded-lg blur-sm opacity-80 select-none pointer-events-none"
        draggable="false"
      />
    </div>

    <div class="flex items-center gap-3">
      <button
        class="btn bg-emerald-500 text-slate-900 font-extrabold px-4 py-2 rounded-lg
               hover:bg-emerald-400 disabled:opacity-60 disabled:cursor-not-allowed"
        :disabled="loading"
        @click="goToCheckout"
      >
        <span v-if="!loading">Continue to Checkout</span>
        <span v-else>Redirecting…</span>
      </button>

      <span v-if="error" class="text-amber-400 text-sm">{{ error }}</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

const loading = ref(false)
const error = ref<string | null>(null)

interface CheckoutResponse {
  checkout_url: string
}

onMounted(() => {
  document.title = 'Paywall • MailTrace'
})

async function goToCheckout() {
  try {
    loading.value = true
    error.value = null

    const res = await fetch('/billing/checkout', {
      method: 'POST',
      // If your Flask session/cookies are needed for Checkout, keep this:
      credentials: 'include',
      headers: {
        // Add CSRF header here if your API uses it, e.g. 'X-CSRF-Token': token
      }
    })

    if (!res.ok) throw new Error(`Checkout failed (${res.status})`)

    const data = (await res.json()) as Partial<CheckoutResponse>
    if (!data.checkout_url) throw new Error('Missing checkout_url from server')

    window.location.href = data.checkout_url
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
/* Optional: if you relied on a global .btn class before, you can delete this. */
.btn { border: none; cursor: pointer; }
</style>