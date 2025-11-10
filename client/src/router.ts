// src/router/index.js
import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  // Dashboard stays on root to avoid breaking existing links
  {
    path: '/',
    name: 'Dashboard',
    component: () => import('./pages/Dashboard.vue'),
    meta: { title: 'Dashboard • MailTrace'/*, requiresPro: true */ }
  },
  // Nice to have: alias /dashboard -> /
  { path: '/dashboard', redirect: '/' },

  // Home (previously unwired)
  {
    path: '/home',
    name: 'Home',
    component: () => import('./pages/Home.vue'),
    meta: { title: 'Home • MailTrace' }
  },

  // Map (previously unwired)
  {
    path: '/map',
    name: 'Map',
    component: () => import('./pages/MapPage.vue'),
    meta: { title: 'Map • MailTrace'/*, requiresPro: true */ }
  },

  // Paywall (new Vue version)
  {
    path: '/paywall',
    name: 'Paywall',
    component: () => import('./pages/Paywall.vue'), // <- change to ./views/Paywall.vue if that's where you put it
    meta: { title: 'Paywall • MailTrace' }
  },

  // (Optional) Stripe return pages — add quick placeholders below if you don’t have them yet
  /*{
    path: '/billing/success',
    name: 'BillingSuccess',
    component: () => import('./pages/BillingSuccess.vue'),
    meta: { title: 'Payment Success • MailTrace' }
  },
  {
    path: '/billing/cancel',
    name: 'BillingCancel',
    component: () => import('./pages/BillingCancel.vue'),
    meta: { title: 'Payment Canceled • MailTrace' }
  },*/

  // Catch-all → send them to Dashboard
  { path: '/:pathMatch(.*)*', redirect: '/' }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

/**
 * (Optional) Gating example:
 * Uncomment this block when your “pro/paid” flag is available
 * (e.g., from Pinia, Vuex, or window.APP_USER set by Flask).
 */
// router.beforeEach((to) => {
//   const isPro = window.APP_USER?.is_pro // or use your store getter
//   if (to.meta?.requiresPro && !isPro) {
//     return { name: 'Paywall', query: { redirect: to.fullPath } }
//   }
// })

export default router
