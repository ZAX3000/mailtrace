import { createRouter, createWebHistory } from 'vue-router'
export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: () => import('./pages/Dashboard.vue') },
    // { path: '/uploads', component: () => import('./pages/Uploads.vue') },
    // { path: '/runs', component: () => import('./pages/Runs.vue') },
    // { path: '/map', component: () => import('./pages/Map.vue') },
    // { path: '/billing', component: () => import('./pages/Billing.vue') },
  ],
})