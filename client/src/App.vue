<!-- client/src/App.vue -->
<script setup lang="ts">
import { RouterView } from 'vue-router'
import Loader from '@/components/Loader.vue'
import Sidebar from '@/components/Sidebar.vue'
import '@/styles/index.css'
</script>

<template>
  <Loader />
  <div class="app">
    <Sidebar />
    <main class="app-main">
      <RouterView />
    </main>
  </div>
</template>
