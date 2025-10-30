<!-- client/src/App.vue -->
<script setup lang="ts">
import { RouterView } from 'vue-router'
import Loader from '@/components/Loader.vue'
import Navbar from '@/components/Navbar.vue'

import '@/styles/tokens.css'
import '@/styles/theme-light.css'
import '@/styles/base.css'
import '@/styles/components.css'
import '@/styles/navbar.css'

</script>

<template>
  <Loader />
  <Navbar />
  <RouterView />
</template>

<style>
  html, body, #app { height: 100%; }
</style>
