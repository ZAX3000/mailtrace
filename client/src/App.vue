<!-- client/src/App.vue -->
<script setup lang="ts">
import { RouterView } from 'vue-router'
import Loader from '@/components/Loader.vue'
import Navbar from '@/components/Navbar.vue'

import '@/styles/index.css'

</script>

<template>
  <Loader />
  <Navbar />
  <RouterView />
</template>
