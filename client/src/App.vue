<!-- client/src/App.vue -->
<script setup lang="ts"></script>

<template>
  <Loader />      <!-- always present -->
  <RouterView />
</template>

<style>
/* Optional: ensure the app fills the viewport (helps legacy CSS behave) */
html, body, #app { height: 100%; }
</style>
