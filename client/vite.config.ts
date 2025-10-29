import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

const FLASK = process.env.FLASK_URL || 'http://127.0.0.1:5000'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  // Serve /public as web root for static assets (e.g., /legacy/**, /img/**, CSS)
  publicDir: 'public',
  server: {
    port: 5173,
    // Turn off the full-screen red overlay if you want during migration
    hmr: { overlay: true }, // set to false if the overlay is annoying
    proxy: {
      // Adjust these to your Flask blueprints
      '^/(api|auth|uploads|runs|dashboard_export)': {
        target: FLASK,
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
})