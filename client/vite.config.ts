// vite.config.ts
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'
import svgLoader from 'vite-svg-loader'

const FLASK = process.env.FLASK_URL || 'http://127.0.0.1:5000'

export default defineConfig({
  plugins: [vue(), svgLoader()],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  publicDir: 'public',
  server: {
    port: 5173,
    hmr: { overlay: true },
    cors: true,
    proxy: {
      '/api': {
        target: FLASK,
        changeOrigin: true,
        secure: false,
      },
      '/auth': {
        target: FLASK,
        changeOrigin: true,
        secure: false,
      },
      '/uploads': {
        target: FLASK,
        changeOrigin: true,
        secure: false,
      },
      '/runs': {
        target: FLASK,
        changeOrigin: true,
        secure: false,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
})