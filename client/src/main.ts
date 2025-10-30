// client/src/main.ts
import { createApp } from 'vue'
import App from './App.vue'
import router from './router'

import Loader from '@/components/Loader.vue'

const app = createApp(App)
app.component('Loader', Loader)
app.use(router).mount('#app')