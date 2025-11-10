<!-- src/pages/MapPage.vue -->
<template>
  <section class="min-h-dvh flex flex-col">
    <!-- Toolbar -->
    <div class="flex items-center gap-4 px-4 py-3 border-b">
      <label class="font-semibold flex items-center gap-2">
        <input type="checkbox" v-model="kinds.match" /> Matches
      </label>
      <label class="font-semibold flex items-center gap-2">
        <input type="checkbox" v-model="kinds.mail" /> Mail addresses
      </label>
      <label class="font-semibold flex items-center gap-2">
        <input type="checkbox" v-model="kinds.crm" /> CRM addresses
      </label>

      <span class="ml-4 flex items-center gap-2">
        From <input type="date" v-model="from" class="border rounded px-2 py-1">
        To <input type="date" v-model="to" class="border rounded px-2 py-1">
      </span>

      <button
        class="ml-2 px-3 py-1.5 rounded-lg border hover:bg-black/5 dark:hover:bg-white/5 transition"
        :disabled="loading"
        @click="draw"
      >
        {{ loading ? 'Loading…' : 'Apply' }}
      </button>

      <span class="ml-auto opacity-70 text-xs">{{ enginePill }}</span>
    </div>

    <!-- Map -->
    <div ref="mapEl" class="flex-1 min-h-[360px]" />
  </section>
</template>

<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, reactive } from 'vue'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

// MarkerCluster plugin (runtime + CSS)
import 'leaflet.markercluster'
import 'leaflet.markercluster/dist/MarkerCluster.css'
import 'leaflet.markercluster/dist/MarkerCluster.Default.css'

// Fix Leaflet default icon paths under Vite
import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png'
import iconUrl from 'leaflet/dist/images/marker-icon.png'
import shadowUrl from 'leaflet/dist/images/marker-shadow.png'
L.Icon.Default.mergeOptions({ iconRetinaUrl, iconUrl, shadowUrl })

// ---- Types ---------------------------------------------------------------
type Point = {
  lat: number
  lon: number
  kind?: string
  address?: string
  date?: string
}

/** Cluster-like group type that avoids `this` return types */
type ClusterLike = L.LayerGroup & {
  clearLayers(): L.LayerGroup
  addLayer(layer: L.Layer): L.LayerGroup
}

// ---- Refs / state --------------------------------------------------------
const mapEl = ref<HTMLDivElement | null>(null)
let map: L.Map | null = null
let cluster: ClusterLike | null = null
const loading = ref(false)
const enginePill = ref('Engine: Leaflet')

// filters
const kinds = reactive({ match: true, mail: false, crm: false })
const from = ref<string>('')
const to = ref<string>('')

// Your API endpoint (array or GeoJSON FeatureCollection)
const MAP_ENDPOINT = '/api/map'

// ---- Helpers -------------------------------------------------------------
function selectedKinds(): string[] {
  const out: string[] = []
  if (kinds.match) out.push('match')
  if (kinds.mail) out.push('mail')
  if (kinds.crm) out.push('crm')
  return out
}

function buildQuery(): string {
  const params = new URLSearchParams()
  selectedKinds().forEach(k => params.append('kind', k))
  if (from.value) params.append('from', from.value)
  if (to.value) params.append('to', to.value)
  return params.toString()
}

async function loadPoints(): Promise<Point[]> {
  const res = await fetch(`${MAP_ENDPOINT}?${buildQuery()}`)
  const json = await res.json().catch(() => null) as any
  if (!json) return []

  // Accept either array of {lat,lon,...} or GeoJSON FeatureCollection
  if (Array.isArray(json)) {
    return json.filter(p => typeof p?.lat === 'number' && typeof p?.lon === 'number')
  }
  if (json && json.type === 'FeatureCollection' && Array.isArray(json.features)) {
    return json.features
      .map((f: any) => {
        const g = f?.geometry
        if (g?.type === 'Point' && Array.isArray(g.coordinates) && g.coordinates.length >= 2) {
          return {
            lon: Number(g.coordinates[0]),
            lat: Number(g.coordinates[1]),
            kind: f?.properties?.kind,
            address: f?.properties?.address || f?.properties?.label,
            date: f?.properties?.date,
          } as Point
        }
        return null
      })
      .filter(Boolean) as Point[]
  }
  return []
}

function createCluster(): ClusterLike {
  const anyL = L as any
  return (anyL.markerClusterGroup ? anyL.markerClusterGroup() : L.layerGroup()) as ClusterLike
}

function ensureMapSized() {
  if (map) map.invalidateSize(false)
}

// ---- Draw flow -----------------------------------------------------------
async function draw() {
  if (!map || !cluster) return
  loading.value = true
  try {
    const points = await loadPoints()

    cluster.clearLayers()

    // Use LatLngTuple[] to make TS accept fitBounds without complaints
    const bounds: L.LatLngTuple[] = []

    for (const p of points) {
      if (typeof p.lat !== 'number' || typeof p.lon !== 'number') continue
      const marker = L.marker([p.lat, p.lon])
        .bindPopup(`<b>${(p.kind || '').toUpperCase()}</b><br>${p.address || ''}<br>${p.date || ''}`)
      cluster.addLayer(marker)
      bounds.push([p.lat, p.lon])
    }

    if (bounds.length) {
      map.fitBounds(bounds as L.LatLngBoundsLiteral, { padding: [40, 40] })
    } else {
      map.setView([37.8, -96], 4) // USA default
    }

    map.invalidateSize(false)
  } finally {
    loading.value = false
  }
}

// ---- Lifecycle -----------------------------------------------------------
function initMap() {
  if (!mapEl.value) return
  map = L.map(mapEl.value, {
    center: [37.8, -96],
    zoom: 4,
  })
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap',
  }).addTo(map)

  cluster = createCluster()
  cluster.addTo(map)

  ensureMapSized()
  window.addEventListener('resize', ensureMapSized)
}

onMounted(async () => {
  initMap()
  await draw()
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', ensureMapSized)
  if (map) {
    map.remove()
    map = null
  }
  cluster = null
})
</script>

<style scoped>
:deep(.leaflet-container) {
  width: 100%;
  height: 100%;
}
</style>