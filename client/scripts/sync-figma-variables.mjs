import fs from 'node:fs/promises'
import path from 'node:path'

const FILE_KEY = process.env.FIGMA_FILE_KEY
const TOKEN    = process.env.FIGMA_TOKEN

if (!FILE_KEY || !TOKEN) {
  console.error('Missing FIGMA_FILE_KEY or FIGMA_TOKEN')
  process.exit(1)
}

async function getJson(url) {
  const res = await fetch(url, { headers: { 'X-Figma-Token': TOKEN } })
  if (!res.ok) {
    console.error('Figma API error:', res.status, await res.text())
    process.exit(1)
  }
  return res.json()
}

function kebab(name) {
  return '--' + name.trim().toLowerCase()
    .replace(/[^\w/ -]+/g, '')
    .replace(/\s+/g, '-')
    .replace(/\//g, '-')
}

function rgbaToCss({ r, g, b, a = 1 }) {
  const R = Math.round(r * 255), G = Math.round(g * 255), B = Math.round(b * 255)
  return a === 1
    ? `#${[R,G,B].map(x => x.toString(16).padStart(2,'0')).join('')}`
    : `rgba(${R}, ${G}, ${B}, ${a})`
}

/**
 * Heuristic units:
 * - Names including radius, radii, corner, spacing, space, size, width, height, px => append px for numbers
 * - opacity -> unitless 0..1
 * - letter/line-height can be unitless, but if number > 3 we’ll add px
 */
function toCssValue(v, resolvedType, varNameKebab) {
  if (v == null) return null
  if (resolvedType === 'COLOR') return rgbaToCss(v)

  if (typeof v === 'number') {
    const n = v
    const name = varNameKebab
    const wantsPx = /(radius|radii|corner|spacing|space|gap|padding|margin|size|width|height|px)/.test(name)
    const isOpacity = /opacity/.test(name)
    const isLineHeight = /line[-]?height/.test(name)
    if (isOpacity) return String(n)                // 0..1
    if (isLineHeight) return n > 3 ? `${n}px` : String(n) // common pattern
    return wantsPx ? `${n}px` : String(n)
  }

  return String(v) // STRING/BOOLEAN fallbacks
}

function buildScopes(payload) {
  const variables   = payload?.meta?.variablesById ?? {}
  const collections = payload?.meta?.variableCollections ?? {}

  // Build: { collectionId: { modes: [{id,name}], defaultModeId } }
  const cols = {}
  for (const c of Object.values(collections)) {
    const modes = c.modes || []
    const light = modes.find(m => /(^|\s)light(\s|$)/i.test(m.name))
    cols[c.id] = {
      modes,
      defaultModeId: (light || modes[0] || {}).modeId
    }
  }

  // Group variables by collection
  const byCollection = {}
  for (const v of Object.values(variables)) {
    if (!byCollection[v.variableCollectionId]) byCollection[v.variableCollectionId] = []
    byCollection[v.variableCollectionId].push(v)
  }

  // Emit CSS scopes
  const chunks = []

  // 1) :root uses each collection's default (prefer Light)
  const rootLines = [':root {']
  for (const [colId, list] of Object.entries(byCollection)) {
    const modeId = cols[colId].defaultModeId
    for (const v of list) {
      const value = v.valuesByMode?.[modeId]
      const cssVal = toCssValue(value, v.resolvedType, kebab(v.name))
      if (cssVal == null) continue
      rootLines.push(`  ${kebab(v.name)}: ${cssVal};`)
    }
  }
  rootLines.push('}', '')
  chunks.push(rootLines.join('\n'))

  // 2) Other modes as opt-in scopes (attribute + class)
  for (const [colId, list] of Object.entries(byCollection)) {
    for (const m of cols[colId].modes) {
      if (m.modeId === cols[colId].defaultModeId) continue
      const attrSel = `[data-theme="${m.name.toLowerCase().replace(/\s+/g,'-')}"]`
      const classSel = `.theme-${m.name.toLowerCase().replace(/\s+/g,'-')}`
      for (const sel of [attrSel, classSel]) {
        const lines = [`${sel} {`]
        for (const v of list) {
          const value = v.valuesByMode?.[m.modeId]
          const cssVal = toCssValue(value, v.resolvedType, kebab(v.name))
          if (cssVal == null) continue
          lines.push(`  ${kebab(v.name)}: ${cssVal};`)
        }
        lines.push('}', '')
        chunks.push(lines.join('\n'))
      }
    }
  }

  return chunks.join('\n')
}

const data = await getJson(`https://api.figma.com/v1/files/${FILE_KEY}/variables/local`)
const css  = buildScopes(data)

const outDir = path.resolve('src', 'styles')
await fs.mkdir(outDir, { recursive: true })
await fs.writeFile(path.join(outDir, 'tokens.css'), css, 'utf8')
console.log('Wrote client/src/styles/tokens.css')