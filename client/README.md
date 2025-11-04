# MailTrace – Frontend (Vue 3 + Vite + TS)

This package hosts the **SPA dashboard**. It talks to the Flask API in `server/`.

---

## 1) Prereqs

* **Node.js**: **20.19+** or **22.12+** (Vite 7 requires this)
* **npm** (bundled with Node) or **pnpm** (optional)
* VS Code: **Vue – Official (Volar)** + **Prettier**; **disable Vetur**

Check:

```bash
node -v
# v20.19.x or v22.12.x+
```

---

## 2) Install & Run

```bash
cd client
npm i                # or: pnpm i
npm run dev          # starts Vite dev server
```

* Dev server: [http://localhost:5173](http://localhost:5173) (by default)
* API base: proxied to Flask via Vite (see proxy below)

**Build & Preview**

```bash
npm run build        # outputs to dist/
npm run preview      # serves dist/ locally
```

---

## 3) Environment & API

Create **client/.env** (dev) and/or **.env.production**:

```env
VITE_API_BASE=http://127.0.0.1:5000   # Flask API base
```

Access in code:

```ts
const API = import.meta.env.VITE_API_BASE
```

### Dev Proxy (recommended)

`vite.config.ts` includes a proxy so frontend calls can go to `/api/...` without CORS:

```ts
// example snippet you can keep in vite.config.ts
server: {
  proxy: {
    '/api': {
      target: process.env.VITE_API_BASE || 'http://127.0.0.1:5000',
      changeOrigin: true,
      secure: false,
    },
  },
}
```

Call the API with relative URLs:

```ts
await fetch('/api/runs/last', { credentials: 'include' })
```

---

## 4) Project Layout (frontend)

```
client/
├─ index.html                # SPA shell (loads legacy CSS/JS + mounts Vue)
├─ public/
│  ├─ css/                   # moved from server/static/dashboard/css
│  ├─ img/                   # moved favicon, logos, etc.
│  └─ legacy/js/             # moved legacy JS (temporary)
├─ src/
│  ├─ main.ts                # Vue bootstrap
│  ├─ router.ts              # routes (/ and /dashboard)
│  ├─ pages/
│  │  └─ Dashboard.vue       # port of dashboard.html
│  └─ components/            # new Vue components (WIP)
├─ vite.config.ts
└─ package.json
```

---

## 5) Formatting / Lint

**VS Code settings (user or workspace):**

```json
{
  "editor.formatOnSave": true,
  "[vue]": { "editor.defaultFormatter": "esbenp.prettier-vscode" },
  "eslint.validate": ["javascript", "typescript", "typescriptreact", "vue"],
  "prettier.requireConfig": true
}
```

Optional configs:

* **.prettierrc**

  ```json
  { "semi": false, "singleQuote": true, "printWidth": 100, "trailingComma": "es5" }
  ```
* **ESLint (optional)**

  ```bash
  npm i -D eslint eslint-plugin-vue @vue/eslint-config-typescript @vue/eslint-config-prettier prettier
  ```

**package.json scripts (suggested):**

```json
{
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview",
    "lint": "eslint \"src/**/*.{ts,vue}\""
  }
}
```

---

## 6) Running With the Backend

* Start Postgres (Docker): `docker compose up -d db` (from repo root)
* Start Flask API: `python -m flask --app app:create_app run --debug -h 127.0.0.1 -p 5000`
* Start Vite: `npm run dev` (from `client/`)

Now visit: `http://localhost:5173/`

---

## 7) Build & Deploy

`npm run build` produces `client/dist/`.

**Option A – Flask serves built assets (single repo):**

* Copy `client/dist/` to `server/app/static/` (or configure Flask to serve `dist/` directly).
* Point Flask’s root route to the SPA shell (`index.html` in `dist/`).
* Keep API under `/api/*`.

**Option B – Split repos:**

* Deploy `client/dist/` to a static host / CDN.
* Set `VITE_API_BASE` to the public Flask API URL (Azure App Service).
* Configure CORS on the API or front a shared domain with path routing.

---
