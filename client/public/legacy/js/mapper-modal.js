// /static/dashboard/js/mapper-modal.js
// UI-only column mapper modal.
// - Builds mapping pickers for Mail + CRM sides
// - Prefills from localStorage and/or caller-provided auto suggestions
// - Accepts an optional SYN (header synonyms) hint from the caller
// - Returns the chosen mapping (or null if cancelled)
//
// This file does NOT parse/normalize dates or enforce required fields.
// Keep those concerns in mtMapper_v12_improveDates.js and mtMapper_v10_enforce.js.

(() => {
  'use strict';

  // Guard against double-load
  if (window.openColumnMapper) return;

  // ---- storage helpers ------------------------------------------------------

  const STORAGE_KEY = 'mt_column_mapping_v1';

  function readMappingFromStorage() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}'); }
    catch { return {}; }
  }
  function saveMappingToStorage(map) {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(map || {})); }
    catch {}
  }
  function clearMappingFromStorage() {
    try { localStorage.removeItem(STORAGE_KEY); }
    catch {}
  }

  // ---- small utilities ------------------------------------------------------

  function uniqueSamples(rows, col, limit = 5) {
    if (!col) return [];
    const seen = new Set();
    const out = [];
    for (const r of (rows || [])) {
      const v = (r?.[col] ?? '').toString().trim();
      if (!v) continue;
      if (!seen.has(v)) {
        seen.add(v);
        out.push(v);
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  // Build a “best guess” mapping from headers using synonyms (SYN)
  // headers: string[]  | SYN: { address1?: string[], city?: string[], ... }
  function suggestedAutoMapping(headers = [], SYN = {}) {
    const low = headers.map(h => String(h || '').toLowerCase());
    const pick = (keys) => {
      for (const k of (keys || [])) {
        const i = low.indexOf(k);
        if (i !== -1) return headers[i];
      }
      return '';
    };
    return {
      address1:  pick(SYN.address1),
      address2:  pick(SYN.address2),
      city:      pick(SYN.city),
      state:     pick(SYN.state),
      zip:       pick(SYN.zip),
      mail_date: pick(SYN.mail_date),
      crm_date:  pick(SYN.crm_date),
      amount:    pick(SYN.amount),
    };
  }

  // ---- DOM helpers ----------------------------------------------------------

  function qs(sel, root = document) { return root.querySelector(sel); }
  function qsa(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }

  // Always put overlay on top and clickable
  function showOverlay(visible) {
    const overlay = qs('#mtMapperOverlay');
    if (!overlay) return;

    overlay.style.position = 'fixed';
    overlay.style.inset = '0';
    overlay.style.zIndex = '2147483600';
    overlay.style.pointerEvents = visible ? 'auto' : 'none';

    overlay.style.display = visible ? 'flex' : 'none';
    overlay.setAttribute('aria-hidden', visible ? 'false' : 'true');
  }

  // ---- main public API ------------------------------------------------------

  // payload shape:
  // {
  //   mailHeaders: string[],
  //   mailRows: object[],
  //   crmHeaders:  string[],
  //   crmRows:     object[],
  //   auto?: { mail?: {..}, crm?: {..} },       // optional caller auto-picks
  //   SYN?:  { address1?:[], city?:[], ... },   // optional synonyms hint
  //   required?: { mail?: string[], crm?: string[] } // optional UI badges
  // }
  //
  // returns Promise<map|null>
  // map shape: { mail: {address1?:string, ...}, crm: {...} }
  //
  window.openColumnMapper = function openColumnMapper(payload = {}) {
    return new Promise(resolve => {
      const overlay = qs('#mtMapperOverlay');
      const mailWrap = qs('#mtMapMail .rows');
      const crmWrap  = qs('#mtMapCRM .rows');
      const helpEl   = qs('#mtMapperHelp');

      // Option A: ensure overlay is a direct child of <body> (escape stacking contexts)
      if (overlay && overlay.parentElement !== document.body) {
        document.body.appendChild(overlay);
      }

      if (!overlay || !mailWrap || !crmWrap) {
        console.error('[mapper-modal] Missing required DOM nodes. Check components in dashboard.html.');
        return resolve(null);
      }

      // Clear old content
      mailWrap.innerHTML = '';
      crmWrap.innerHTML  = '';
      if (helpEl) helpEl.textContent = "We couldn't automatically detect some columns. Map the fields below and we'll remember them for next time.";

      // required badge config (UI only; actual enforcement should run elsewhere)
      const reqMail = payload?.required?.mail || ['address1'];
      const reqCRM  = payload?.required?.crm  || ['address1'];

      // Read existing storage + caller-provided auto + SYN hints
      const storage = readMappingFromStorage();
      const SYNmail = payload.SYN || {};        // You can pass a single SYN object for both sides
      const SYNcrm  = payload.SYN || {};
      const autoMail = payload.auto?.mail || suggestedAutoMapping(payload.mailHeaders || [], SYNmail);
      const autoCRM  = payload.auto?.crm  || suggestedAutoMapping(payload.crmHeaders  || [], SYNcrm);

      // Keep select refs so we can collect mapping at the end
      const state = { map: { mail: {}, crm: {} } };

      function makeRow(side, key, label) {
        const headers = side === 'mail' ? (payload.mailHeaders || []) : (payload.crmHeaders || []);
        const rows    = side === 'mail' ? (payload.mailRows || [])    : (payload.crmRows  || []);

        // Row wrapper
        const wrap = document.createElement('div');
        wrap.className = 'row';

        // Label + optional "required" badge
        const lab = document.createElement('label');
        lab.textContent = label;
        const isRequired = (side === 'mail' ? reqMail : reqCRM).includes(key);
        if (isRequired) {
          const badge = document.createElement('span');
          badge.className = 'req';
          badge.textContent = 'required';
          lab.appendChild(badge);
        }

        // Select
        const sel = document.createElement('select');
        sel.innerHTML = `<option value="">— Select column —</option>` +
          headers.map(h => `<option value="${h}">${h}</option>`).join('');

        // Preselect: storage → caller auto → (fallback) local suggestion
        const stored = storage?.[side]?.[key] || '';
        const autoPick = (side === 'mail' ? autoMail[key] : autoCRM[key]) || '';
        const pre = stored || autoPick || '';
        if (pre && headers.includes(pre)) sel.value = pre;

        // Samples
        const samples = document.createElement('div');
        samples.className = 'samples';
        function updateSamples() {
          const col = sel.value;
          if (!col) { samples.innerHTML = ''; return; }
          const vals = uniqueSamples(rows, col, 4);
          samples.innerHTML = vals.length ? ('e.g. ' + vals.join(' · ')) : '';
        }
        sel.addEventListener('change', updateSamples);
        updateSamples();

        // Mount
        wrap.appendChild(lab);
        wrap.appendChild(sel);
        wrap.appendChild(samples);
        (side === 'mail' ? mailWrap : crmWrap).appendChild(wrap);

        // Track for collection
        state.map[side][key] = sel;
      }

      // Build both sides (matches the original field set + labels)
      makeRow('mail', 'address1',  'Address 1');
      makeRow('mail', 'address2',  'Address 2 (optional)');
      makeRow('mail', 'city',      'City (optional)');
      makeRow('mail', 'state',     'State (optional)');
      makeRow('mail', 'zip',       'ZIP (optional)');
      makeRow('mail', 'mail_date', 'Mail Date (optional)');

      makeRow('crm',  'address1',  'Address 1');
      makeRow('crm',  'address2',  'Address 2 (optional)');
      makeRow('crm',  'city',      'City (optional)');
      makeRow('crm',  'state',     'State (optional)');
      makeRow('crm',  'zip',       'ZIP (optional)');
      makeRow('crm',  'crm_date',  'CRM Date (optional)');
      makeRow('crm',  'amount',    'Amount (optional)');

      function collect() {
        const map = { mail: {}, crm: {} };
        for (const side of ['mail', 'crm']) {
          for (const key of Object.keys(state.map[side])) {
            const sel = state.map[side][key];
            if (sel && sel.value) map[side][key] = sel.value;
          }
        }
        return map;
      }

      // Optional feedback message when required are missing (visual only)
      function setMissingMessage(missLabels) {
        if (!helpEl) return;
        if (!missLabels?.length) {
          helpEl.textContent = "We couldn't automatically detect some columns. Map the fields below and we'll remember them for next time.";
          return;
        }
        helpEl.innerHTML = 'Missing required mappings: <span class="missing">' + missLabels.join(', ') + '</span>';
      }

      // Wire buttons
      const btnClose = qs('#mtMapperClose');
      const btnCancel = qs('#mtMapperCancel');
      const btnAuto = qs('#mtMapperAuto');
      const btnApply = qs('#mtMapperApply');

      function closeAndResolve(val) {
        showOverlay(false);
        resolve(val);
      }

      if (btnClose)  btnClose.onclick  = () => closeAndResolve(null);
      if (btnCancel) btnCancel.onclick = () => closeAndResolve(null);

      if (btnAuto) {
        btnAuto.onclick = () => {
          // Recompute suggestions each time in case headers changed upstream
          const mailAuto = payload.auto?.mail || suggestedAutoMapping(payload.mailHeaders || [], SYNmail);
          const crmAuto  = payload.auto?.crm  || suggestedAutoMapping(payload.crmHeaders  || [], SYNcrm);
          const ap = { mail: mailAuto, crm: crmAuto };
          for (const side of ['mail', 'crm']) {
            for (const [k, v] of Object.entries(ap[side])) {
              const sel = state.map[side][k];
              if (sel && v && Array.isArray(side === 'mail' ? payload.mailHeaders : payload.crmHeaders)) {
                const headers = side === 'mail' ? payload.mailHeaders : payload.crmHeaders;
                if (headers.includes(v)) {
                  sel.value = v;
                  sel.dispatchEvent(new Event('change'));
                }
              }
            }
          }
        };
      }

      if (btnApply) {
        btnApply.onclick = () => {
          const map = collect();

          // Visual-only validation for required badges (real enforcement stays outside)
          const missing = [];
          if (reqMail.includes('address1') && !map.mail?.address1) missing.push('Mail: Address 1');
          if (reqCRM.includes('address1')  && !map.crm?.address1)  missing.push('CRM: Address 1');

          if (missing.length) {
            setMissingMessage(missing);
            return;
          }

          saveMappingToStorage(map);
          closeAndResolve(map);
        };
      }

      // Optional "Clear saved mapping" control if present in DOM
      const possibleClearButtons = qsa('#mtMapperModal .mapper-clear, #mtMapperModal button, #mtMapperModal .btn');
      const clearBtn = possibleClearButtons.find(b => (b.textContent || '').trim().toLowerCase() === 'clear saved mapping');
      if (clearBtn && !clearBtn.__mtClearWired) {
        clearBtn.__mtClearWired = true;
        clearBtn.addEventListener('click', (e) => {
          e.preventDefault();
          clearMappingFromStorage();
          // Reset all selects
          for (const side of ['mail', 'crm']) {
            for (const key of Object.keys(state.map[side])) {
              const sel = state.map[side][key];
              if (sel) { sel.value = ''; sel.dispatchEvent(new Event('change')); }
            }
          }
          setMissingMessage([]);
        });
      }

      // Hide loader if present so it can't intercept clicks
      const loader = document.getElementById('mailtrace-modal');
      if (loader) loader.style.display = 'none';

      // Finally, show the overlay
      showOverlay(true);
    });
  };

  // Optional: expose minimal storage API for other modules (e.g., a “Reset mapping” CTA)
  window.mapperStorage = {
    read:  readMappingFromStorage,
    save:  saveMappingToStorage,
    clear: clearMappingFromStorage,
    key:   STORAGE_KEY
  };

  // --- Edit Mapping button bootstrap (keeps your old UX) ---------------------

  function ensureEditMappingButton() {
    const runBtn = document.getElementById('runBtn');
    if (!runBtn) return;

    let editBtn = document.getElementById('editMappingBtn');
    if (!editBtn) {
      editBtn = document.createElement('button');
      editBtn.id = 'editMappingBtn';
      editBtn.type = 'button';
      editBtn.className = 'btn';
      editBtn.textContent = 'Edit mapping';
      runBtn.insertAdjacentElement('afterend', document.createElement('div')).appendChild(editBtn);
      editBtn.parentElement.style.marginTop = '10px';
    }

    // Always enabled; modal handles empty headers gracefully
    editBtn.disabled = false;

    editBtn.onclick = async () => {
      try {
        let payload = null;

        if (typeof window.getLatestMappingPayload === 'function') {
          payload = await window.getLatestMappingPayload();
        }
        if (!payload) {
          const SYN = window.MT_SYNONYMS || { address1:[], address2:[], city:[], state:[], zip:[], mail_date:[], crm_date:[], amount:[] };
          payload = {
            mailHeaders: window.MT_LAST_MAIL_HEADERS || [],
            crmHeaders:  window.MT_LAST_CRM_HEADERS  || [],
            mailRows:    window.MT_LAST_MAIL_ROWS    || [],
            crmRows:     window.MT_LAST_CRM_ROWS     || [],
            SYN,
            auto: {}
          };
        }

        payload.mailHeaders ||= [];
        payload.crmHeaders  ||= [];
        payload.mailRows    ||= [];
        payload.crmRows     ||= [];
        payload.SYN         ||= { address1:[], address2:[], city:[], state:[], zip:[], mail_date:[], crm_date:[], amount:[] };
        payload.auto        ||= {};

        const result = await window.openColumnMapper(payload);
        if (result && typeof window.onMappingChanged === 'function') {
          window.onMappingChanged(result);
        }
      } catch (err) {
        console.error('Edit mapping failed:', err);
      }
    };
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', ensureEditMappingButton, { once: true });
  } else {
    ensureEditMappingButton();
  }
  new MutationObserver(() => ensureEditMappingButton()).observe(document.body, { childList: true, subtree: true });

})();