// /static/dashboard/js/dash-mapper-modal-apply.js
// One authoritative mapper-modal tagger/styler hook.
// - Tags modals/backdrops for CSS
// - Syncs --mapper-* vars
// - (optional) forces light sections for Mail/CRM headings
// - (optional) builds mapper-grid + mapper-card structure
// Safe to run multiple times; throttled on mutations.

(() => {
  'use strict';
  if (window.__dashMapperModalInit) return;
  window.__dashMapperModalInit = true;

  // ---------- Config ----------
  const CFG = Object.assign(
    {
      borderless: true,
      forceLightSections: true,
      structure: false, // off by default (DOM moves can upset frameworks)
    },
    window.MT_MAPPER_STYLE || {}
  );

  // ---------- Selectors ----------
  const CANDIDATE_SEL = [
    '#mtMapperModal',
    '.modal', '.dialog', '.popup', '.modal-container',
    '.ReactModal__Content', '.ant-modal', '.chakra-modal__content',
    '[role="dialog"]'
  ].join(',');

  const BACKDROP_SEL = [
    '#mtMapperOverlay',
    '.modal-backdrop', '.backdrop',
    '.ant-modal-mask', '.chakra-modal__overlay'
  ].join(',');

  // ---------- Utils ----------
  const cssVar = (name, fallback) =>
    (getComputedStyle(document.body).getPropertyValue(name).trim() || fallback);

  function textContains(el, ...needles) {
    const txt = (el.textContent || '').toLowerCase();
    return needles.every(n => txt.includes(n));
  }

  function looksLikeMapperModal(el) {
    if (!el) return false;
    if (el.id === 'mtMapperModal') return true;
    // phrases unique to the mapping flow
    const a = textContains(el, 'map your columns');
    const b = textContains(el, "we couldn't automatically detect some columns");
    const c = textContains(el, 'column') && textContains(el, 'map');
    return (a && b) || (a && c);
  }

  function tagBackdrop(nearEl) {
    const root = (nearEl && (nearEl.parentElement || nearEl.ownerDocument || document)) || document;
    let found = false;
    root.querySelectorAll(BACKDROP_SEL).forEach(bg => {
      if (!bg.classList.contains('dash-mapper-backdrop')) {
        bg.classList.add('dash-mapper-backdrop');
        found = true;
      }
    });
    if (!found) {
      document.querySelectorAll(BACKDROP_SEL).forEach(bg => {
        bg.classList.add('dash-mapper-backdrop');
      });
    }
  }

  function syncMapperVars(el) {
    if (!el) return;
    // Use dashboard tokens when present
    const mapperBg   = cssVar('--dash-panel', cssVar('--dash-bg-light', '#E6F1FF'));
    const mapperCard = '#ffffff';
    const dashBlue   = cssVar('--brand-blue', cssVar('--dash-blue', '#173b64'));
    const ring       = `color-mix(in oklab, ${dashBlue} 22%, transparent)`;

    el.style.setProperty('--mapper-bg', mapperBg);
    el.style.setProperty('--mapper-card', mapperCard);
    el.style.setProperty('--mapper-ring', ring);
  }

  // ----- Lightening (from first inline script) -----
  function isDarkRGB(rgb) {
    if (!rgb || rgb === 'transparent') return false;
    const m = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!m) return false;
    const r = +m[1], g = +m[2], b = +m[3];
    const lum = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    return lum < 140;
  }
  function forceLight(el) {
    if (!el) return;
    const cs = getComputedStyle(el);
    if (isDarkRGB(cs.backgroundColor)) {
      const panel = cssVar('--dash-panel', '#E6F1FF');
      const text  = cssVar('--dash-text',  '#0b2942');
      el.style.background = panel;
      el.style.color = text;
      // prefer class-driven borders, but keep a safe minimum if needed:
      if (!el.classList.contains('mapper-card') && !el.classList.contains('panel')) {
        el.style.border = '1px solid var(--dash-border)';
      }
      if (!el.style.borderRadius) el.style.borderRadius = '12px';
      if (!el.style.padding)      el.style.padding      = '12px';
    }
  }

  function nearestSectionContainer(head) {
    // climb a few levels to find a block wrapper for a heading
    let el = head; let steps = 0;
    while (el && steps < 6) {
      el = el.parentElement; steps++;
      if (!el) break;
      const bg = getComputedStyle(el).backgroundColor;
      // pick the first non-transparent background container
      if (bg && bg !== 'transparent') return el;
    }
    return head.parentElement || head;
  }

  function polishSections(modal) {
    if (!CFG.forceLightSections || !modal) return;

    // locate headings named exactly “Mail CSV” / “CRM CSV”
    const heads = Array.from(modal.querySelectorAll('h1,h2,h3,h4,.title,.section-title'))
      .filter(h => {
        const t = (h.textContent || '').trim().toLowerCase();
        return t === 'mail csv' || t === 'crm csv';
      });

    heads.forEach(h => {
      const wrap = nearestSectionContainer(h);
      // Add a panel class (lets CSS handle surface)
      if (wrap && !wrap.classList.contains('panel')) {
        wrap.classList.add('panel');
      }
      // And force-light if it’s still dark
      forceLight(wrap);
      // Also gently fix direct children that remain dark
      Array.from(wrap.children).forEach(ch => forceLight(ch));
    });

    // Safety net: anything obviously dark inside the mapper modal becomes light
    modal.querySelectorAll('.card,.panel,.section,.group,fieldset,div').forEach(node => {
      if (node.closest('.dash-mapper-modal')) {
        const cs = getComputedStyle(node);
        if (isDarkRGB(cs.backgroundColor)) forceLight(node);
      }
    });
  }

  // ----- Optional structure (from third inline script; opt-in) -----
  function maybeStructure(modal) {
    if (!CFG.structure || !modal) return;
    if (modal.dataset.mtStructured === '1') return; // idempotent

    const headings = Array.from(modal.querySelectorAll('h1,h2,h3,.title,.section-title'));
    const mailHead = headings.find(h => (h.textContent || '').trim().toLowerCase() === 'mail csv');
    const crmHead  = headings.find(h => (h.textContent || '').trim().toLowerCase() === 'crm csv');

    if (!mailHead || !crmHead) return;

    // Find a common ancestor that contains both sections
    let anc = mailHead.parentElement;
    while (anc && !anc.contains(crmHead)) anc = anc.parentElement;
    if (!anc) return;

    // Avoid re-structuring if a grid already exists
    if (anc.querySelector('.mapper-grid')) {
      modal.dataset.mtStructured = '1';
      return;
    }

    // Build a grid and carve two cards by moving nodes (opt-in because it’s invasive)
    const grid = document.createElement('div');
    grid.className = 'mapper-grid';

    function makeCard(head) {
      const card = document.createElement('div');
      card.className = 'mapper-card';
      card.appendChild(head); // move heading
      let n = head.nextSibling;
      while (n) {
        const next = n.nextSibling;
        if (n.nodeType === 1) {
          const t = (n.textContent || '').trim().toLowerCase();
          if (t === 'mail csv' || t === 'crm csv') break;
        }
        card.appendChild(n);
        n = next;
      }
      return card;
    }

    const mailCard = makeCard(mailHead);
    const crmCard  = makeCard(crmHead);

    anc.appendChild(grid);
    grid.appendChild(mailCard);
    grid.appendChild(crmCard);

    // Light polish on borders/backgrounds inside cards
    [mailCard, crmCard].forEach(forceLight);

    modal.dataset.mtStructured = '1';
  }

  function tagModal(el) {
    if (!el) return;
    if (!(el.classList.contains('dash-mapper-modal') || looksLikeMapperModal(el))) return;

    el.classList.add('dash-mapper-modal');
    if (CFG.borderless) el.classList.add('borderless'); else el.classList.remove('borderless');

    syncMapperVars(el);
    tagBackdrop(el);
    polishSections(el); // from 1st & 2nd inline scripts
    maybeStructure(el); // from 3rd inline script (opt-in)
  }

  function scan() {
    document.querySelectorAll(CANDIDATE_SEL).forEach(tagModal);
  }

  // ---------- Boot & observe ----------
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', scan, { once: true });
  } else {
    scan();
  }

  let raf = null;
  const mo = new MutationObserver(() => {
    if (raf != null) return;
    raf = requestAnimationFrame(() => { raf = null; scan(); });
  });
  mo.observe(document.body || document.documentElement, { subtree: true, childList: true });

  // Optional manual hook
  window.reapplyDashMapperModalSkin = scan;
})();