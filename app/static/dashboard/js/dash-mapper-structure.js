// /static/dashboard/js/dash-mapper-structure.js
(() => {
  'use strict';
  if (window.__dashMapperStructureInit) return;
  window.__dashMapperStructureInit = true;

  const DIALOG_SEL = [
    '.dash-mapper-modal', // if already tagged
    '#mtMapperModal',
    '.modal', '.dialog', '.popup', '.modal-container',
    '.ReactModal__Content', '.ant-modal', '.chakra-modal__content',
    '[role="dialog"]'
  ].join(',');

  const normText = (el) => (el?.textContent || '').replace(/\s+/g,' ').trim().toLowerCase();

  function luminance(rgb) {
    const m = rgb && rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!m) return 255;
    const r = +m[1], g = +m[2], b = +m[3];
    return 0.2126*r + 0.7152*g + 0.0722*b;
  }

  function findPanelBg() {
    const anchor = document.querySelector('canvas')?.parentElement || document.body;
    let el = anchor;
    while (el) {
      const bg = getComputedStyle(el).backgroundColor;
      if (bg && bg !== 'transparent' && luminance(bg) > 180) return bg;
      el = el.parentElement;
    }
    const bodyBg = getComputedStyle(document.body).backgroundColor;
    return (bodyBg && bodyBg !== 'transparent') ? bodyBg : '#f5f9ff';
  }

  // Visual polish: remove inline “hairline/inset” shadows and inline borders UI libs inject
  function stripHairlines(modal) {
    modal.querySelectorAll('*').forEach(n => {
      const bs = n.style.boxShadow || '';
      if (bs && (bs.includes('inset 0 0 0 1px') || /inset\s+0\s+0\s+0\s+1px/i.test(bs))) {
        const parts = bs.split(',').map(s => s.trim()).filter(s => !s.startsWith('inset'));
        n.style.boxShadow = parts.join(', ');
      }
      if (n.style.border)       n.style.border       = '0';
      if (n.style.borderTop)    n.style.borderTop    = '0';
      if (n.style.borderRight)  n.style.borderRight  = '0';
      if (n.style.borderBottom) n.style.borderBottom = '0';
      if (n.style.borderLeft)   n.style.borderLeft   = '0';
    });
  }

  // Make the main title plain & consistent (visual only)
  function markTitles(modal){
    const nodes = modal.querySelectorAll('h1,h2,.title,.section-title');
    nodes.forEach(n => {
      const t = normText(n);
      if (t === 'map your columns'){
        n.classList.remove('section-title','title','pill');
        n.classList.add('mapper-main-title','mapper-title-plain');
        n.style.setProperty('background','transparent','important');
        n.style.setProperty('box-shadow','none','important');
        n.style.setProperty('border','0','important');
        n.style.setProperty('padding','0','important');
      }
    });
  }

  // Build a clean two-card grid for Mail/CRM sections (idempotent)
  function ensureCardsGrid(modal) {
    if (modal.querySelector('.mapper-grid')) return; // already structured

    const heads = Array.from(modal.querySelectorAll('h1,h2,h3,.title,.section-title'));
    const mailHead = heads.find(h => normText(h) === 'mail csv');
    const crmHead  = heads.find(h => normText(h) === 'crm csv');
    if (!mailHead || !crmHead) return;

    // Don’t restructure if heads are already inside .mapper-card
    if (mailHead.closest('.mapper-card') && crmHead.closest('.mapper-card')) return;

    // Find common ancestor that contains both sections
    let anc = mailHead.parentElement;
    while (anc && !anc.contains(crmHead)) anc = anc.parentElement;
    if (!anc) return;

    const grid = document.createElement('div');
    grid.className = 'mapper-grid';

    function makeCard(head) {
      const card = document.createElement('div');
      card.className = 'mapper-card';
      card.appendChild(head); // move the heading
      let n = head.nextSibling;
      while (n) {
        const next = n.nextSibling;
        if (n.nodeType === 1 && (normText(n) === 'mail csv' || normText(n) === 'crm csv')) break;
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
  }

  function looksLikeMapper(el){
    if (!el) return false;
    if (el.id === 'mtMapperModal') return true;
    const txt = normText(el);
    // Heuristics specific to your mapper copy
    const hasTitle = txt.includes('map your columns');
    const hintA = txt.includes("we couldn't automatically detect some columns");
    const hintB = (txt.includes('column') && txt.includes('map'));
    return hasTitle && (hintA || hintB);
  }

  function scan() {
    document.querySelectorAll(DIALOG_SEL).forEach(el => {
      if (!looksLikeMapper(el)) return;

      el.classList.add('dash-mapper-modal');
      el.style.setProperty('--mapper-bg', findPanelBg());

      markTitles(el);
      ensureCardsGrid(el);
      stripHairlines(el);
    });
  }

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

  // optional manual hook
  window.reapplyDashMapperStructure = scan;
})();