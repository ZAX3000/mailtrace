// /static/dashboard/js/dash-mapper-actions-apply.js
(() => {
  'use strict';
  if (window.__dashMapperActionsInit) return;
  window.__dashMapperActionsInit = true;

  function panelBg() {
    const modal = document.querySelector('.dash-mapper-modal') || document.body;
    const cs = getComputedStyle(modal);
    const bg = cs.getPropertyValue('--mapper-bg') || cs.getPropertyValue('--dash-panel') || cs.backgroundColor;
    return (bg && bg.trim()) || '#E6F1FF';
  }

  function tagClearButton(modal) {
    const btns = Array.from(modal.querySelectorAll('button, a, .btn'));
    const text = el => (el.textContent || '').trim().toLowerCase();
    const clearBtn = btns.find(b => text(b) === 'clear saved mapping');
    if (clearBtn && !clearBtn.classList.contains('mapper-clear')) {
      clearBtn.classList.add('mapper-clear');
      // remove dark/danger classes that force dark bg
      ['danger', 'primary', 'bg-dark', 'bg-blue', 'bg-primary'].forEach(c => clearBtn.classList.remove(c));
      clearBtn.style.background = ''; // let CSS class win
      clearBtn.style.color = '';
    }
  }

  function commonAncestor(a, b) {
    if (!a || !b) return null;
    const seen = new Set();
    for (let n = a; n; n = n.parentElement) seen.add(n);
    for (let m = b; m; m = m.parentElement) if (seen.has(m)) return m;
    return null;
  }

  // ---- NEW: helpers that must stay in this closure ----
  function killStickyAlong(node, modal){
    // Walk up from node to (and including) the modal, clearing sticky/fixed inline styles
    while (node && node !== modal?.parentElement){
      node.classList.add('mapper-actions-fixed-killer');
      node.style.setProperty('position', 'static', 'important');
      node.style.setProperty('top', 'auto', 'important');
      node.style.setProperty('bottom', 'auto', 'important');
      node.style.setProperty('inset', 'auto', 'important');
      node.style.setProperty('z-index', 'auto', 'important');
      node.style.setProperty('box-shadow', 'none', 'important');
      node.style.setProperty('border', '0', 'important');
      node.style.setProperty('background', 'var(--mapper-bg, transparent)', 'important');
      node.style.setProperty('backdrop-filter', 'none', 'important');

      // remove common sticky/fixed class hints from UI libs
      ['sticky','fixed','affix','footer-fixed','ant-modal-footer','chakra-modal__footer']
        .forEach(c => node.classList.remove(c));

      if (node === modal) break;
      node = node.parentElement;
    }
  }

  function sweepStickyHeaderFooter(modal){
    const sel = [
      '.modal-header','.ant-modal-header','.chakra-modal__header','header','[class*="header"]',
      '.footer','.modal-footer','.ant-modal-footer','.chakra-modal__footer','.actions',
      '[class*="footer"]','[class*="actions"]','[class*="bar"]','[class*="bottom"]'
    ].join(',');
    modal.querySelectorAll(sel).forEach(n => {
      n.style.setProperty('position','static','important');
      n.style.setProperty('top','auto','important');
      n.style.setProperty('bottom','auto','important');
      n.style.setProperty('inset','auto','important');
      n.style.setProperty('z-index','auto','important');
      n.style.setProperty('box-shadow','none','important');
      n.style.setProperty('border','0','important');
      n.style.setProperty('background-attachment','scroll','important');
    });
  }
  // ---- end helpers ----

  function tagActionsBar() {
    const modal = document.querySelector('.dash-mapper-modal, [role="dialog"]');
    if (!modal) return;

    // Buttons we expect
    const btns = Array.from(modal.querySelectorAll('button, a, .btn'));
    const text = el => (el.textContent || '').trim().toLowerCase();
    const clearBtn  = btns.find(b => text(b) === 'clear saved mapping');
    const cancelBtn = btns.find(b => text(b) === 'cancel');
    const useBtn    = btns.find(b => text(b) === 'use mapping');

    tagClearButton(modal);

    // Determine the action bar container
    let container = null;
    if (clearBtn && cancelBtn) container = commonAncestor(clearBtn, cancelBtn);
    if (container && useBtn)    container = commonAncestor(container, useBtn) || container;

    if (!container) {
      const candidates = Array.from(modal.querySelectorAll(
        '.footer, .modal-footer, .actions, footer, [class*="footer"], [class*="actions"], [class*="bar"], [class*="bottom"]'
      ));
      container = candidates.reverse().find(el => el.offsetTop > modal.offsetHeight * 0.5) || candidates.pop();
    }
    if (!container) return;

    // Walk up to the modal and neutralize any dark backgrounds inline (rare lib overrides)
    const bgColor = panelBg();
    let node = container;
    while (node && node !== modal.parentElement) {
      const cs = getComputedStyle(node);
      const bg = cs.backgroundColor;
      if (bg && bg !== 'transparent') {
        const m = bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
        if (m) {
          const r = +m[1], g = +m[2], b = +m[3];
          const lum = 0.2126*r + 0.7152*g + 0.0722*b;
          if (lum < 180) {
            node.style.background = bgColor;
            node.style.backgroundImage = 'none';
            node.style.border = '0';
            node.style.boxShadow = 'none';
          }
        }
      }
      if (node === modal) break;
      node = node.parentElement;
    }

    // Ensure non-sticky behavior and tag for CSS kill switch
    killStickyAlong(container, modal);
    sweepStickyHeaderFooter(modal);

    container.classList.add('mapper-actions'); // CSS hook
  }

  function scan() { tagActionsBar(); }

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
  window.reapplyDashMapperActions = scan;
})();