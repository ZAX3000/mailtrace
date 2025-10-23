// dash-mapper-skin-apply.js
// Applies the dashboard skin to any mapping-tool UIs that might render with a dark inline theme.
// Safe to run multiple times; throttled and guarded.

(() => {
  'use strict';

  // global guard so we don’t install multiple observers if the file is loaded twice
  if (window.__dashMapperSkinInit) return;
  window.__dashMapperSkinInit = true;

  const SELECTOR = '.mapping-tool, #mappingTool, .map-tool, .column-mapper, .mapping-panel, [data-role="mapper"], [data-view="mapping"]';
  const getVar = (name, fallback) => (getComputedStyle(document.body).getPropertyValue(name).trim() || fallback);

  function looksLikeDarkBlue(rgb) {
    const m = rgb && rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!m) return false;
    const r = +m[1], g = +m[2], b = +m[3];
    const lum = 0.2126 * r + 0.7152 * g + 0.0722 * b; // rough perceptual luminance
    return lum < 120 && b > r && b > g; // “dark-ish & bluish”
  }

  function applyDashSkin() {
    const bgPanel  = getVar('--dash-panel', '#E6F1FF');
    const fgText   = getVar('--dash-text',  '#0b2942');
    document.querySelectorAll(SELECTOR).forEach(el => {
      const bg = getComputedStyle(el).backgroundColor;
      if (looksLikeDarkBlue(bg)) {
        el.style.background = bgPanel;
        el.style.color = fgText;
      }
      // Ensure our CSS skin rules target it
      el.classList.add('mapping-tool');
    });
  }

  // Throttle MutationObserver callbacks so we don’t spam applyDashSkin on busy DOMs
  let rafId = null;
  function scheduleApply() {
    if (rafId != null) return;
    rafId = requestAnimationFrame(() => { rafId = null; applyDashSkin(); });
  }

  // Initial run
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', applyDashSkin, { once: true });
  } else {
    applyDashSkin();
  }

  // Re-apply when the app swaps routes or injects the mapper
  const mo = new MutationObserver(scheduleApply);
  mo.observe(document.body || document.documentElement, { childList: true, subtree: true });

  // Expose a manual hook for one-off reflows (optional)
  window.reapplyDashMapperSkin = applyDashSkin;
})();