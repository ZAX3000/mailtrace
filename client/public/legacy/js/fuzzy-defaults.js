// /static/dashboard/js/fuzzy-defaults.js
(() => {
  function applyFuzzyDefaults() {
    try {
      const toggle = document.getElementById('fuzzyToggle');
      const mode = document.getElementById('fuzzyMode');

      if (mode) mode.value = 'loose';       // keep Loose selected
      if (toggle) toggle.checked = true;    // keep fuzzy ON

      // Hide the control row that contains both controls
      if (toggle && toggle.parentElement && toggle.parentElement.parentElement) {
        const row = toggle.parentElement.parentElement; // the <div ...>
        row.style.display = 'none';

        // Hide the trailing "Stays in-browser" line, if present
        const sib = row.nextElementSibling;
        if (sib && sib.classList && sib.classList.contains('muted')) {
          sib.style.display = 'none';
        }
      }
    } catch (_) { /* noop */ }
  }

  // Run after DOM is ready (works with/without `defer`)
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', applyFuzzyDefaults);
  } else {
    applyFuzzyDefaults();
  }
})();