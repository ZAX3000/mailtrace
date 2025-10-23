(function () {
  const BACKDROP_ID = 'mt-modal-backdrop';
  const MODAL_ID = 'mt-modal';
  
  // Helper function to select an element
  function $(sel, root) { return (root || document).querySelector(sel); }

  // Helper function to select all matching elements
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  // Count the number of files selected across all file inputs
  function countPickedFiles() {
    const inputs = $all('input[type="file"]');
    let total = 0;
    inputs.forEach(function (i) {
      try { total += (i.files && i.files.length) ? i.files.length : 0; } catch (e) { }
    });
    return total;
  }

  // Open the modal dialog
  function openModal() {
    const bd = $('#' + BACKDROP_ID), m = $('#' + MODAL_ID);
    if (!bd || !m) return;
    bd.style.display = 'flex';
    // force reflow then animate open
    requestAnimationFrame(function () {
      bd.classList.add('show');
      m.classList.add('open');
      try { m.focus(); } catch (e) { }
    });
  }

  // Close the modal dialog
  function closeModal() {
    const bd = $('#' + BACKDROP_ID), m = $('#' + MODAL_ID);
    if (!bd || !m) return;
    m.classList.remove('open');
    bd.classList.remove('show');
    setTimeout(function () { bd.style.display = 'none'; }, 120);
  }

  // Prevent the "Run matching" action if fewer than 2 files are selected
  function guardRun(ev) {
    if (countPickedFiles() < 2) {
      if (ev) { ev.preventDefault(); ev.stopPropagation(); }
      openModal(); // Show the modal
      return false;
    }
    return true;
  }

  // Attach listeners to "Run Matching" buttons to check file selection
  function attachToRunButtons(root) {
    const RE_RUN = /run\s*matching/i;
    const btns = $all('button, a, [role="button"], input[type="button"], input[type="submit"]', root);
    btns.forEach(function (b) {
      const txt = (b.textContent || b.value || b.getAttribute('aria-label') || b.getAttribute('title') || '').replace(/\s+/g, ' ').trim();
      if (RE_RUN.test(txt)) {
        // Capture phase handler to block before other listeners
        b.addEventListener('click', function (ev) { guardRun(ev); }, true);
      }
    });
  }

  // Intercept form submit on containing forms as a fallback
  function attachToForms() {
    $all('form').forEach(function (f) {
      f.addEventListener('submit', function (ev) {
        guardRun(ev);
      }, true);
    });
  }

  // Hide the top red banner if it shows up, and display the modal instead
  function watchErrors() {
    const mo = new MutationObserver(function (muts) {
      muts.forEach(function (m) {
        (m.addedNodes || []).forEach(function (n) {
          if (!n || n.nodeType !== 1) return;
          const txt = (n.textContent || '').trim();
          if (/please\s*pick\s*both\s*csv\s*files\s*first/i.test(txt)) {
            // Hide the error and show the modal instead
            n.style.display = 'none';
            openModal();
          }
        });
      });
    });
    try { mo.observe(document.body, { childList: true, subtree: true }); } catch (e) { }
  }

  // Initialize the modal handler
  function boot() {
    attachToRunButtons(document);
    attachToForms();
    watchErrors();
    // Close handlers
    const ok = $('#mt-modal-ok'), cancel = $('#mt-modal-cancel'), bd = $('#' + BACKDROP_ID);
    if (ok) ok.addEventListener('click', closeModal);
    if (cancel) cancel.addEventListener('click', closeModal);
    if (bd) bd.addEventListener('click', function (e) { if (e.target === bd) closeModal(); });
    // Keyboard escape to close modal
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeModal(); });
    // Observe dynamic buttons (in case the UI renders later)
    const mo2 = new MutationObserver(function (muts) {
      muts.forEach(function (m) {
        (m.addedNodes || []).forEach(function (n) {
          if (n && n.nodeType === 1) attachToRunButtons(n);
        });
      });
    });
    try { mo2.observe(document.body, { childList: true, subtree: true }); } catch (e) { }
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(boot, 60);
  } else {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 60); });
  }
})();