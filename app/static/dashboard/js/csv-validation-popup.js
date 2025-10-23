// modal-logic.js
(function () {
  var BACKDROP_ID = 'mt-modal-backdrop';
  var MODAL_ID = 'mt-modal';

  function $(sel, root) { return (root || document).querySelector(sel); }
  function $all(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function countPickedFiles() {
    var inputs = $all('input[type="file"]');
    var total = 0;
    inputs.forEach(function (i) {
      try { total += (i.files && i.files.length) ? i.files.length : 0; } catch (e) { }
    });
    return total;
  }

  function openModal() {
    var bd = $('#' + BACKDROP_ID), m = $('#' + MODAL_ID);
    if (!bd || !m) return;
    bd.style.display = 'flex';
    requestAnimationFrame(function () {
      bd.classList.add('show');
      m.classList.add('open');
      try { m.focus(); } catch (e) { }
    });
  }

  function closeModal() {
    var bd = $('#' + BACKDROP_ID), m = $('#' + MODAL_ID);
    if (!bd || !m) return;
    m.classList.remove('open');
    bd.classList.remove('show');
    setTimeout(function () { bd.style.display = 'none'; }, 120);
  }

  function guardRun(ev) {
    if (countPickedFiles() < 2) {
      if (ev) { ev.preventDefault(); ev.stopPropagation(); }
      openModal();
      return false;
    }
    return true;
  }

  function attachToRunButtons(root) {
    var RE_RUN = /run\s*matching/i;
    var btns = $all('button, a, [role="button"], input[type="button"], input[type="submit"]', root);
    btns.forEach(function (b) {
      var txt = (b.textContent || b.value || b.getAttribute('aria-label') || b.getAttribute('title') || '').replace(/\s+/g, ' ').trim();
      if (RE_RUN.test(txt)) {
        b.addEventListener('click', function (ev) { guardRun(ev); }, true);
      }
    });
  }

  function attachToForms() {
    $all('form').forEach(function (f) {
      f.addEventListener('submit', function (ev) {
        guardRun(ev);
      }, true);
    });
  }

  function watchErrors() {
    var mo = new MutationObserver(function (muts) {
      muts.forEach(function (m) {
        (m.addedNodes || []).forEach(function (n) {
          if (!n || n.nodeType !== 1) return;
          var txt = (n.textContent || '').trim();
          if (/please\s*pick\s*both\s*csv\s*files\s*first/i.test(txt)) {
            n.style.display = 'none';
            openModal();
          }
        });
      });
    });
    try { mo.observe(document.body, { childList: true, subtree: true }); } catch (e) { }
  }

  function boot() {
    attachToRunButtons(document);
    attachToForms();
    watchErrors();
    var ok = $('#mt-modal-ok'), cancel = $('#mt-modal-cancel'), bd = $('#' + BACKDROP_ID);
    if (ok) ok.addEventListener('click', closeModal);
    if (cancel) cancel.addEventListener('click', closeModal);
    if (bd) bd.addEventListener('click', function (e) { if (e.target === bd) closeModal(); });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeModal(); });

    var mo2 = new MutationObserver(function (muts) {
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