// cta-3d-apply.js
(function () {
  var SELECTOR = 'button, a, [role="button"], input[type="button"], input[type="submit"], .btn, .button';
  var RE_RUN = /run\s*matching/i;
  var RE_EDIT = /edit\s*mapping/i;
  var RE_KPIS = /(show|hide)\s*.*advanced\s*kpi[\u2019']?s?/i;
  var rafScanEnd = 0;

  function norm(s) {
    return (s || '')
      .replace(/[\u00A0]/g, ' ')        // nbsp -> space
      .replace(/\s+/g, ' ')             // collapse whitespace
      .replace(/[»›<>\(\)\[\]]/g, '')   // strip adornments
      .trim();
  }

  function labelOf(el) {
    return norm(
      (el.textContent || '') ||
      (el.value || '') ||
      el.getAttribute('aria-label') ||
      el.getAttribute('title')
    );
  }

  function makePrimary(el) {
    el.classList.add('mt-cta-3d');
    el.classList.remove('mt-cta-secondary');
    el.style.visibility = 'visible';
    el.style.opacity = '1';
    el.dataset.mtCta3d = '1';
  }

  function makeSecondary(el) {
    el.classList.add('mt-cta-secondary');
    el.classList.remove('mt-cta-3d');
    el.style.visibility = 'visible';
    el.style.opacity = '1';
    el.dataset.mtCta3d = '1';
  }

  function maybeTag(el) {
    if (!el || el.dataset.mtCta3d === '1') return false;
    var t = labelOf(el);
    if (!t) return false;
    if (RE_RUN.test(t)) { makePrimary(el); return true; }
    if (RE_EDIT.test(t)) { makeSecondary(el); return true; }
    if (RE_KPIS.test(t)) { makePrimary(el); return true; } // Show/Hide Advanced KPIs
    return false;
  }

  function sweep(root) {
    var nodes = (root || document).querySelectorAll ? (root || document).querySelectorAll(SELECTOR) : [];
    nodes = Array.prototype.slice.call(nodes);
    nodes.forEach(maybeTag);
  }

  var mo = new MutationObserver(function (muts) {
    for (var i = 0; i < muts.length; i++) {
      var m = muts[i];
      for (var j = 0; j < (m.addedNodes || []).length; j++) {
        var n = muts[i].addedNodes[j];
        if (n.nodeType !== 1) continue;
        if (!maybeTag(n)) sweep(n);
      }
    }
  });

  function startObserver(doc) {
    try { mo.observe(doc.body, { childList: true, subtree: true }); } catch (e) { }
  }

  var oldAttach = Element.prototype.attachShadow;
  if (oldAttach) {
    Element.prototype.attachShadow = function (init) {
      var sr = oldAttach.call(this, init);
      try {
        var mo2 = new MutationObserver(function (muts) {
          for (var i = 0; i < muts.length; i++) {
            for (var j = 0; j < (muts[i].addedNodes || []).length; j++) {
              var n = muts[i].addedNodes[j];
              if (n.nodeType !== 1) continue;
              if (!maybeTag(n)) {
                try { sweep(n); } catch (e) { }
              }
            }
          }
        });
        mo2.observe(sr, { childList: true, subtree: true });
        sweep(sr);
      } catch (e) { }
      return sr;
    };
  }

  function hookIframes() {
    var iframes = Array.prototype.slice.call(document.querySelectorAll('iframe'));
    iframes.forEach(function (fr) {
      try {
        var idoc = fr.contentDocument;
        if (!idoc || idoc.__mtCTAHooked) return;
        idoc.__mtCTAHooked = true;
        sweep(idoc);
        var moF = new MutationObserver(function (muts) {
          for (var i = 0; i < muts.length; i++) {
            for (var j = 0; j < (muts[i].addedNodes || []).length; j++) {
              var n = muts[i].addedNodes[j];
              if (n.nodeType !== 1) continue;
              if (!maybeTag(n)) sweep(n);
            }
          }
        });
        moF.observe(idoc, { childList: true, subtree: true });
      } catch (e) { /* cross-origin -> ignore */ }
    });
  }

  function hookRunMatching() {
    var nodes = Array.prototype.slice.call(document.querySelectorAll(SELECTOR));
    nodes.forEach(function (el) {
      var t = labelOf(el);
      if (!t) return;
      if (RE_RUN.test(t)) {
        el.addEventListener('click', function () {
          var end = Date.now() + 12000; // 12s aggressive scan
          function pump() {
            sweep(document);
            hookIframes();
            if (Date.now() < end) requestAnimationFrame(pump);
          }
          requestAnimationFrame(pump);
        }, { once: false });
      }
    });
  }

  function boot() {
    sweep(document);
    startObserver(document);
    hookIframes();
    hookRunMatching();

    var count = 0;
    var t = setInterval(function () {
      sweep(document);
      hookIframes();
      if (++count > 60) clearInterval(t);
    }, 2000);
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(boot, 60);
  } else {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 60); });
  }
})();