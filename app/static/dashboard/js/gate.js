/*! Lsv1 deferral gate v2: show loader on first "Run Matching" click, auto-close on network idle */
(function () {
  if (window.__Lsv1GateV2) return;
  window.__Lsv1GateV2 = true;

  // Yield 2 frames so the page paints before we pop the loader
  const nextPaint = () => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));

  const Session = {
    active: false,
    fetches: 0,
    xhrs: 0,
    lastSettle: 0,
    startedAt: 0,
    quietTimer: null,
    restore: null,
  };

  function looksLikeRun(el) {
    if (!el) return false;
    try {
      const id = (el.id || "").toLowerCase();
      const cls = (el.className || "").toString().toLowerCase();
      const txt = (el.innerText || el.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
      if (id === "runbtn") return true;
      if (/(^|\s)run[-_\s]?matching(\s|$)/.test(id) || /(run[-_\s]?matching)/.test(cls)) return true;
      if (txt && txt.includes("run") && txt.includes("matching")) return true;
      if (el.hasAttribute && el.hasAttribute("data-run-matching")) return true;
    } catch (_) {}
    return false;
  }

  function startSession() {
    if (Session.active) return;
    Session.active = true;
    Session.fetches = 0;
    Session.xhrs = 0;
    Session.lastSettle = performance.now();
    Session.startedAt = performance.now();

    // Show loader fast
    try { MailTraceLoader.show({ progress: 0 }); } catch (_) {}
    try {
      const m = document.getElementById("mailtrace-modal");
      if (m) m.style.display = "block";
    } catch (_) {}

    // Instrument fetch + XHR
    const origFetch = window.fetch;
    const origOpen = XMLHttpRequest.prototype.open;
    const origSend = XMLHttpRequest.prototype.send;

    window.fetch = function () {
      if (Session.active) Session.fetches++;
      const p = origFetch.apply(this, arguments);
      if (p && typeof p.finally === "function") {
        p.finally(() => {
          if (Session.active) {
            Session.fetches = Math.max(0, Session.fetches - 1);
            Session.lastSettle = performance.now();
          }
        });
      } else {
        setTimeout(() => {
          if (Session.active) {
            Session.fetches = Math.max(0, Session.fetches - 1);
            Session.lastSettle = performance.now();
          }
        }, 0);
      }
      return p;
    };

    XMLHttpRequest.prototype.open = function () {
      this.__mtTracked = true;
      return origOpen.apply(this, arguments);
    };
    XMLHttpRequest.prototype.send = function () {
      if (Session.active && this.__mtTracked) Session.xhrs++;
      this.addEventListener("loadend", function () {
        if (Session.active && this.__mtTracked) {
          Session.xhrs = Math.max(0, Session.xhrs - 1);
          Session.lastSettle = performance.now();
        }
      });
      return origSend.apply(this, arguments);
    };

    Session.restore = function () {
      window.fetch = origFetch;
      XMLHttpRequest.prototype.open = origOpen;
      XMLHttpRequest.prototype.send = origSend;
    };

    // Quiet-close loop
    const QUIET_MS = 800;
    const MIN_OPEN_MS = 600;
    Session.quietTimer = setInterval(() => {
      if (!Session.active) return;
      const now = performance.now();
      const openLongEnough = now - Session.startedAt > MIN_OPEN_MS;
      const quiet = Session.fetches + Session.xhrs === 0 && now - Session.lastSettle > QUIET_MS;
      if (openLongEnough && quiet) stopSession(true);
    }, 200);
  }

  function stopSession(/* ok */) {
    if (!Session.active) return;
    Session.active = false;
    if (Session.quietTimer) { clearInterval(Session.quietTimer); Session.quietTimer = null; }
    try { Session.restore && Session.restore(); } catch (_) {}
    try { MailTraceLoader.setProgress(100); } catch (_) {}
    try { window.onMatchingDone && window.onMatchingDone(); } catch (_) {}
    try { MailTraceLoader.hide(); } catch (_) {}
  }

  // Public manual close hooks (optional)
  window.matchingDone = function () { stopSession(true); };
  window.addEventListener("matching:done", function () { stopSession(true); });

  // Backstop: hard close after 45s
  setInterval(function () {
    if (!Session.active) return;
    if (performance.now() - Session.startedAt > 45_000) stopSession(false);
  }, 5_000);

  // Deferral gate: intercept first "Run Matching" click, then re-fire it
  let gateOpen = false;
  document.addEventListener("click", function (ev) {
    if (gateOpen) return; // allow the synthetic click we dispatch
    const el = ev.target && ev.target.closest ? ev.target.closest('button, a, [role="button"], [data-run-matching], #runBtn') : null;
    if (!looksLikeRun(el)) return;

    ev.preventDefault();
    ev.stopImmediatePropagation();
    startSession();
    nextPaint().then(() => {
      gateOpen = true;
      try {
        el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window }));
      } finally {
        setTimeout(() => { gateOpen = false; }, 0);
      }
    });
  }, true);
})();