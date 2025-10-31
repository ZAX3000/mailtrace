/* /static/dashboard/js/progress_wiring.js
 *
 * Demo-safe wiring for MailTrace progress + controls.
 * - Disables ingest while a match job is running
 * - Handles 409s gracefully (no 500s / broken UI)
 * - Keeps backward compatibility with existing globals
 */

function mtHandleStartResponse(resp) {
  if (!resp || (resp.ok === false)) {
    return resp && resp.text ? resp.text().then(t => { throw new Error(t || ('HTTP ' + resp.status)); }) : Promise.reject(new Error('HTTP error'));
  }
  return (typeof resp.json === 'function' ? resp.json() : Promise.resolve(resp)).then(data => {
    if (data && data.error) { throw new Error(data.error); }
    return data;
  });
}

(function () {
  const g = (typeof window !== "undefined") ? window : globalThis;
  g.Mailtrace = g.Mailtrace || {};

  // ---------- utils ----------
  function safe(fn, ...a) {
    try { return typeof fn === "function" ? fn(...a) : undefined; } catch (_) {}
  }
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }

  function showToast(msg, type = "info") {
    // If you have a real toast system wired at g.toast, use it. Otherwise fallback to console.
    const t = g.toast || {};
    if (type === "error") return safe(t.error, msg) ?? console.error(msg);
    if (type === "success") return safe(t.success, msg) ?? console.log(msg);
    return safe(t.info, msg) ?? console.log(msg);
  }

  function setIngestEnabled(enabled) {
    qsa("[data-ingest]").forEach(btn => {
      btn.disabled = !enabled;
      if (btn.classList) btn.classList.toggle("is-disabled", !enabled);
      // If it's a file input wrapped in a label, also gray the label
      const label = btn.closest("label, .btn, .button, .ui-button");
      if (label && label.classList) label.classList.toggle("is-disabled", !enabled);
    });
  }

  async function httpPost(url, body, headers = {}) {
    const res = await fetch(url, { method: "POST", body, headers });
    let json = {};
    try { json = await res.json(); } catch (_) { /* ignore */ }
    if (res.status === 409) { // our intentional "busy" status
      showToast(json.error === "job_running" || json.error === "job_already_running"
        ? "Matching is running; please wait for it to finish."
        : (json.error || "Busy. Try again when the current job finishes."), "info");
      return { ok: false, busy: true, json };
    }
    if (!res.ok) {
      throw new Error(json && json.error ? json.error : `HTTP ${res.status}`);
    }
    return { ok: true, json };
  }

  // ---------- progress polling ----------
  async function pollMatch(jobId) {
    try {
      const res = await fetch(`/api/match_progress?job_id=${encodeURIComponent(jobId)}`, {
        headers: { "accept": "application/json" }
      });

      // Some backends return 202 Accepted while still running; handle both 200/202.
      const text = await res.text();
      let json = {};
      try { json = text ? JSON.parse(text) : {}; } catch (_) { json = {}; }

      if (!res.ok && res.status !== 202) {
        throw new Error((json && json.error) || `HTTP ${res.status}`);
      }

      // Normalize shapes:
      // new-ish: { ok: true, done: bool, percent: n, error: null }
      // current: { status: "running"|"done"|"error", percent: n, phase: "..." }
      const done   = (json.done !== undefined) ? !!json.done : (json.status === "done");
      const failed = (json.ok === false) || !!json.error || (json.status === "error");

      if (failed) {
        setIngestEnabled(true);
        throw new Error(json.error || "Server error");
      }

      if (done) {
        setIngestEnabled(true);
        safe(g.Mailtrace?.updateProgress, 100);
        showToast("Match complete", "success");
        // If the app defines a dashboard refresh, prefer it; otherwise a soft reload is fine for demos.
        if (typeof g.Mailtrace?.refreshDashboard === "function") {
          safe(g.Mailtrace.refreshDashboard);
        }
        return;
      }

      // still running
      setIngestEnabled(false);
      const pct = Number(json.percent ?? 0);
      safe(g.Mailtrace?.updateProgress, pct);

      setTimeout(() => pollMatch(jobId), 800);
    } catch (err) {
      setIngestEnabled(true);
      console.error("match poll error", err);
      showToast(`Match failed: ${err?.message || "Server error"}`, "error");
    }
  }

  // Expose to global so existing code that calls pollMatch keeps working.
  g.pollMatch = pollMatch;

  // ---------- optional: start button wiring ----------
  document.addEventListener("DOMContentLoaded", () => {
    // Wire the "Start Matching" button if present
    const startBtn = document.querySelector("[data-match-start]");
    if (startBtn) {
      // Make sure it's not a submit button
      try { startBtn.setAttribute("type", "button"); } catch (_) {}

      startBtn.addEventListener("click", async (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        try {
          const { ok, busy, json } = await httpPost("/api/match_start");
          if (!ok) { if (busy) return; throw new Error(json?.error || "Unable to start match"); }
          const jobId = json.job_id || json.jobId || json.id;
          if (!jobId) throw new Error("No job id");
          setIngestEnabled(false);
          pollMatch(jobId);
        } catch (e) {
          console.error("match_start error", e);
          showToast(`Match failed: ${e?.message || "Server error"}`, "error");
        }
      });
    }

    // OPTIONAL: If you have upload/ingest buttons that call fetch from JS,
    // you can use this helper to gracefully handle 409s. Just add data-action and data-form refs.
    // Example markup:
    //   <button data-ingest data-upload="mail" data-form="#mailForm">Upload Mail CSV</button>
    //   <form id="mailForm"><input type="file" name="file" /></form>
    qsa("[data-ingest][data-form]").forEach(btn => {
      btn.addEventListener("click", async (ev) => {
        const formSel = btn.getAttribute("data-form");
        const kind = (btn.getAttribute("data-upload") || "").toLowerCase(); // "mail" or "crm"
        const form = formSel ? qs(formSel) : null;
        if (!form) return; // nothing to wire
        ev.preventDefault();

        try {
          const fd = new FormData(form);
          const url = kind === "crm" ? "/api/ingest-crm" : "/api/ingest-mail";
          const { ok, busy, json } = await httpPost(url, fd);
          if (busy) return; // already toasted
          if (!ok) throw new Error(json && json.error ? json.error : "Upload failed");
          showToast("Upload complete", "success");
          // you can optionally refresh a table or counts here
          safe(g.Mailtrace?.afterIngest, kind, json);
        } catch (e) {
          console.error("ingest error", e);
          showToast(`Upload failed: ${e?.message || "Server error"}`, "error");
        }
      });
    });
  });
})();