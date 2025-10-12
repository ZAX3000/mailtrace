
function mtHandleStartResponse(resp) {
  if (!resp || (resp.ok === false)) {
    return resp && resp.text ? resp.text().then(t => { throw new Error(t || ('HTTP ' + resp.status)); }) : Promise.reject(new Error('HTTP error'));
  }
  return (typeof resp.json === 'function' ? resp.json() : Promise.resolve(resp)).then(data => {
    if (data && data.error) { throw new Error(data.error); }
    return data;
  });
}

/* Mailtrace tolerant progress polling (frontend-only patch)
 * - Avoids false "Match failed: ' + (e && (e.message || e) || 'Server error') + '" popups when backend is OK.
 * - Accepts both old and new response shapes.
 */
(function () {
  const g = (typeof window !== "undefined") ? window : globalThis;
  g.Mailtrace = g.Mailtrace || {};

  function safe(fn, ...a) {
    try { return fn && fn(...a); } catch (_) {}
  }

  async function pollMatch(jobId) {
    try {
      const res = await fetch(`/api/match_progress?job_id=${encodeURIComponent(jobId)}`, { headers: { "accept": "application/json" } });
      const json = await res.json().catch(() => ({}));

      if (!res.ok) throw new Error(json.error || `HTTP ${res.status}`);

      // Normalize shapes:
      // new: { ok: true, done: bool, percent: n, error: null }
      // old: { status: "running"|"done"|"error", percent: n, error: null|"..." }
      const done   = (json.done !== undefined) ? !!json.done : (json.status === "done");
      const failed = (json.ok === false) || !!json.error || (json.status === "error");

      if (failed) {
        throw new Error(json.error || "Server error");
      }

      if (done) {
        safe(g.toast?.success, "Match complete");
        safe(g.Mailtrace?.refreshDashboard);
        return;
      }

      const pct = Number(json.percent ?? 0);
      safe(g.Mailtrace?.updateProgress, pct);

      setTimeout(() => pollMatch(jobId), 800);
    } catch (err) {
      console.error("match poll error", err);
      safe(g.toast?.error, `Match failed: ${err?.message || "Server error"}`);
    }
  }

  // Expose to global so existing code that calls pollMatch keeps working.
  g.pollMatch = pollMatch;

  // Optional helper: if page defines a start button with [data-match-start],
  // wire it automatically. This is no-op if element not present.
  document.addEventListener("DOMContentLoaded", () => {
    const btn = document.querySelector("[data-match-start]");
    if (!btn) return;
    btn.addEventListener("click", async () => {
      try {
        const res = await fetch("/api/match_start", { method: "POST" });
        const json = await res.json().catch(() => ({}));
        if (!res.ok || json.ok === false) throw new Error(json.error || `HTTP ${res.status}`);
        const jobId = json.job_id || json.jobId || json.id;
        if (!jobId) throw new Error("No job id");
        pollMatch(jobId);
      } catch (e) {
        console.error("match_start error", e);
        safe(g.toast?.error, `Match failed: ${e?.message || "Server error"}`);
      }
    });
  });
})();
