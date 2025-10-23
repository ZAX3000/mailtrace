// /static/dashboard/js/match_upload.js
(function () {
  // ---- helpers --------------------------------------------------------------

  // Upload one file; server returns { id: "..." }
  async function uploadOne(kind, file) {
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch(`/api/upload_${kind}`, { method: 'POST', body: fd });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data?.id) {
      throw new Error(data?.error || `Failed to upload ${kind}`);
    }
    return data.id;
  }

  // Start matching with JSON body (no multipart here)
  async function startJSON(payload) {
    const r = await fetch('/api/match_start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || 'start failed');
    // Support either { job_id } or { run_id, job_id }
    return d.job_id || d.run_id || d.id;
  }

  async function poll(id) {
    while (true) {
      const r = await fetch('/api/match_progress?job_id=' + encodeURIComponent(id));
      const d = await r.json();
      if (d.status === 'done') {
        if (window.MailTraceLoader?.setProgress) MailTraceLoader.setProgress(100, 'Finishing…');
        break;
      }
      if (d.status === 'error') {
        throw new Error(d.message || 'Server error');
      }
      if (window.MailTraceLoader?.setProgress) {
        MailTraceLoader.setProgress(d.percent || 0, d.phase || 'matching');
      }
      await new Promise(s => setTimeout(s, 1200));
    }
  }

  async function getRes(id) {
    const r = await fetch('/api/match_result?job_id=' + encodeURIComponent(id));
    if (r.status === 202) return null;
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'failed');
    return d;
  }

  // ---- main click handler ---------------------------------------------------

  const btn = document.getElementById('runBtn');
  if (!btn) return;

  btn.addEventListener('click', async (ev) => {
    ev.preventDefault();

    const mail = document.getElementById('mailCsv')?.files?.[0];
    const crm  = document.getElementById('crmCsv')?.files?.[0];
    if (!mail || !crm) { alert('Please select both Mail and CRM CSV files.'); return; }

    // TEMP PATCH (frontend gate) ---------------------------------------------
    // We no longer auto-open the mapper on Run. Require mapping to be pre-set.
    // TODO(migration): When the flow is backend-driven, validate mapping server-side
    // and surface a structured error. Remove this client-side gate at that time.
    let mapping = (window.mapperStorage && window.mapperStorage.read && window.mapperStorage.read()) || {};
    const needsMap = !(mapping?.mail?.address1 && mapping?.crm?.address1);
    if (needsMap) {
      alert('Please configure column mapping first (click “Edit mapping”).');
      document.getElementById('editMappingBtn')?.focus();
      return;
    }
    // End TEMP PATCH ----------------------------------------------------------

    // Prevent double-clicks
    btn.disabled = true;

    try {
      if (window.MailTraceLoader?.show) MailTraceLoader.show({ progress: 0, text: 'Uploading…' });

      // 1) Upload both files; get IDs
      const [mail_id, crm_id] = await Promise.all([
        uploadOne('mail', mail),
        uploadOne('crm',  crm),
      ]);

      // 2) Start with JSON (was multipart before—this fixes no_files_here)
      if (window.MailTraceLoader?.setProgress) MailTraceLoader.setProgress(5, 'Starting…');
      const jobOrRunId = await startJSON({ mail_id, crm_id, mapping });

      // Persist run_id for any other modules that poll on it
      window.MT_CONTEXT = Object.assign(window.MT_CONTEXT || {}, { run_id: jobOrRunId });

      // 3) Poll until done
      await poll(jobOrRunId);

      // 4) Fetch results and render
      const data = await getRes(jobOrRunId); if (!data) return;
      // Render-only functions are exposed by kpi-graph-summary.js
      renderKpis(data.kpis || {});
      renderGraph(data.graph || { months: [], matches: [], prev_year: [] });
      renderSummary(data.summary || []);
      renderTop('cities', data.top_cities || []);
      renderTop('zips',   data.top_zips   || []);

      if (typeof window.onMatchCompleted === 'function') window.onMatchCompleted(jobOrRunId, data);

    } catch (e) {
      console.error(e);
      alert('Match failed: ' + (e.message || 'unknown error'));
    } finally {
      if (window.MailTraceLoader?.hide) MailTraceLoader.hide();
      btn.disabled = false;
    }

    return false;
  }, { capture: true });
})();