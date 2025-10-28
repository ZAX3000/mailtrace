// kpi-summary-renderer.js
(function () {
  function setKpi(name, val) {
    const el = document.querySelector(`[data-kpi="${name}"]`);
    if (!el) return;
    if (name === 'match_rate') { el.textContent = (Number(val) || 0).toFixed(2) + '%'; }
    else if (name === 'match_revenue') { el.textContent = fmtMoney(val); }
    else { el.textContent = (Number(val) || 0).toLocaleString(); }
  }

  function fmtMoney(n) {
    try { return '$' + n.toLocaleString(undefined, { maximumFractionDigits: 2 }); }
    catch (_) { return '$0'; }
  }

  function renderKpis(kpiData) {
    setKpi('mail', kpiData.mail);
    setKpi('uniqmail', kpiData.uniqmail);
    setKpi('crm', kpiData.crm);
    setKpi('matches', kpiData.matches);
    setKpi('match_rate', kpiData.match_rate);
    setKpi('match_revenue', kpiData.match_revenue);
  }

  // Export the function to update KPIs
  window.renderKpis = renderKpis;
})();