// kpi-graph-summary.js
(function () {
  // Format money with 2 decimal points
  function fmtMoney(n) {
    try { n = Number(n) || 0; return '$' + n.toLocaleString(undefined, { maximumFractionDigits: 2 }); }
    catch (_) { return '$0'; }
  }

  // Set the KPI values
  function setKpi(name, val) {
    const el = document.querySelector(`[data-kpi="${name}"]`);
    if (!el) return;
    if (name === 'match_rate') { el.textContent = (Number(val) || 0).toFixed(2) + '%'; }
    else if (name === 'match_revenue') { el.textContent = fmtMoney(val); }
    else { el.textContent = (Number(val) || 0).toLocaleString(); }
  }

  // Render the KPIs
  function renderKpis(k) {
    setKpi('mail', k.mail);
    setKpi('uniqmail', k.uniqmail);
    setKpi('crm', k.crm);
    setKpi('matches', k.matches);
    setKpi('match_rate', k.match_rate);
    setKpi('match_revenue', k.match_revenue);
  }

  // Render the graph
  function renderGraph(g) {
    const c = document.getElementById('chart');
    if (!c || !c.getContext) return;
    const ctx = c.getContext('2d');
    function draw() {
      const dpr = window.devicePixelRatio || 1, W = c.clientWidth * dpr, H = c.clientHeight * dpr;
      c.width = W; c.height = H;
      ctx.clearRect(0, 0, W, H);
      const padL = 40, padR = 10, padT = 16, padB = 26, xs = g.months || [], ys = g.matches || [], py = g.prev_year || [];
      const n = xs.length;
      if (!n) return;
      const ymax = Math.max(10, Math.max.apply(null, ys.concat(py)));
      const yPix = v => H - padB - (v / ymax) * (H - padT - padB);
      const xPix = i => padL + (i * (W - padL - padR) / Math.max(1, n - 1));
      ctx.strokeStyle = '#e6edf5'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(padL, padT); ctx.lineTo(padL, H - padB); ctx.lineTo(W - padR, H - padB); ctx.stroke();
      ctx.strokeStyle = '#0d2c54'; ctx.lineWidth = 2; ctx.beginPath();
      for (let i = 0; i < n; i++) {
        const x = xPix(i), y = yPix(ys[i] || 0);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();

      const btn = document.getElementById('chartYoyToggle');
      const showYoy = localStorage.getItem('mt_yoy') === '1';
      if (btn) {
        btn.textContent = showYoy ? 'Hide YoY' : 'Show YoY';
        btn.onclick = () => { localStorage.setItem('mt_yoy', showYoy ? '0' : '1'); renderGraph(g); };
      }
      if (showYoy && py.length === n) {
        ctx.setLineDash([8, 6]);
        ctx.beginPath();
        for (let j = 0; j < n; j++) {
          const x2 = xPix(j), y2 = yPix(py[j] || 0);
          if (j === 0) ctx.moveTo(x2, y2);
          else ctx.lineTo(x2, y2);
        }
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }
    draw();
    window.addEventListener('resize', draw);
  }

  // Render the summary table
  function renderSummary(rows) {
    const tb = document.querySelector('[data-summary="rows"]');
    if (!tb) return;
    tb.innerHTML = '';
    (rows || []).forEach(r => {
      const tr = document.createElement('tr');
      const cols = [
        r.mail_address1 || '', r.mail_unit || '', r.crm_address1 || '', r.crm_unit || '', r.city || '', r.state || '',
        (r.zip || '').toString().slice(0, 5), r.mail_dates || '', r.crm_date || '', r.amount || 0, (r.confidence || 0) + '%', r.notes || ''
      ];
      cols.forEach((val, i) => {
        const td = document.createElement('td');
        td.textContent = (i === 9 ? ('$' + Number(val).toLocaleString()) : val);
        if (i === 9 || i === 10) td.style.textAlign = 'right';
        if (i === 10) {
          const v = parseFloat(String(val).replace('%', '')) || 0;
          td.className = v > 95 ? 'conf-green' : (v >= 86 ? 'conf-yellow' : 'conf-red');
        }
        tr.appendChild(td);
      });
      tb.appendChild(tr);
    });
  }

  // Render top cities or zips
  function renderTop(kind, arr) {
    const tb = document.querySelector(`[data-top="${kind}"]`);
    if (!tb) return;
    tb.innerHTML = '';
    (arr || []).forEach(p => {
      const tr = document.createElement('tr');
      const n = p[0], t = p[1] || 0;
      const c1 = document.createElement('td'); c1.textContent = n;
      const c2 = document.createElement('td'); c2.style.textAlign = 'center'; c2.textContent = Number(t).toLocaleString();
      const c3 = document.createElement('td'); c3.style.textAlign = 'center'; c3.textContent = '';
      tr.appendChild(c1); tr.appendChild(c2); tr.appendChild(c3);
      tb.appendChild(tr);
    });
  }

  // Expose renderers for the matching flow to call
  window.renderKpis    = renderKpis;
  window.renderGraph   = renderGraph;
  window.renderSummary = renderSummary;
  window.renderTop     = renderTop;
})();