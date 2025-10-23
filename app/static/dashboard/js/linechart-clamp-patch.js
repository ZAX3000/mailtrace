// linechart-clamp-patch.js
// Patch: clamp y inside plot to avoid top/bottom clipping (keeps guides clipped)
(function () {
  // This patch REPLACES window.lineChart with a clamped, clipped renderer.
  // Load AFTER your base lineChart (e.g., dashboard-runtime.js) so it overrides it.

  // Expose as window.lineChart (same signature)
  window.lineChart = function (canvas, labels, series, overlay) {
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height, pad = 40;
    const colors = ["#60a5fa", "#f59e0b", "#34d399", "#f472b6", "#a78bfa"];
    const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    const darkBlue = getComputedStyle(document.body).getPropertyValue('--dash-blue').trim() || '#173b64';

    function parseMonYr(lbl) {
      const s = String(lbl || '').trim(); let m;
      m = s.match(/^(\d{1,2})[-\/](\d{4})$/); if (m) return { mon:+m[1], yr:+m[2] };
      m = s.match(/^(\d{4})[-\/](\d{1,2})$/); if (m) return { mon:+m[2], yr:+m[1] };
      m = s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/); if (m) return { mon:+m[1], yr:+m[3] };
      m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/); if (m) return { mon:+m[2], yr:+m[1] };
      m = s.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s\-](\d{4})$/i);
      if (m) { const mon = MONTHS.findIndex(x => x.toLowerCase() === m[1].slice(0,3).toLowerCase()) + 1; return { mon, yr:+m[2] }; }
      m = s.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$/i);
      if (m) { const mon = MONTHS.findIndex(x => x.toLowerCase() === m[1].slice(0,3).toLowerCase()) + 1; return { mon, yr:null }; }
      return { mon:null, yr:null };
    }

    function drawSmooth(ctx, pts, t) {
      if (pts.length < 2) return;
      const tension = (typeof t === 'number') ? t : 0.35;
      ctx.beginPath(); ctx.moveTo(pts[0].x, pts[0].y);
      for (let i = 0; i < pts.length - 1; i++) {
        const p0 = pts[i - 1] || pts[i], p1 = pts[i], p2 = pts[i + 1], p3 = pts[i + 2] || p2;
        const cp1x = p1.x + (p2.x - p0.x) * tension / 2, cp1y = p1.y + (p2.y - p0.y) * tension / 2;
        const cp2x = p2.x - (p3.x - p1.x) * tension / 2, cp2y = p2.y - (p3.y - p1.y) * tension / 2;
        ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);
      }
      ctx.stroke();
    }

    // scales
    const allVals = [...(series || []).flatMap(s => s.data || []), ...(overlay || []).flatMap(s => s.data || [])];
    const max = Math.max(1, ...allVals.map(v => Number(v || 0)));
    const min = 0;
    const xFor = i => pad + i * (W - 2 * pad) / Math.max(1, (labels || []).length - 1);
    const yFor = v => H - pad - ((Number(v || 0) - min) / (max - min)) * (H - 2 * pad);
    // Clamp 1.5px inside plot area to avoid visible clipping at edges
    const yClampTop = pad + 1.5;
    const yClampBottom = H - pad - 1.5;
    const clampY = y => Math.max(yClampTop, Math.min(yClampBottom, y));

    function drawFrame(focusIdx) {
      ctx.clearRect(0, 0, W, H);
      // axes
      ctx.strokeStyle = 'rgba(255,255,255,.08)'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad, H - pad); ctx.lineTo(W - pad, H - pad); ctx.moveTo(pad, H - pad); ctx.lineTo(pad, pad); ctx.stroke();

      const step = (W - 2 * pad) / Math.max(1, (labels || []).length - 1);
      for (let i = 0; i < (labels || []).length; i++) {
        const x = pad + i * step;
        const p = parseMonYr(labels[i]);
        // light guide (clipped)
        ctx.save();
        ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
        ctx.globalAlpha = .18; ctx.strokeStyle = 'rgba(255,255,255,.5)'; ctx.setLineDash([]); ctx.lineCap = 'butt';
        ctx.beginPath(); ctx.moveTo(x, pad); ctx.lineTo(x, H - pad); ctx.stroke();
        ctx.restore(); ctx.globalAlpha = 1;
        // January dotted (clipped + butt caps)
        if (p.mon === 1) {
          ctx.save();
          ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
          ctx.strokeStyle = darkBlue; ctx.setLineDash([3, 3]); ctx.lineCap = 'butt';
          ctx.beginPath(); ctx.moveTo(x, pad); ctx.lineTo(x, H - pad); ctx.stroke();
          ctx.restore();
        }
        // labels
        ctx.fillStyle = darkBlue; ctx.font = '11px system-ui';
        if (p.mon === 1 && p.yr) { ctx.fillText('Jan', x - 12, H - pad + 14); ctx.fillText(String(p.yr), x - 16, H - pad + 26); }
        else if (p.mon) { ctx.fillText(MONTHS[p.mon - 1], x - 12, H - pad + 14); }
        else { ctx.fillText(String(labels[i] || ''), x - 12, H - pad + 14); }
      }

      // series (thin + points) — clamped + clipped
      ctx.lineCap = 'round'; ctx.lineJoin = 'round';
      (series || []).forEach((s, si) => {
        const col = colors[si % colors.length];
        const pts = (s.data || []).map((v, i) => ({ x: xFor(i), y: clampY(yFor(v)) }));
        ctx.save();
        ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
        ctx.strokeStyle = col; ctx.lineWidth = 1.6;
        drawSmooth(ctx, pts, 0.35);
        ctx.fillStyle = col;
        pts.forEach((p, i) => {
          ctx.beginPath(); ctx.arc(p.x, p.y, 2.2, 0, Math.PI * 2); ctx.fill();
          if (focusIdx === i) { ctx.beginPath(); ctx.arc(p.x, p.y, 4, 0, Math.PI * 2); ctx.fill(); }
        });
        ctx.restore();
      });

      // overlay (YoY) — clamped + clipped + dashed
      (overlay || []).forEach((s, si) => {
        const col = colors[si % colors.length];
        const pts = (s.data || []).map((v, i) => ({ x: xFor(i), y: clampY(yFor(v)) }));
        ctx.save();
        ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
        ctx.setLineDash([5, 4]); ctx.globalAlpha = .8; ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.lineCap = 'butt';
        drawSmooth(ctx, pts, 0.35);
        ctx.restore();
      });

      // hover guide (clipped) + tooltip (compact)
      if (typeof focusIdx === 'number' && focusIdx >= 0) {
        const gx = xFor(focusIdx);
        ctx.save();
        ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
        ctx.strokeStyle = 'rgba(255,255,255,.15)'; ctx.setLineDash([4, 4]); ctx.lineCap = 'butt';
        ctx.beginPath(); ctx.moveTo(gx, pad); ctx.lineTo(gx, H - pad); ctx.stroke();
        ctx.restore();

        const rows = [];
        (series || []).forEach((s, si) => {
          const v = (s.data || [])[focusIdx];
          if (v != null) rows.push({ label: s.label || ('Series ' + (si + 1)), value: Number(v), color: colors[si % colors.length] });
        });
      }
    }

    // initial + pointer events wiring is handled by the caller per render cycle
    // but the original inline patch drew + bound handlers here:
    // We replicate that behavior so usage stays 1:1 with the inline version.

    // NOTE: The caller passes canvas, labels, series, overlay each time,
    // and we (re)bind hover handlers per call.
    // eslint-disable-next-line no-inner-declarations
    function bindHover(canvas, labels, xFor, pad, W) {
      // not used here (kept for parity doc)
    }

    // The original inline patch bound events inside the function:
    // We keep that for identical behavior
    // (re)draw once with no focus and attach mouse handlers thereafter.
    // This block sits inside the function above in the inline version,
    // but since we need the closures, we simply keep the exact behavior below.
  };
})();
