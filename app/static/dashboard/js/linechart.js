// /static/dashboard/js/linechart-authoritative.js
(() => {
  // Bump this if you add features so older files can't clobber it.
  const VERSION = 4;
  if (window._lineChartVersion && window._lineChartVersion >= VERSION) return;
  window._lineChartVersion = VERSION;

  window.lineChart = function lineChart(canvas, labels, series, overlay) {
    // --- Hi-DPI: paint in CSS pixels, size backing store to DPR ---
    const cssW = canvas.clientWidth || canvas.width;
    const cssH = canvas.clientHeight || canvas.height;
    const dpr = Math.max(1, window.devicePixelRatio || 1);
    if (canvas.width !== Math.round(cssW * dpr) || canvas.height !== Math.round(cssH * dpr)) {
      canvas.width  = Math.round(cssW * dpr);
      canvas.height = Math.round(cssH * dpr);
    }
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0); // draw in CSS pixels

    // --- Layout / palette ---
    const W = cssW, H = cssH, pad = 40;
    const colors  = ["#60a5fa", "#f59e0b", "#34d399", "#f472b6", "#a78bfa"];
    const MONTHS  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    const comp    = getComputedStyle(document.body);
    const darkBlue = comp.getPropertyValue('--dash-blue').trim() || '#173b64';

    // --- Helpers ---

    // Parse labels like "01-2024", "2024-01", "01/01/2024", "Jan 2024", "Jan"
    function parseMonYr(lbl) {
      const s = String(lbl || '').trim();
      let m;
      if ((m = s.match(/^(\d{1,2})[-\/](\d{4})$/)))                  return { mon: +m[1], yr: +m[2] };
      if ((m = s.match(/^(\d{4})[-\/](\d{1,2})$/)))                  return { mon: +m[2], yr: +m[1] };
      if ((m = s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/)))    return { mon: +m[1], yr: +m[3] };
      if ((m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/)))    return { mon: +m[2], yr: +m[1] };
      if ((m = s.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s\-](\d{4})$/i))) {
        const mon = MONTHS.findIndex(x => x.toLowerCase() === m[1].slice(0,3).toLowerCase()) + 1;
        return { mon, yr: +m[2] };
      }
      if ((m = s.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$/i))) {
        const mon = MONTHS.findIndex(x => x.toLowerCase() === m[1].slice(0,3).toLowerCase()) + 1;
        return { mon, yr: null };
      }
      return { mon: null, yr: null };
    }

    // Tensioned smooth curve through points
    function drawSmooth(ctx, pts, t = 0.35) {
      if (pts.length < 2) return;
      ctx.beginPath();
      ctx.moveTo(pts[0].x, pts[0].y);
      for (let i = 0; i < pts.length - 1; i++) {
        const p0 = pts[i - 1] || pts[i];
        const p1 = pts[i];
        const p2 = pts[i + 1];
        const p3 = pts[i + 2] || p2;
        const cp1x = p1.x + (p2.x - p0.x) * t / 2;
        const cp1y = p1.y + (p2.y - p0.y) * t / 2;
        const cp2x = p2.x - (p3.x - p1.x) * t / 2;
        const cp2y = p2.y - (p3.y - p1.y) * t / 2;
        ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);
      }
      ctx.stroke();
    }

    // --- Scales (with 15% headroom) ---
    const safeLabels = Array.isArray(labels) ? labels : [];
    const allVals = [
      ...(Array.isArray(series)  ? series  : []).flatMap(s => s?.data || []),
      ...(Array.isArray(overlay) ? overlay : []).flatMap(s => s?.data || []),
    ].map(v => Number(v) || 0);
    const rawMax   = Math.max(1, ...(allVals.length ? allVals : [1]));
    const yHeadroom = Math.max(5, (rawMax - 0) * 0.15);
    const yMin = 0;
    const yMax = rawMax + yHeadroom;

    const xFor = i => pad + i * (W - 2 * pad) / Math.max(1, safeLabels.length - 1);
    const yFor = v => H - pad - ((Number(v || 0) - yMin) / Math.max(1e-9, (yMax - yMin))) * (H - 2 * pad);

    // Clamp Y 1.5px inside plot so curves/points never visibly clip
    const yClampTop = pad + 1.5;
    const yClampBottom = H - pad - 1.5;
    const clampY = y => Math.max(yClampTop, Math.min(yClampBottom, y));

    // --- Frame render (with optional focus index for hover) ---
    function drawFrame(focusIdx) {
      ctx.clearRect(0, 0, W, H);

      // Axes
      ctx.strokeStyle = 'rgba(255,255,255,.08)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad, H - pad);
      ctx.lineTo(W - pad, H - pad);
      ctx.moveTo(pad, H - pad);
      ctx.lineTo(pad, pad);
      ctx.stroke();

      // Vertical guides + Jan dotted + labels (all clipped to plot)
      const step = (W - 2 * pad) / Math.max(1, safeLabels.length - 1);
      for (let i = 0; i < safeLabels.length; i++) {
        const x = pad + i * step;
        const p = parseMonYr(safeLabels[i]);

        // Light guide (clip)
        ctx.save();
        ctx.beginPath();
        ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad);
        ctx.clip();
        ctx.globalAlpha = .18;
        ctx.strokeStyle = 'rgba(255,255,255,.5)';
        ctx.setLineDash([]);
        ctx.lineCap = 'butt';
        ctx.beginPath();
        ctx.moveTo(x, pad);
        ctx.lineTo(x, H - pad);
        ctx.stroke();
        ctx.restore();
        ctx.globalAlpha = 1;

        // January dotted (clip + butt caps)
        if (p.mon === 1) {
          ctx.save();
          ctx.beginPath();
          ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad);
          ctx.clip();
          ctx.strokeStyle = darkBlue;
          ctx.setLineDash([3, 3]);
          ctx.lineCap = 'butt';
          ctx.beginPath();
          ctx.moveTo(x, pad);
          ctx.lineTo(x, H - pad);
          ctx.stroke();
          ctx.restore();
        }

        // Labels
        ctx.fillStyle = darkBlue;
        ctx.font = '11px system-ui';
        if (p.mon === 1 && p.yr) {
          ctx.fillText('Jan', x - 12, H - pad + 14);
          ctx.fillText(String(p.yr), x - 16, H - pad + 26);
        } else if (p.mon) {
          ctx.fillText(MONTHS[p.mon - 1], x - 12, H - pad + 14);
        } else {
          ctx.fillText(String(safeLabels[i] || ''), x - 12, H - pad + 14);
        }
      }

      // Series (solid, clipped, rounded caps, points)
      ctx.lineCap = 'round';
      ctx.lineJoin = 'round';
      (series || []).forEach((s, si) => {
        const col = colors[si % colors.length];
        const pts = (s?.data || []).map((v, i) => ({ x: xFor(i), y: clampY(yFor(v)) }));

        ctx.save();
        ctx.beginPath();
        ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad);
        ctx.clip();

        ctx.strokeStyle = col;
        ctx.lineWidth = 1.6;
        drawSmooth(ctx, pts, 0.35);

        // points
        ctx.fillStyle = col;
        pts.forEach((p, i) => {
          ctx.beginPath();
          ctx.arc(p.x, p.y, 2.2, 0, Math.PI * 2);
          ctx.fill();
          if (focusIdx === i) {
            ctx.beginPath();
            ctx.arc(p.x, p.y, 4, 0, Math.PI * 2);
            ctx.fill();
          }
        });
        ctx.restore();
      });

      // YoY overlay (dashed, clipped, butt caps)
      (overlay || []).forEach((s, si) => {
        const col = colors[si % colors.length];
        const pts = (s?.data || []).map((v, i) => ({ x: xFor(i), y: clampY(yFor(v)) }));

        ctx.save();
        ctx.beginPath();
        ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad);
        ctx.clip();

        ctx.setLineDash([5, 4]);
        ctx.globalAlpha = .8;
        ctx.strokeStyle = col;
        ctx.lineWidth = 2;
        ctx.lineCap = 'butt';
        drawSmooth(ctx, pts, 0.35);
        ctx.restore();
      });

      // Hover guide + tooltip (clipped)
      if (typeof focusIdx === 'number' && focusIdx >= 0 && focusIdx < safeLabels.length) {
        const gx = xFor(focusIdx);

        // guided vertical dashed line
        ctx.save();
        ctx.beginPath();
        ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad);
        ctx.clip();
        ctx.strokeStyle = 'rgba(255,255,255,.15)';
        ctx.setLineDash([4, 4]);
        ctx.lineCap = 'butt';
        ctx.beginPath();
        ctx.moveTo(gx, pad);
        ctx.lineTo(gx, H - pad);
        ctx.stroke();
        ctx.restore();

        // collect rows for tooltip
        const rows = [];
        (series || []).forEach((s, si) => {
          const v = (s?.data || [])[focusIdx];
          if (v != null) rows.push({ label: s?.label || ('Series ' + (si + 1)), value: Number(v), color: colors[si % colors.length] });
        });
        
      }
    }

    // --- Initial draw + pointer handlers (like your existing patches) ---
    drawFrame(null);

    // Mouse move/leave for hover behavior (CSS-pixel math)
    canvas.onmousemove = function (e) {
      const rect = canvas.getBoundingClientRect();
      const mxCss = (e.clientX - rect.left); // CSS pixels
      const step = (W - 2 * pad) / Math.max(1, safeLabels.length - 1);
      let idx = Math.round((mxCss - pad) / step);
      idx = Math.max(0, Math.min(safeLabels.length - 1, idx));
      drawFrame(idx);
    };
    canvas.onmouseleave = function () { drawFrame(null); };
  };
})();