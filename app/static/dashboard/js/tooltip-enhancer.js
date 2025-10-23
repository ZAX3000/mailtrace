// tooltip-enhancer.js
(function () {
  // replace existing patch if present
  if (window.__mtTooltipPatchedV2) return; window.__mtTooltipPatchedV2 = true;

  const tryPatch = () => {
    if (typeof window.lineChart !== 'function') { setTimeout(tryPatch, 50); return; }
    const orig = window.lineChart;
    window.lineChart = function (canvas, labels, series, overlay) {
      try { orig(canvas, labels, series, overlay); } catch (e) { console.error(e); }

      try {
        const W = canvas.width, H = canvas.height, pad = 30;
        const all = [...(series || []).flatMap(s => s.data || []), ...((overlay || []).flatMap(s => s.data || []))];
        const min = 0, max = Math.max(1, Math.max.apply(null, [1].concat(all)));
        const xFor = i => pad + i * (W - 2 * pad) / Math.max(1, (labels || []).length - 1);
        const yFor = v => H - pad - ((v - min) / (max - min)) * (H - 2 * pad);
        canvas.__mtMeta = { labels, series, overlay, pad, W, H, min, max, xFor, yFor };

        // tooltip
        let tip = canvas.__mtTip;
        if (!tip) {
          tip = document.createElement('div');
          tip.className = 'mt-tooltip';
          Object.assign(tip.style, {
            position: 'absolute', pointerEvents: 'none', background: '#fff', color: '#0d2c54',
            border: '1px solid #e6edf5', padding: '6px 8px', borderRadius: '6px',
            font: '12px system-ui', boxShadow: '0 4px 14px rgba(13,44,84,.15)', zIndex: 1000, display: 'none',
            whiteSpace: 'nowrap'
          });
          canvas.__mtTip = tip;
          const host = canvas.parentElement || document.body;
          if (getComputedStyle(host).position === 'static') host.style.position = 'relative';
          host.appendChild(tip);
        }

        // listeners
        if (!canvas.__mtHoverV2) {
          canvas.addEventListener('mousemove', function (e) {
            const rect = canvas.getBoundingClientRect();
            const mx = e.clientX - rect.left;
            const my = e.clientY - rect.top;
            const meta = canvas.__mtMeta; if (!meta) return;
            const { labels, series, overlay, pad, W, H } = meta;
            if (mx < pad || mx > W - pad || my < pad || my > H - pad) { tip.style.display = 'none'; return; }
            const frac = (mx - pad) / Math.max(1, (W - 2 * pad));
            let idx = Math.round(frac * Math.max(0, (labels || []).length - 1));
            idx = Math.max(0, Math.min((labels || []).length - 1, idx));

            // format label mm-yyyy
            const rawLab = (labels || [])[idx] || '';
            let lab = rawLab;
            const m1 = /^(\d{4})-(\d{1,2})$/.exec(rawLab);           // yyyy-mm
            const m2 = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(rawLab); // yyyy-mm-dd
            if (m2) lab = (m2[2].padStart(2, '0')) + '-' + m2[1]; else
              if (m1) lab = (m1[2].padStart(2, '0')) + '-' + m1[1];

            // series values
            const parts = [];
            (series || []).forEach(s => { const v = (s.data || [])[idx]; parts.push(`${s.label || 'Series'}: ${Number(v || 0).toLocaleString()}`); });
            (overlay || []).forEach(s => { const v = (s.data || [])[idx]; parts.push(`${s.label || 'YoY'}: ${Number(v || 0).toLocaleString()}`); });
            tip.innerHTML = `<div style="font-weight:700;margin-bottom:4px">${lab}</div><div>${parts.join('<br>')}</div>`;
            tip.style.display = 'block';

            // position with larger right offset; flip to left near edge
            const host = canvas.parentElement || document.body;
            const hostRect = host.getBoundingClientRect();
            const offsetX = 28, offsetY = 16;
            let left = mx + offsetX, top = my + offsetY;
            const need = left + tip.offsetWidth + 8;
            if (need > hostRect.width) {
              left = mx - tip.offsetWidth - offsetX;
              if (left < 8) left = hostRect.width - tip.offsetWidth - 8;
            }
            if (top + tip.offsetHeight + 8 > hostRect.height) {
              top = hostRect.height - tip.offsetHeight - 8;
            }
            tip.style.left = left + 'px';
            tip.style.top = top + 'px';
          });

          canvas.addEventListener('mouseleave', () => { if (canvas.__mtTip) canvas.__mtTip.style.display = 'none'; });
          canvas.__mtHoverV2 = true;
        }
      } catch (e) { console.error('Tooltip v2 error:', e); }
    };
  };
  tryPatch();
})();