(function() {
    const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const colors = ["#60a5fa", "#f59e0b", "#34d399", "#f472b6", "#a78bfa"];
    const darkBlue = getComputedStyle(document.body).getPropertyValue('--dash-blue').trim() || '#173b64';
    
    // Parse label (e.g., 'Jan 2024' -> { mon: 1, yr: 2024 })
    function parseMonYr(lbl) {
        const s = String(lbl || '').trim();
        let m;
        m = s.match(/^(\d{1,2})[-\/](\d{4})$/); if (m) { return { mon: +m[1], yr: +m[2] }; }
        m = s.match(/^(\d{4})[-\/](\d{1,2})$/); if (m) { return { mon: +m[2], yr: +m[1] }; }
        m = s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/); if (m) { return { mon: +m[1], yr: +m[3] }; }
        m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/); if (m) { return { mon: +m[2], yr: +m[1] }; }
        m = s.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s\-](\d{4})$/i);
        if (m) { const mon = MONTHS.findIndex(x => x.toLowerCase() === m[1].slice(0, 3).toLowerCase()) + 1; return { mon, yr: +m[2] }; }
        m = s.match(/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$/i);
        if (m) { const mon = MONTHS.findIndex(x => x.toLowerCase() === m[1].slice(0, 3).toLowerCase()) + 1; return { mon, yr: null }; }
        return { mon: null, yr: null };
    }

    // Smooth line drawing
    function drawSmooth(ctx, pts, t) {
        if (pts.length < 2) return;
        const tension = (typeof t === 'number') ? t : 0.35;
        ctx.beginPath(); ctx.moveTo(pts[0].x, pts[0].y);
        for (let i = 0; i < pts.length - 1; i++) {
            const p0 = pts[i - 1] || pts[i], p1 = pts[i], p2 = pts[i + 1], p3 = pts[i + 2] || p2;
            const cp1x = p1.x + (p2.x - p0.x) * t / 2, cp1y = p1.y + (p2.y - p0.y) * t / 2;
            const cp2x = p2.x - (p3.x - p1.x) * t / 2, cp2y = p2.y - (p3.y - p1.y) * t / 2;
            ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);
        }
        ctx.stroke();
    }

    // Main graph rendering function
    window.MailTraceUI.YOYGraphRenderer = function(canvas, labels, series, overlay) {
        const ctx = canvas.getContext('2d');
        const W = canvas.width, H = canvas.height, pad = 40;

        const allVals = [...(series || []).flatMap(s => s.data || []), ...(overlay || []).flatMap(s => s.data || [])];
        const max = Math.max(1, ...allVals.map(v => Number(v || 0)));
        const min = 0;

        const xFor = i => pad + i * (W - 2 * pad) / Math.max(1, (labels || []).length - 1);
        const yFor = v => H - pad - ((Number(v || 0) - min) / (max - min)) * (H - 2 * pad);

        function drawFrame(focusIdx) {
            ctx.clearRect(0, 0, W, H);
            ctx.strokeStyle = 'rgba(255,255,255,.08)'; ctx.lineWidth = 1;
            ctx.beginPath(); ctx.moveTo(pad, H - pad); ctx.lineTo(W - pad, H - pad); ctx.moveTo(pad, H - pad); ctx.lineTo(pad, pad); ctx.stroke();

            const step = (W - 2 * pad) / Math.max(1, (labels || []).length - 1);
            for (let i = 0; i < (labels || []).length; i++) {
                const x = pad + i * step;
                const p = parseMonYr(labels[i]);
                ctx.save();
                ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
                ctx.globalAlpha = .18; ctx.strokeStyle = 'rgba(255,255,255,.5)'; ctx.setLineDash([]); ctx.lineCap = 'butt';
                ctx.beginPath(); ctx.moveTo(x, pad); ctx.lineTo(x, H - pad); ctx.stroke();
                ctx.restore(); ctx.globalAlpha = 1;

                if (p.mon === 1) {
                    ctx.save();
                    ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
                    ctx.strokeStyle = darkBlue; ctx.setLineDash([3, 3]); ctx.lineCap = 'butt';
                    ctx.beginPath(); ctx.moveTo(x, pad); ctx.lineTo(x, H - pad); ctx.stroke();
                    ctx.restore();
                }
                ctx.fillStyle = darkBlue; ctx.font = '11px system-ui';
                if (p.mon === 1 && p.yr) { ctx.fillText('Jan', x - 12, H - pad + 14); ctx.fillText(String(p.yr), x - 16, H - pad + 26); }
                else if (p.mon) { ctx.fillText(MONTHS[p.mon - 1], x - 12, H - pad + 14); }
                else { ctx.fillText(String(labels[i] || ''), x - 12, H - pad + 14); }
            }

            ctx.lineCap = 'round'; ctx.lineJoin = 'round';
            (series || []).forEach((s, si) => {
                const col = colors[si % colors.length];
                const pts = (s.data || []).map((v, i) => ({ x: xFor(i), y: yFor(v) }));
                ctx.save();
                ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
                ctx.strokeStyle = col; ctx.lineWidth = 1.6;
                drawSmooth(ctx, pts, 0.35);
                ctx.fillStyle = col;
                pts.forEach((p, i) => { ctx.beginPath(); ctx.arc(p.x, p.y, 2.2, 0, Math.PI * 2); ctx.fill(); if (focusIdx === i) { ctx.beginPath(); ctx.arc(p.x, p.y, 4, 0, Math.PI * 2); ctx.fill(); } });
                ctx.restore();
            });

            (overlay || []).forEach((s, si) => {
                const col = colors[si % colors.length];
                const pts = (s.data || []).map((v, i) => ({ x: xFor(i), y: yFor(v) }));
                ctx.save();
                ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
                ctx.setLineDash([5, 4]); ctx.globalAlpha = .8; ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.lineCap = 'butt';
                drawSmooth(ctx, pts, 0.35);
                ctx.restore();
            });

            if (typeof focusIdx === 'number' && focusIdx >= 0) {
                const gx = xFor(focusIdx);
                ctx.save();
                ctx.beginPath(); ctx.rect(pad, pad, W - 2 * pad, H - 2 * pad); ctx.clip();
                ctx.strokeStyle = 'rgba(255,255,255,.15)'; ctx.setLineDash([4, 4]); ctx.lineCap = 'butt';
                ctx.beginPath(); ctx.moveTo(gx, pad); ctx.lineTo(gx, H - pad); ctx.stroke();
                ctx.restore();

                const rows = [];
                (series || []).forEach((s, si) => { const v = (s.data || [])[focusIdx]; if (v != null) { rows.push({ label: s.label || ('Series ' + (si + 1)), value: Number(v), color: colors[si % colors.length] }); } });
            }
        }

        drawFrame(null);
        canvas.onmousemove = function (e) {
            const rect = canvas.getBoundingClientRect();
            const mx = (e.clientX - rect.left) * (canvas.width / rect.width);
            const step = (W - 2 * pad) / Math.max(1, (labels || []).length - 1);
            let idx = Math.round((mx - pad) / step);
            idx = Math.max(0, Math.min((labels || []).length - 1, idx));
            drawFrame(idx);
        };
        canvas.onmouseleave = function () { drawFrame(null); };
    };
})();