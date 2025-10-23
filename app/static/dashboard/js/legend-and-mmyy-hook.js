// Legend + MM-YY tick formatter wrapper for lineChart
(() => {
  if (window.__mmyy_hook__) return;

  function ensureLegendNear(canvas, series, overlay) {
    if (!canvas) return;
    const wrap = canvas.parentElement || canvas;
    let legend = wrap.querySelector('.mt-legend');
    if (!legend) {
      legend = document.createElement('div');
      legend.className = 'mt-legend';
      wrap.appendChild(legend);
    }
    const colors = ["#60a5fa", "#34d399", "#f59e0b", "#f472b6", "#a78bfa"];
    const labelOf = (s, i) => (s && (s.label || s.name || s.title)) || ('Series ' + (i + 1));
    let html = '';
    if (Array.isArray(series)) {
      series.forEach((s, i) => {
        html += '<span class="item"><span class="swatch" style="background:' + colors[i % colors.length] + '; color:' + colors[i % colors.length] + '"></span>' + labelOf(s, i) + '</span>';
      });
    }
    if (overlay && overlay.length) {
      html += '<span class="item hint"><span class="swatch dash" style="color:#94a3b8"></span>Dashed = Previous Year</span>';
    }
    legend.innerHTML = html;
  }

  function install() {
    if (typeof window.lineChart !== 'function') return; // try again later if not ready yet
    const orig = window.lineChart;

    window.lineChart = function(canvas, labels, series, overlay) {
      const proto = (window.CanvasRenderingContext2D && CanvasRenderingContext2D.prototype);
      const saved = proto && proto.fillText;
      if (saved) {
        proto.fillText = function(text, x, y, maxWidth) {
          try {
            if (typeof text === 'string' && /^\d{4}-\d{2}$/.test(text)) {
              // turn YYYY-MM into MM-YY
              text = text.slice(5) + '-' + text.slice(2, 4);
            }
          } catch (_) {}
          return saved.call(this, text, x, y, maxWidth);
        };
      }
      try {
        const res = orig(canvas, labels, series, overlay);
        try { ensureLegendNear(canvas, series, overlay); } catch (_) {}
        return res;
      } finally {
        if (saved) proto.fillText = saved;
      }
    };

    window.__mmyy_hook__ = true;
  }

  // install immediately if possible; otherwise after DOMContentLoaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', install);
  } else {
    install();
  }
})();