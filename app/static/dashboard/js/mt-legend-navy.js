// mt-legend-navy.js
(function () {
  var NAVY = '#0d2c54';

  // Treat these as legend labels or note fragments (segment-aware)
  var patterns = [
    /mail\s*volume/i,
    /crm\s*jobs/i,
    /matches/i,
    /dashed/i,
    /previous\s*year/i
  ];

  function isHit(text) {
    if (text == null) return false;
    var t = String(text).replace(/\s+/g, ' ').trim();
    for (var i = 0; i < patterns.length; i++) {
      if (patterns[i].test(t)) return true;
    }
    return false;
  }

  // DOM/SVG pass to change color of matching text
  function paintDOM(root) {
    var walker = document.createTreeWalker(root || document.body, NodeFilter.SHOW_TEXT, null, false);
    var node;
    while ((node = walker.nextNode())) {
      var val = (node.nodeValue || '').trim();
      if (!val) continue;
      if (!isHit(val)) continue;
      var el = node.parentElement;
      if (!el) continue;
      el.style.color = NAVY;
      if (el.tagName && el.tagName.toLowerCase() === 'text') {
        el.setAttribute('fill', NAVY);
        el.style.fill = NAVY;
        // Guard against muted styles
        el.setAttribute('opacity', '1');
        el.style.opacity = '1';
      }
    }
  }

  // Canvas hook for chart libraries
  (function hookCanvas() {
    if (typeof CanvasRenderingContext2D === 'undefined') return;
    var proto = CanvasRenderingContext2D.prototype;
    if (proto.__mtLegendPatchedV3) return;
    ['fillText', 'strokeText'].forEach(function (method) {
      var orig = proto[method];
      if (!orig) return;
      proto[method] = function (text) {
        try {
          if (isHit(text)) {
            var pf = this.fillStyle, ps = this.strokeStyle,
              pa = this.globalAlpha, sc = this.shadowColor, flt = this.filter;
            this.fillStyle = NAVY;
            this.strokeStyle = NAVY;
            this.globalAlpha = 1;
            this.shadowColor = 'transparent';
            this.filter = 'none';
            var out = orig.apply(this, arguments);
            this.fillStyle = pf; this.strokeStyle = ps;
            this.globalAlpha = pa; this.shadowColor = sc; this.filter = flt;
            return out;
          }
        } catch (e) { }
        return orig.apply(this, arguments);
      };
    });
    proto.__mtLegendPatchedV3 = true;
  })();

  // Library-specific adjustments (Plotly, Highcharts, ECharts, ChartJS)
  function tryPlotly() {
    if (!window.Plotly) return;
    document.querySelectorAll('.js-plotly-plot').forEach(function (div) {
      try { window.Plotly.relayout(div, { 'legend.font.color': NAVY }); } catch (e) { }
    });
  }

  function tryHighcharts() {
    if (!window.Highcharts || !window.Highcharts.charts) return;
    window.Highcharts.charts.forEach(function (c) {
      if (!c) return;
      try {
        c.update({ legend: { itemStyle: { color: NAVY }, itemHoverStyle: { color: NAVY } } }, false);
        c.redraw(false);
      } catch (e) { }
    });
  }

  function tryECharts() {
    if (!window.echarts) return;
    document.querySelectorAll('[__echarts_instance__]').forEach(function (el) {
      try {
        var inst = window.echarts.getInstanceByDom(el);
        if (!inst) return;
        var opt = inst.getOption && inst.getOption();
        inst.setOption({
          legend: (opt && opt.legend && opt.legend.length ? opt.legend.map(function (l) {
            l = l || {}; l.textStyle = l.textStyle || {}; l.textStyle.color = NAVY; return l;
          }) : [{ textStyle: { color: NAVY } }])
        }, false);
      } catch (e) { }
    });
  }

  function tryChartJS() {
    if (!window.Chart || !window.Chart.getChart) return;
    document.querySelectorAll('canvas').forEach(function (cv) {
      try {
        var ch = window.Chart.getChart(cv);
        if (!ch || !ch.options) return;
        ch.options.plugins = ch.options.plugins || {};
        ch.options.plugins.legend = ch.options.plugins.legend || {};
        ch.options.plugins.legend.labels = ch.options.plugins.legend.labels || {};
        ch.options.plugins.legend.labels.color = NAVY;
        ch.update('none');
      } catch (e) { }
    });
  }

  // Function to trigger all chart legend updates
  function tick() {
    paintDOM(document.body);
    tryPlotly(); tryHighcharts(); tryECharts(); tryChartJS();
  }

  // Boot the process
  function boot() {
    tick();
    var i = 0, t = setInterval(function () {
      tick();
      if (++i > 30) clearInterval(t);
    }, 200);
    var mo = new MutationObserver(function (muts) {
      for (var k = 0; k < muts.length; k++) {
        for (var j = 0; j < (muts[k].addedNodes || []).length; j++) {
          var n = muts[k].addedNodes[j];
          if (n.nodeType === 1) paintDOM(n);
        }
      }
    });
    mo.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(boot, 50);
  } else {
    document.addEventListener('DOMContentLoaded', boot);
  }
})();