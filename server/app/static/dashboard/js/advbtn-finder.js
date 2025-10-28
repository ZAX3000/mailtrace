// advbtn-finder.js
(function () {
  function pickButton() {
    var root = document.querySelector('#cmp-kpis');
    if (!root) return;
    var cand = Array.prototype.slice.call(
      root.querySelectorAll('button, .btn, [role="button"], a.button, a.btn')
    );
    if (!cand.length) return;
    var best = null, bestTop = -Infinity;
    cand.forEach(function (el) {
      var r = el.getBoundingClientRect();
      if (!r || !isFinite(r.top)) return;
      if (r.top > bestTop) { bestTop = r.top; best = el; }
    });
    if (!best) return;
    best.classList.add('mt-advbtn');
    best.style.visibility = 'visible';
    best.style.opacity = '1';
  }

  function boot() {
    pickButton();
    var tries = 0, t = setInterval(function () {
      pickButton();
      if (++tries > 20) clearInterval(t);
    }, 200);
    var root = document.querySelector('#cmp-kpis');
    if (root) {
      var mo = new MutationObserver(function () { pickButton(); });
      mo.observe(root, { childList: true, subtree: true });
    }
  }

  if (document.readyState === 'complete' || document.readyState === 'interactive') {
    setTimeout(boot, 40);
  } else {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 40); });
  }
})();