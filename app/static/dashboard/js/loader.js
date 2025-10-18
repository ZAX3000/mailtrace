/*! Lsv1 Loader (no listeners, no hooks) */
(function () {
  if (window.MailTraceLoader) return;

  var modal, fill, val, etaEl, scene, scope, chart;

  function clamp(n) { return Math.max(0, Math.min(100, Math.round(n))); }
  function setPct(p) {
    p = clamp(p);
    if (fill) fill.style.width = p + '%';
    if (val) val.textContent = p + '%';
  }
  function setETA(sec) {
    if (!etaEl) return;
    if (sec == null) { etaEl.innerHTML = '≈ <strong>--:--</strong> left'; return; }
    sec = Math.max(0, Math.round(sec));
    var m = Math.floor(sec / 60), s = sec % 60;
    etaEl.innerHTML = m > 0 ? '≈ <strong>' + m + 'm ' + s + 's</strong> left' : '≈ <strong>' + s + 's</strong> left';
  }

  function ensureDOM() {
    if (modal) return;

    modal = document.createElement('div');
    modal.id = 'mailtrace-modal';
    modal.innerHTML =
      '<div style="position:relative;width:420px;height:510px;border-radius:28px;background:radial-gradient(120% 120% at 50% 10%,rgba(255,255,255,.07),rgba(255,255,255,.02));box-shadow:0 14px 40px rgba(0,0,0,.45),inset 0 0 0 1px rgba(255,255,255,.08);display:flex;align-items:center;justify-content:center;overflow:hidden">' +
      '  <button aria-label="Close" onclick="MailTraceLoader.hide()" style="position:absolute;top:10px;right:10px;background:transparent;border:none;color:#cbd5e1;font-size:18px;cursor:pointer;padding:6px;border-radius:10px">✕</button>' +
      '  <div class="scene">' +
      '    <svg viewBox="0 0 240 200" width="280" height="230" aria-label="Envelope back">' +
      '      <rect x="20" y="36" rx="14" ry="14" width="200" height="128" fill="#ffffff"/>' +
      '      <rect x="24" y="40" rx="12" ry="12" width="192" height="120" fill="#f5f9fc"/>' +
      '      <path d="M20,52 L120,116 L220,52 L220,36 C220,28.268 213.732,22 206,22 L34,22 C26.268,22 20,28.268 20,36 Z" fill="#dde8f2"/>' +
      '      <path d="M20,52 L120,116 L220,52" fill="none" stroke="#c8d9e7" stroke-width="2"/>' +
      '    </svg>' +
      '    <div class="chart">' +
      '      <svg viewBox="0 0 240 200" width="280" height="230">' +
      '        <defs>' +
      '          <pattern id="gridM" width="20" height="20" patternUnits="userSpaceOnUse"><path d="M20 0 H0 V20" fill="none" stroke="rgba(0,0,0,0.06)" stroke-width="1"/></pattern>' +
      '          <linearGradient id="fillM" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="rgba(110,231,183,0.35)"/><stop offset="100%" stop-color="rgba(110,231,183,0)"/></linearGradient>' +
      '        </defs>' +
      '        <g id="mag"><g transform="translate(0,-12)">' +
      '          <rect x="24" y="40" width="192" height="120" fill="url(#gridM)"/>' +
      '          <path d="M32 150 H208" stroke="rgba(0,0,0,0.12)" stroke-width="1"/>' +
      '          <path d="M32 150 V56" stroke="rgba(0,0,0,0.12)" stroke-width="1"/>' +
      '          <path d="M32 150 L48 138 C56 132,64 126,72 128 S88 138,96 132 S112 108,120 112 S136 140,144 132 S160 98,168 100 S184 126,192 118 L208 118 L208 150 Z" fill="url(#fillM)"/>' +
      '          <path d="M32 150 L48 138 C56 132,64 126,72 128 S88 138,96 132 S112 108,120 112 S136 140,144 132 S160 98,168 100 S184 126,192 118 L208 118" fill="none" stroke="#6ee7b7" stroke-width="3" stroke-linecap="round"/>' +
      '        </g></g>' +
      '      </svg>' +
      '    </div>' +
      '    <svg class="scope" viewBox="0 0 120 120" aria-hidden="true">' +
      '      <defs><linearGradient id="rimM" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#1b6a3a"/><stop offset="100%" stop-color="#1db954"/></linearGradient><linearGradient id="handleM" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#16a34a"/><stop offset="100%" stop-color="#12843e"/></linearGradient></defs>' +
      '      <circle cx="54" cy="52" r="33" fill="none" stroke="url(#rimM)" stroke-width="6"/>' +
      '      <g transform="translate(54,52) rotate(40)"><rect x="31" y="-9" width="10" height="18" rx="5" fill="#0f7a3a"/><rect x="41" y="-7" width="42" height="14" rx="7" fill="url(#handleM)"/><rect x="43" y="-5" width="24" height="10" rx="5" fill="rgba(255,255,255,0.18)"/></g>' +
      '    </svg>' +
      '  </div>' +
      '  <div style="position:absolute;bottom:144px;left:32px;right:32px;color:#e8f1f8;text-align:center;font-weight:600">Analyzing run</div>' +
      '  <div style="position:absolute;bottom:118px;left:32px;right:32px;text-align:center;font-size:14px;color:#a8c3d9"><span>This may take a moment</span></div>' +
      '  <div style="position:absolute;left:32px;right:32px;bottom:48px">' +
      '    <div style="width:100%;height:10px;background:rgba(255,255,255,0.08);border-radius:999px;overflow:hidden;box-shadow:inset 0 0 0 1px rgba(255,255,255,.06)">' +
      '      <div id="mt-progress-fill" style="height:100%;width:0%;background:linear-gradient(90deg,#16a34a,#22c55e);border-radius:999px;transition:width .2s ease"></div>' +
      '    </div>' +
      '    <div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;color:#a8c3d9;font-size:12px">' +
      '      <span>Completed</span><span id="mt-progress-value" class="mt-strong">0%</span>' +
      '    </div>' +
      '  </div>' +
      '  <div id="mt-eta" class="mt-muted" style="position:absolute;bottom:20px;left:32px;right:32px;text-align:center">≈ <strong>--:--</strong> left</div>' +
      '</div>';

    document.body.appendChild(modal);
    fill  = document.getElementById('mt-progress-fill');
    val   = document.getElementById('mt-progress-value');
    etaEl = document.getElementById('mt-eta');
    scene = modal.querySelector('.scene');
    scope = modal.querySelector('.scope');
    chart = modal.querySelector('.chart');
  }

  window.MailTraceLoader = {
    show: function (opts) {
      ensureDOM();
      var o = opts || {};
      setPct(o.progress || 0);
      setETA(o.etaSeconds);
      modal.style.display = 'flex';
    },
    hide: function () {
      if (modal) modal.style.display = 'none';
    },
    setProgress: function (n) { ensureDOM(); setPct(n); },
    setETA: function (sec) { ensureDOM(); setETA(sec); }
  };
})();