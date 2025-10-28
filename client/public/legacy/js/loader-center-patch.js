// Ensure loader modal is centered after show()
(function () {
  if (window.__MT_CenterPatch) return; window.__MT_CenterPatch = true;

  function centerModal() {
    try {
      var m = document.getElementById('mailtrace-modal');
      if (!m) return;
      m.style.position = 'fixed';
      m.style.left = '0';
      m.style.top = '0';
      m.style.right = '0';
      m.style.bottom = '0';
      m.style.display = 'flex';
      m.style.alignItems = 'center';
      m.style.justifyContent = 'center';
      m.style.margin = '0';
      if (!m.style.transform) m.style.transform = 'none';
    } catch (e) {}
  }

  function install() {
    try {
      if (window.MailTraceLoader && typeof MailTraceLoader.show === 'function' && !MailTraceLoader.show.__mtCenterWrapped) {
        var _show = MailTraceLoader.show.bind(MailTraceLoader);
        MailTraceLoader.show = function (opts) {
          var r = _show(opts || {});
          requestAnimationFrame(centerModal);
          return r;
        };
        MailTraceLoader.show.__mtCenterWrapped = true;
      } else {
        setTimeout(install, 100);
      }
    } catch (e) {}
  }
  install();
})();