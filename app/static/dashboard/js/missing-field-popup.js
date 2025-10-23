// app\static\dashboard\js\missing-field-popup.js
(function () {
  function ensurePopup() {
    if (document.getElementById("mtPopupOverlay")) return;

    const c = document.createElement('div');
    c.innerHTML = `
    <!-- v9 popup -->
    <div id="mtPopupOverlay" aria-hidden="true">
      <div id="mtPopup" role="alertdialog" aria-modal="true" aria-labelledby="mtPopupTitle">
        <header>
          <h3 id="mtPopupTitle">Missing required mappings</h3>
          <button class="btn" id="mtPopupClose" title="Close">Close</button>
        </header>
        <div class="body">
          <p>We couldn’t proceed because the following required fields aren’t mapped yet:</p>
          <ul id="mtPopupList"></ul>
        </div>
        <footer>
          <button class="btn primary" id="mtPopupOk">OK—take me back</button>
        </footer>
      </div>
    </div>
    `;
    document.body.appendChild(c.firstElementChild);
  }

  function showPopup(items) {
    ensurePopup();
    const ov = document.getElementById("mtPopupOverlay");
    const ul = document.getElementById("mtPopupList");
    ul.innerHTML = (items || []).map(x => `<li>${x}</li>`).join("");
    ov.style.display = "flex";
    ov.setAttribute("aria-hidden", "false");
    const close = () => { ov.style.display = "none"; ov.setAttribute("aria-hidden", "true"); };
    document.getElementById("mtPopupClose").onclick = close;
    document.getElementById("mtPopupOk").onclick = close;
  }

  // Expose the function for external use if needed
  window.showMissingFieldPopup = showPopup;
})();