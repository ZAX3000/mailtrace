// /static/dashboard/js/mtMapper_v10_enforce.js
(function () {
  // Ensure popup exists
  function ensurePopup() {
    if (document.getElementById("mtPopupOverlay")) return;
    const head = document.head || document.getElementsByTagName('head')[0];
    if (!document.querySelector('style[data-mtpopup]')) {
      const st = document.createElement('style');
      st.setAttribute('data-mtpopup', '1');
      st.textContent = `
        #mtPopupOverlay{position:fixed; inset:0; background:rgba(0,0,0,.55); display:none; align-items:center; justify-content:center; z-index:10000;}
        #mtPopup{background:#111827; color:#f9fafb; border:1px solid rgba(255,255,255,.12); border-radius:16px; width:min(620px, 92vw); box-shadow:0 20px 70px rgba(0,0,0,.45)}
        #mtPopup header{display:flex; align-items:center; justify-content:space-between; padding:14px 18px; border-bottom:1px solid rgba(255,255,255,.08)}
        #mtPopup header h3{margin:0; font-size:18px}
        #mtPopup .body{padding:16px 18px}
        #mtPopup .body ul{margin:.25rem 0 .5rem 1rem; padding:0}
        #mtPopup .body li{margin:.25rem 0; font-size:14px}
        #mtPopup footer{display:flex; gap:10px; justify-content:flex-end; padding:14px 18px; border-top:1px solid rgba(255,255,255,.08)}
        #mtPopup .btn{background:#1f2937; color:#e5eef9; border:1px solid rgba(255,255,255,.12); border-radius:12px; padding:.55rem 1rem; font-weight:700; cursor:pointer}
        #mtPopup .btn.primary{background:#dc2626; border-color:transparent; color:#fff}
        .mt-missing { outline:2px solid #dc2626 !important; box-shadow:0 0 0 3px rgba(220,38,38,.3); }
        @keyframes mt-shake { 10%, 90% { transform: translateX(-1px);} 20%, 80% { transform: translateX(2px);} 30%, 50%, 70% { transform: translateX(-4px);} 40%, 60% { transform: translateX(4px);} }
        .mt-shake { animation: mt-shake .45s ease-in-out both; }
      `;
      head.appendChild(st);
    }

    const c = document.createElement('div');
    c.innerHTML = `
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

  function showPopup(list) {
    ensurePopup();
    const ov = document.getElementById("mtPopupOverlay");
    const ul = document.getElementById("mtPopupList");
    ul.innerHTML = (list || []).map(x => `<li>${x}</li>`).join("");
    ov.style.display = "flex"; ov.setAttribute("aria-hidden", "false");
    const close = () => { ov.style.display = "none"; ov.setAttribute("aria-hidden", "true"); };
    document.getElementById("mtPopupClose").onclick = close;
    document.getElementById("mtPopupOk").onclick = close;
  }

  function labelize(k) {
    return k.replace("_", " ").replace(/\b\w/g, c => c.toUpperCase());
  }

  function findMissing() {
    const reqMail = ["address1", "address2", "city", "state", "zip", "mail_date"];
    const reqCRM = ["address1", "address2", "city", "state", "zip", "crm_date", "amount"];
    const out = [];
    const pairs = [];
    function scan(side, req) {
      const container = side === "mail" ? document.querySelector("#mtMapMail .rows") : document.querySelector("#mtMapCRM .rows");
      if (!container) return;
      const rows = Array.from(container.querySelectorAll(".row"));
      for (const key of req) {
        let sel = null;
        for (const r of rows) {
          const lab = r.querySelector("label");
          const s = r.querySelector("select");
          if (!lab || !s) continue;
          const txt = (lab.textContent || "").toLowerCase();
          const target = key.replace("_", " ");
          if (txt.includes(target)) {
            sel = s; break;
          }
        }
        if (sel && !sel.value) {
          out.push((side === "mail" ? "Mail" : "CRM") + " → " + labelize(key));
          pairs.push(sel);
        }
      }
      return { out, pairs };
    }
    scan("mail", reqMail); scan("crm", reqCRM);
    return { list: out, selects: pairs };
  }

  function highlight(selects) {
    if (!selects || !selects.length) return;
    selects.forEach((sel, idx) => {
      sel.classList.remove("mt-missing", "mt-shake");
      void sel.offsetWidth;
      sel.classList.add("mt-missing", "mt-shake");
      if (idx === 0) {
        sel.scrollIntoView({ behavior: "smooth", block: "center" });
        sel.focus({ preventScroll: true });
      }
    });
  }

  // Capture click on Apply to pre-empt any existing handler
  function onClickCapture(ev) {
    const tgt = ev.target.closest && ev.target.closest("#mtMapperApply");
    if (!tgt) return;
    const miss = findMissing();
    if (miss.list.length) {
      ev.preventDefault();
      ev.stopImmediatePropagation();
      ev.stopPropagation();
      // Clear any subtle top message if present
      const help = document.getElementById("mtMapperHelp");
      if (help) help.textContent = " ";
      highlight(miss.selects);
      
      // Call the showPopup function from missing-field-popup.js
      showMissingFieldsPopup(miss.list);
      return false;
    }
  }

  document.addEventListener("click", onClickCapture, true); // CAPTURE phase

  // Re-attach when the mapper opens (for safety)
  const obs = new MutationObserver(() => {
    const apply = document.getElementById("mtMapperApply");
    if (apply && !apply.__mtV10Bound) {
      apply.__mtV10Bound = true;
    }
  });

  document.addEventListener("DOMContentLoaded", () => {
    try { obs.observe(document.body, { childList: true, subtree: true }); } catch { }
  });
})();