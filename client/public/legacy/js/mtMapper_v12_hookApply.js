// /static/dashboard/js/mtMapper_v12_hookApply.js
(function () {
  function applyLimitedSuggestions() {
    if (!window.__mt_smartSuggestLimited) return;
    const overlay = document.getElementById("mtMapperOverlay");
    if (!overlay || overlay.style.display === "none") return;

    // Collect headers from selects
    const mailHeaders = Array.from(document.querySelectorAll("#mtMapMail .row select option")).map(o => o.value).filter(Boolean);
    const crmHeaders = Array.from(document.querySelectorAll("#mtMapCRM .row select option")).map(o => o.value).filter(Boolean);

    // Build sample rows from visible options (cached state if available)
    const st = window.MailTraceUI && window.MailTraceUI.state;
    const mailRows = (st && st.mail && st.mail.rows) ? st.mail.rows.slice(0, 200) : [];
    const crmRows = (st && st.crm && st.crm.rows) ? st.crm.rows.slice(0, 200) : [];

    const SYN = {
      first_name: ["first name", "firstname", "fname", "given name", "givenname", "first"],
      last_name: ["last name", "lastname", "lname", "surname", "family name", "familyname", "last"],
      address1: ["address 1", "address1", "addr1", "street", "street address", "primarylocation", "linea", "line 1", "address line 1"],
      address2: ["address 2", "address2", "addr2", "line2", "secondarylocation", "apartment", "apt", "unit", "suite", "ste", "#"],
      city: ["city", "cityname", "town", "locality"],
      state: ["state", "stateprov", "region", "province", "st", "state code"],
      zip: ["zip", "zipcode", "postal", "postalcode"],
      mail_date: ["mailed", "maileddate", "mailedon", "mail date", "mailed on", "postmark", "postmarkdate", "postmark_dt", "postmarkdt", "post_date"],
      crm_date: ["crm", "crmdate", "date", "created", "updated", "last activity", "activity date", "closed", "closedate", "closed_dt", "close date", "closed date", "dt"],
      amount: ["amount", "amountusd", "value", "total", "sum", "revenue", "donation", "paid", "invoice", "invoice_total"]
    };

    const wantMail = ["first_name", "last_name", "address1", "address2", "city", "state", "zip", "mail_date"];
    const wantCRM = ["first_name", "last_name", "address1", "address2", "city", "state", "zip", "crm_date", "amount"];

    const suggMail = window.__mt_smartSuggestLimited(mailHeaders, mailRows, SYN, wantMail);
    const suggCRM = window.__mt_smartSuggestLimited(crmHeaders, crmRows, SYN, wantCRM);

    // Apply to selects by label matching
    function apply(side, sugg) {
      const labels = Array.from(document.querySelectorAll(side === "mail" ? "#mtMapMail .row label" : "#mtMapCRM .row label")).map(el => el.textContent.toLowerCase());
      const selects = Array.from(document.querySelectorAll(side === "mail" ? "#mtMapMail .row select" : "#mtMapCRM .row select"));

      function setFor(key, val) {
        // find row index by key
        const pretty = key.replace("_", " ");
        for (let i = 0; i < labels.length; i++) {
          if (labels[i].includes(pretty)) {
            const sel = selects[i];
            if (sel && Array.from(sel.options).some(o => o.value === val)) {
              sel.value = val;
              sel.dispatchEvent(new Event("change"));
            }
            break;
          }
        }
      }
      for (const [k, v] of Object.entries(sugg || {})) setFor(k, v);
    }
    apply("mail", suggMail);
    apply("crm", suggCRM);
  }

  // Run after the modal opens (observe overlay visibility changes)
  const obs = new MutationObserver(() => {
    const ov = document.getElementById("mtMapperOverlay");
    if (ov && ov.style.display !== "none") {
      setTimeout(applyLimitedSuggestions, 50);
    }
  });
  document.addEventListener("DOMContentLoaded", () => {
    try { obs.observe(document.body, { attributes: true, childList: true, subtree: true }); } catch { }
  });
})();