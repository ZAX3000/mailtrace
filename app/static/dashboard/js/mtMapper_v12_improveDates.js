// /static/dashboard/js/mtMapper_v12_improveDates.js
(function () {
  // Patch smartSuggest if present; otherwise define a focused version used by the modal
  function install() {
    const US_STATES = new Set(["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY"]);
    const DATE_MIN = new Date(2000, 0, 1).getTime();
    const DATE_MAX = new Date(2100, 0, 1).getTime();

    const isName = s => /^[A-Za-z .'\-]{2,}$/.test(s || "");
    const isAddress1 = s => !!s && s.length >= 5 && /\d/.test(s) && /[A-Za-z]/.test(s);
    const isAddress2 = s => !!s && (/apt|unit|ste|suite|#|fl|floor|bldg|building|lot|trlr|trailer|rm|room/i.test(s) || s.length >= 2);
    const isCity = s => /^[A-Za-z .'\-]{2,}$/.test(s || "");
    const isState = s => { const t = (s || "").trim(); return US_STATES.has(t.toUpperCase()) || /^[A-Za-z .'\-]{3,}$/.test(t); };
    const isZip = s => /^\d{5}(-\d{4})?$/.test(String(s || "").trim());
    const isDate = s => { const t = Date.parse(s); return !Number.isNaN(t) && t >= DATE_MIN && t <= DATE_MAX; };
    const isAmount = s => { const t = String(s ?? "").replace(/[$,]/g, "").trim(); if (!t) return false; const n = Number(t); return Number.isFinite(n); };

    function sampleScore(rows, col, check, cap = 200) {
      let ok = 0, tot = 0;
      for (const r of rows) {
        const v = (r[col] ?? "").toString().trim();
        if (v === "") continue;
        tot++; if (check(v)) ok++;
        if (tot >= cap) break;
      }
      return tot ? (ok / tot) : 0;
    }

    function normalize(s) { return (s || "").toLowerCase().replace(/[\s_\-]/g, ""); }
    function containsToken(name, tokens) {
      const low = (name || "").toLowerCase();
      return tokens.some(t => low.includes(t));
    }

    // Expose for debugging if needed
    window.__mt_isDate = isDate;

    function smartSuggestLimited(headers, rows, SYN, wantKeys) {
      const CHECKERS = {
        first_name: isName, last_name: isName, address1: isAddress1, address2: isAddress2,
        city: isCity, state: isState, zip: isZip, mail_date: isDate, crm_date: isDate, amount: isAmount
      };
      const normalizedSyn = {};
      for (const [k, arr] of Object.entries(SYN)) {
        normalizedSyn[k] = (arr || []).map(a => normalize(a));
      }
      const picks = {};
      const used = new Set();
      for (const key of wantKeys) {
        const checker = CHECKERS[key];
        let best = { col: "", score: -1, dataScore: 0, headerScore: 0 };
        for (const col of headers) {
          if (used.has(col)) continue;
          const colNorm = normalize(col);
          const syns = normalizedSyn[key] || [];
          const exact = syns.includes(colNorm) ? 1 : 0;
          const partial = (!exact && containsToken(col, syns)) ? 1 : 0;
          const headerScore = exact ? 0.7 : (partial ? 0.35 : 0);
          const dataScore = sampleScore(rows, col, checker);
          // Increase weight on data for date fields
          const weight = (key === "crm_date" || key === "mail_date") ? 0.9 : 0.8;
          const total = headerScore + (weight * dataScore);
          // Keep best by total score; tie-break by higher dataScore
          if (total > best.score || (total === best.score && dataScore > best.dataScore)) {
            best = { col, score: total, dataScore, headerScore };
          }
        }
        // thresholds
        const minData = (key === "address2") ? 0.15 : (key === "amount" ? 0.35 : (key === "crm_date" || key === "mail_date" ? 0.3 : 0.4));
        if (best.col && (best.dataScore >= minData || best.headerScore >= 0.7)) {
          picks[key] = best.col;
          used.add(best.col);
        }
      }
      return picks;
    }

    // Hook into existing openColumnMapper && replace its suggestion phase if we can find it
    const oldOpen = window.openColumnMapper;
    if (typeof oldOpen === "function") {
      window.openColumnMapper = async function (payload) {
        // if our sentinel is present, skip (already using v8 smart)
        // We will wrap after the UI is built to apply our limited smart suggestions.
        const res = await oldOpen(payload);
        return res;
      };
    }

    // Provide a helper other parts of the script can call when building the modal
    window.__mt_smartSuggestLimited = smartSuggestLimited;
  }

  try { install(); } catch (e) { console.error("v12 install failed", e); }
})();