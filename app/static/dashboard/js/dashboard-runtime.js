// /static/dashboard/js/dashboard-runtime.js
(() => {
    'use strict';

    // ---- Safety shims (in case other bundles load later) ----
    if (typeof window.ColumnMappingError === 'undefined') {
    class ColumnMappingError extends Error {
        constructor(payload) { super('ColumnMappingError'); this.name = 'ColumnMappingError'; this.__isMappingError = true; this.payload = payload; }
    }
    window.ColumnMappingError = ColumnMappingError;
    }

    // Keep a predictable namespace available early
    window.MailTraceUI ||= { state: null, render: {} };

    function showError(msg) { const b = document.getElementById('err'); const m = document.getElementById('errmsg'); b.style.display = 'block'; m.textContent = msg; }
    function parseCSV(text) {
      const rows = []; let i = 0, field = '', row = [], inQ = false;
      while (i < text.length) {
        const c = text[i];
        if (inQ) {
          if (c === '"' && text[i + 1] === '"') { field += '"'; i += 2; continue; }
          if (c === '"') { inQ = false; i++; continue; }
          field += c; i++; continue;
        } else {
          if (c === '"') { inQ = true; i++; continue; }
          if (c === ',') { row.push(field); field = ''; i++; continue; }
          if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
          if (c === '\r') { i++; continue; }
          field += c; i++; continue;
        }
      }
      row.push(field); rows.push(row);
      const header = rows.shift().map(h => h.trim());
      const out = rows.filter(r => r.some(x => x && x.trim() !== '')).map(r => { const o = {}; for (let j = 0; j < header.length; j++) { o[header[j]] = (r[j] ?? '').trim(); } return o; });
      return { header, rows: out };
    }
    function nz(x) { return (x || '').trim(); }
    function stripUnit(addr) { return (nz(addr)).replace(/\b(?:unit|apt|apartment|suite|ste|#)\s*[A-Za-z0-9\-]+\b/ig, ''); }
    const DIRS = { "north": "n", "south": "s", "east": "e", "west": "w", "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw" };
    const SUF = {
      "street": "st", "st.": "st", "avenue": "ave", "ave.": "ave", "road": "rd", "rd.": "rd", "boulevard": "blvd", "blvd.": "blvd",
      "parkway": "pkwy", "pkwy.": "pkwy", "place": "pl", "pl.": "pl", "lane": "ln", "ln.": "ln", "drive": "dr", "dr.": "dr",
      "court": "ct", "ct.": "ct", "highway": "hwy", "hwy.": "hwy", "circle": "cir", "cir.": "cir", "terrace": "ter", "ter.": "ter", "park": "park"
    };
    function normStreet(a1, a2) {
      let s = (nz(a1) + ' ' + nz(a2)).trim();
      s = stripUnit(s); s = s.replace(/\./g, ' ').replace(/[^A-Za-z0-9\s\-]/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
      return s.split(' ').filter(Boolean).map(t => SUF[DIRS[t] || t] || (DIRS[t] || t)).join(' ');
    }
    function normStreetCollapsed(a1, a2) { return normStreet(a1, a2).replace(/\s+/g, ''); }
    function normCity(c) { return nz(c).replace(/\s+/g, ' ').trim().replace(/\b\w/g, ch => ch.toUpperCase()); }
    function zip5(z) { return (nz(z).match(/\d/g) || []).join('').slice(0, 5); }
    function extractUnit(a1, a2) { const s = (nz(a1) + ' ' + nz(a2)).trim(); const m = s.match(/\b(?:unit|apt|apartment|suite|ste|#)\s*([A-Za-z0-9\-]+)\b/i); return m ? m[1].toUpperCase() : ''; }
    function normKey(a1, a2, city, state, zip) { return [normStreet(a1, a2), normCity(city), nz(state).toUpperCase(), zip5(zip)].join('|'); }
    function normKeyCollapsed(a1, a2, city, state, zip) { return [normStreetCollapsed(a1, a2), normCity(city), nz(state).toUpperCase(), zip5(zip)].join('|'); }

    // Month and Date helpers
    function monthKey(s) {
      s = nz(s);
      let m = s.match(/^(\d{2})-(\d{2})-(\d{2,4})$/);
      if (m) { const mm = m[1], yy = (m[3].length === 2 ? '20' + m[3] : m[3]); return `${yy}-${mm}`; }
      m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (m) { return `${m[1]}-${m[2]}`; }
      return null;
    }
    function dateKey(s) {
      s = (s || "").trim();
      let m = s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{2,4})$/);
      if (m) {
        let mm = parseInt(m[1], 10), dd = parseInt(m[2], 10), yy = parseInt(m[3], 10);
        if (yy < 100) yy = 2000 + yy;
        return yy * 10000 + mm * 100 + dd;
      }
      m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
      if (m) { return parseInt(m[1], 10) * 10000 + parseInt(m[2], 10) * 100 + parseInt(m[3], 10); }
      const t = Date.parse(s);
      if (!isNaN(t)) { const d = new Date(t); return d.getFullYear() * 10000 + (d.getMonth() + 1) * 100 + d.getDate(); }
      return Number.MAX_SAFE_INTEGER;
    }

    // Display helpers
    function pad2(n) { return String(n).padStart(2, '0'); }
    function parseDateParts(s) {
      s = (s || "").trim();
      let m = s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{2,4})$/);
      if (m) { let mm = parseInt(m[1], 10), dd = parseInt(m[2], 10), yy = parseInt(m[3], 10); if (yy < 100) yy = 2000 + yy; return { y: yy, m: mm, d: dd }; }
      m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/]?(\d{1,2})$/);
      if (m) { return { y: parseInt(m[1], 10), m: parseInt(m[2], 10), d: parseInt(m[3], 10) }; }
      const t = Date.parse(s);
      if (!isNaN(t)) { const d = new Date(t); return { y: d.getFullYear(), m: d.getMonth() + 1, d: d.getDate() }; }
      return null;
    }
    function fmtDate(s) { const p = parseDateParts(s); return p ? `${pad2(p.m)}-${pad2(p.d)}-${p.y}` : (s || "—"); }
    function fmtDates(arr) { if (!arr || !arr.length) return "—"; return arr.map(fmtDate).join(", "); }
    function fmtAmount(s) {
      const t = (s || "").replace(/[^0-9.\-]/g, ""); if (!t) return "—";
      const v = Number(t); if (!isFinite(v)) return s;
      try { return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(v); }
      catch (e) { return "$" + v.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ","); }
    }

    // Fuzzy helpers + config (toggle/strictness controls)
    function houseNumberFrom(normStreetStr) { const toks = (normStreetStr || "").trim().split(/\s+/); const first = toks[0] || ""; return /^\d+$/.test(first) ? first : ""; }
    function streetNameOnly(normStreetStr) { const toks = (normStreetStr || "").trim().split(/\s+/); if (toks.length && /^\d+$/.test(toks[0])) toks.shift(); return toks.join(" "); }
    function collapseLetters(s) { return (s || "").replace(/\s+/g, ""); }
    function editDistance(a, b) {
      a = String(a || ""); b = String(b || ""); const n = a.length, m = b.length; if (!n) return m; if (!m) return n;
      const dp = new Array(n + 1); for (let i = 0; i <= n; i++) { dp[i] = new Array(m + 1); dp[i][0] = i; } for (let j = 0; j <= m; j++) { dp[0][j] = j; }
      for (let i = 1; i <= n; i++) { const ai = a.charCodeAt(i - 1); for (let j = 1; j <= m; j++) { const cost = (ai === b.charCodeAt(j - 1)) ? 0 : 1; dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost); } } return dp[n][m];
    }
    function simRatio(a, b) { const d = editDistance(a, b); const L = Math.max(a.length, b.length) || 1; return 1 - d / L; }
    function getFuzzyConfig() {
      const enabled = document.getElementById('fuzzyToggle')?.checked;
      const mode = document.getElementById('fuzzyMode')?.value || 'standard';
      let maxEdits = 2, minSim = 0.85, base = 95, perEdit = 4, floor = 60;
      if (mode === 'strict') { maxEdits = 1; minSim = 0.90; base = 96; perEdit = 5; floor = 70; }
      else if (mode === 'loose') { maxEdits = 3; minSim = 0.80; base = 94; perEdit = 3; floor = 55; }
      return { enabled, maxEdits, minSim, base, perEdit, floor };
    }

    // ========================= State computation =========================
    function computeState(mailText, crmText) {
      const mail = parseCSV(mailText);
      const crm = parseCSV(crmText);
      const SYN = {
        address1: ["address 1", "address1", "street", "street address", "addr1"],
        address2: ["address 2", "address2", "apt", "unit", "suite", "addr2"],
        city: ["city", "town"],
        state: ["state", "province", "st"],
        zip: ["zip", "zipcode", "postal_code", "postcode", "zip code"],
        mail_date: ["mail date", "sent_date", "date sent", "date"],
        crm_date: ["job date", "crm date", "date"],
        amount: ["job value", "amount", "crm amount", "value", "invoice amount"]
      };
      const pick = (cols, ...keys) => { const lower = cols.map(c => c.toLowerCase()); for (const k of keys) { const i = lower.indexOf(k); if (i !== -1) return cols[i]; } return null; };


      const mapping = arguments[2] || (function () { try { return JSON.parse(localStorage.getItem("mt_column_mapping_v1") || "{}"); } catch { return {}; } })();

      function pickMap(side, key, autoPick) {
        const v = (mapping?.[side]?.[key]) || "";
        return v && (side === "mail" ? mail.header.includes(v) : crm.header.includes(v)) ? v : autoPick;
      }

      const M_ADDR1 = pickMap('mail', 'address1', pick(mail.header, ...SYN.address1));
      const M_ADDR2 = pickMap('mail', 'address2', pick(mail.header, ...SYN.address2));
      const M_CITY = pickMap('mail', 'city', pick(mail.header, ...SYN.city));
      const M_STATE = pickMap('mail', 'state', pick(mail.header, ...SYN.state));
      const M_ZIP = pickMap('mail', 'zip', pick(mail.header, ...SYN.zip));
      const M_DATE = pickMap('mail', 'mail_date', pick(mail.header, ...SYN.mail_date));

      const C_ADDR1 = pickMap('crm', 'address1', pick(crm.header, ...SYN.address1));
      const C_ADDR2 = pickMap('crm', 'address2', pick(crm.header, ...SYN.address2));
      const C_CITY = pickMap('crm', 'city', pick(crm.header, ...SYN.city));
      const C_STATE = pickMap('crm', 'state', pick(crm.header, ...SYN.state));
      const C_ZIP = pickMap('crm', 'zip', pick(crm.header, ...SYN.zip));
      const C_DATE = pickMap('crm', 'crm_date', pick(crm.header, ...SYN.crm_date));
      const C_AMT = pickMap('crm', 'amount', pick(crm.header, ...SYN.amount));

      if (!M_ADDR1 || !C_ADDR1) {
        const payload = {
          mailHeaders: mail.header,
          crmHeaders: crm.header,
          mailRows: mail.rows.slice(0, 200),
          crmRows: crm.rows.slice(0, 200),
          SYN,
          auto: {
            mail: { address1: M_ADDR1, address2: M_ADDR2, city: M_CITY, state: M_STATE, zip: M_ZIP, mail_date: M_DATE },
            crm: { address1: C_ADDR1, address2: C_ADDR2, city: C_CITY, state: C_STATE, zip: C_ZIP, crm_date: C_DATE, amount: C_AMT }
          }
        };
        throw new ColumnMappingError(payload);
      }


      const mailExt = mail.rows.map(r => ({ ...r, _key: normKey(r[M_ADDR1], r[M_ADDR2], r[M_CITY], r[M_STATE], r[M_ZIP]), _unit: extractUnit(r[M_ADDR1], r[M_ADDR2]) }));
      const crmExt = crm.rows.map(r => ({ ...r, _key: normKey(r[C_ADDR1], r[C_ADDR2], r[C_CITY], r[C_STATE], r[C_ZIP]), _unit: extractUnit(r[C_ADDR1], r[C_ADDR2]) }));

      const byKey = {}; const byKeyCollapsed = {}; const byZipState = {};
      for (const r of mailExt) {
        (byKey[r._key] ||= []).push(r);
        const ck = normKeyCollapsed(r[M_ADDR1], r[M_ADDR2], r[M_CITY], r[M_STATE], r[M_ZIP]);
        (byKeyCollapsed[ck] ||= []).push(r);
        const zsk = zip5(r[M_ZIP]) + "|" + nz(r[M_STATE]).toUpperCase();
        (byZipState[zsk] ||= []).push(r);
      }

      const md = {}; if (M_DATE) { for (const r of mailExt) { const d = nz(r[M_DATE]); if (!d) continue; (md[r._key] ||= new Set()).add(d); } }

      function streetTokenDiffs(crmA1, mailA1) {
        const ctoks = nz(crmA1).split(/\s+/), mtoks = nz(mailA1).split(/\s+/);
        const n = Math.min(ctoks.length, mtoks.length);
        const seen = new Set(); const diffs = [];
        function tokKind(tok) {
          const t = (tok || '').replace(/\W+/g, '').toLowerCase();
          if (/^\d+$/.test(t)) return ["num", t];
          const dirMap = { "n": ["n", "north"], "s": ["s", "south"], "e": ["e", "east"], "w": ["w", "west"], "ne": ["ne", "northeast"], "nw": ["nw", "northwest"], "se": ["se", "southeast"], "sw": ["sw", "southwest"] };
          const sufMap = { "st": ["st", "street"], "ave": ["ave", "avenue"], "rd": ["rd", "road"], "blvd": ["blvd", "boulevard"], "pkwy": ["pkwy", "parkway"], "pl": ["pl", "place"], "ln": ["ln", "lane"], "dr": ["dr", "drive"], "ct": ["ct", "court"], "hwy": ["hwy", "highway"], "cir": ["cir", "circle"], "ter": ["ter", "terrace"] };
          for (const k in dirMap) { if (dirMap[k].includes(t)) return ["dir", k]; }
          for (const k in sufMap) { if (sufMap[k].includes(t)) return ["suf", k]; }
          return ["txt", t];
        }
        function ed(a, b) {
          a = String(a || ""); b = String(b || ""); const n = a.length, m = b.length; if (!n) return m; if (!m) return n;
          const dp = new Array(n + 1); for (let i = 0; i <= n; i++) { dp[i] = new Array(m + 1); dp[i][0] = i; } for (let j = 0; j <= m; j++) { dp[0][j] = j; }
          for (let i = 1; i <= n; i++) {
            for (let j = 1; j <= m; j++) {
              const cost = (a.charCodeAt(i - 1) === b.charCodeAt(j - 1)) ? 0 : 1;
              dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
            }
          } return dp[n][m];
        }
        for (let i = 0; i < n; i++) {
          const c = ctoks[i], m = mtoks[i]; if (!c || !m || c.toLowerCase() === m.toLowerCase()) continue;
          const [ck, cn] = tokKind(c), [mk, mn] = tokKind(m);
          // Case A: Same canonical kind+value (e.g., "Street" vs "St.")
          if (ck === mk && cn === mn && ck !== "num") {
            const note = (c.length <= m.length ? c : m) + " vs " + (c.length <= m.length ? m : c);
            const key = note.toLowerCase(); if (!seen.has(key)) { seen.add(key); diffs.push(note); }
            continue;
          }
          // Case B: Slight spelling mismatch on word tokens (exclude nums/dirs/suffixes)
          if (ck === "txt" && mk === "txt") {
            const d = ed(cn, mn), L = Math.max(cn.length, mn.length) || 1;
            const near = (L >= 4 && d <= 2) || (L <= 3 && d <= 1);
            if (near) {
              const note = (c.length <= m.length ? c : m) + " vs " + (c.length <= m.length ? m : c);
              const key = note.toLowerCase(); if (!seen.has(key)) { seen.add(key); diffs.push(note); }
            }
          }
        }
        return diffs.slice(0, 2);
      }

      const matches = [];
      const FZ = getFuzzyConfig();

      for (const cr of crmExt) {
        const key = cr._key;
        const cu = nz(cr._unit);
        const key2 = normKeyCollapsed(cr[C_ADDR1], cr[C_ADDR2], cr[C_CITY], cr[C_STATE], cr[C_ZIP]);
        let mrows = Array.from(new Set([...(byKey[key] || []), ...(byKeyCollapsed[key2] || [])]));
        if (!mrows.length && FZ.enabled) { // fuzzy fallback
          const zsk = zip5(cr[C_ZIP]) + "|" + nz(cr[C_STATE]).toUpperCase();
          const cand = (byZipState[zsk] || []);
          if (cand.length) {
            const crmNS = normStreet(cr[C_ADDR1], cr[C_ADDR2]);
            const crmNum = houseNumberFrom(crmNS);
            const crmName = collapseLetters(streetNameOnly(crmNS));
            let bestRow = null, bestDist = 1e9, bestSim = 0;
            for (const r of cand) {
              const rNS = normStreet(r[M_ADDR1], r[M_ADDR2]);
              if (houseNumberFrom(rNS) !== crmNum) continue;
              const rName = collapseLetters(streetNameOnly(rNS));
              const d = editDistance(crmName, rName);
              const sim = simRatio(crmName, rName);
              if (d <= FZ.maxEdits || sim >= FZ.minSim) { if (d < bestDist) { bestDist = d; bestSim = sim; bestRow = r; } }
            }
            if (bestRow) mrows = [bestRow];
          }
        }
        if (!mrows.length) continue;

        let mr = null, unit_case = "none", fuzzyUsed = false;
        if (mrows.length === 1 && mrows[0]._key !== key && mrows[0]._key !== key2) { fuzzyUsed = true; }
        const exact = cu ? mrows.filter(r => nz(r._unit) === cu) : [];
        if (cu && exact.length) { mr = exact[0]; unit_case = "exact"; }
        else {
          const bothUnitsDiff = mrows.filter(r => nz(r._unit) && cu && nz(r._unit) !== cu);
          if (bothUnitsDiff.length) { continue; }
          mr = mrows[0]; unit_case = (!!nz(mr._unit) ^ !!cu) ? "one_sided" : "none";
        }





        const streetDiffs = streetTokenDiffs(cr[C_ADDR1], mr[M_ADDR1]);
        const cityDiffs = streetTokenDiffs(cr[C_CITY], mr[M_CITY]);
        const stateDiffs = streetTokenDiffs(cr[C_STATE], mr[M_STATE]);
        const zipDiffs = streetTokenDiffs(String(cr[C_ZIP] || '').slice(0, 5), String(mr[M_ZIP] || '').slice(0, 5));

        // Merge & dedupe while preserving order; cap to 4 items to keep Notes readable
        const diffs = Array.from(new Set([...streetDiffs, ...cityDiffs, ...stateDiffs, ...zipDiffs])).slice(0, 4);

        // Raw City strings (to surface spacing/punctuation-only differences)
        const rawCityCRM = String(cr[C_CITY] || '');
        const rawCityMail = String(mr[M_CITY] || '');
        const rawCityDiff = (cityDiffs.length === 0 && rawCityCRM.trim().toLowerCase() !== rawCityMail.trim().toLowerCase());

        // ---------- Diff classification helpers (local to this block) ----------
        function splitPair(s) { const i = s.toLowerCase().indexOf(' vs '); if (i < 0) return [s, '']; return [s.slice(0, i).trim(), s.slice(i + 4).trim()]; }
        function normLetters(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); }
        function hasOnlyPunctSpaceDiff(a, b) { return normLetters(a) === normLetters(b) && a.toLowerCase() !== b.toLowerCase(); }

        const ABBREV = new Map([
          ['st', 'street'], ['st.', 'street'], ['ave', 'avenue'], ['ave.', 'avenue'], ['av', 'avenue'], ['av.', 'avenue'],
          ['blvd', 'boulevard'], ['blvd.', 'boulevard'], ['rd', 'road'], ['rd.', 'road'], ['dr', 'drive'], ['dr.', 'drive'],
          ['ln', 'lane'], ['ln.', 'lane'], ['ct', 'court'], ['ct.', 'court'], ['cir', 'circle'], ['cir.', 'circle'],
          ['trl', 'trail'], ['trl.', 'trail'], ['pkwy', 'parkway'], ['pkwy.', 'parkway'], ['hwy', 'highway'], ['hwy.', 'highway'],
          ['pl', 'place'], ['pl.', 'place'], ['mt', 'mount'], ['ft', 'fort'],
          ['n', 'north'], ['n.', 'north'], ['s', 'south'], ['s.', 'south'], ['e', 'east'], ['e.', 'east'], ['w', 'west'], ['w.', 'west'],
          ['saint', 'st'], ['st', 'saint'] // city variant both ways
        ]);
        function isAbbrevOrDirectional(a, b) {
          const A = a.toLowerCase(), B = b.toLowerCase();
          const aStripped = A.replace(/\./g, ''), bStripped = B.replace(/\./g, '');
          // Exact mapped pairs either way
          if (ABBREV.get(aStripped) === bStripped) return true;
          if (ABBREV.get(bStripped) === aStripped) return true;
          // One is prefix of the other with dot removed (e.g., blvd. vs boulevard)
          if (aStripped && bStripped && (aStripped === bStripped.slice(0, aStripped.length) || bStripped === aStripped.slice(0, bStripped.length))) return true;
          return false;
        }
        function levenshtein(a, b) {
          a = String(a || '').toLowerCase(); b = String(b || '').toLowerCase();
          const m = a.length, n = b.length; if (m === 0) return n; if (n === 0) return m;
          const dp = Array(n + 1); for (let j = 0; j <= n; j++) dp[j] = j;
          for (let i = 1; i <= m; i++) {
            let prev = dp[0]; dp[0] = i;
            for (let j = 1; j <= n; j++) {
              const temp = dp[j];
              dp[j] = Math.min(
                dp[j] + 1,           // deletion
                dp[j - 1] + 1,         // insertion
                prev + (a[i - 1] === b[j - 1] ? 0 : 1) // substitution
              );
              prev = temp;
            }
          }
          return dp[n];
        }
        function classifyDiff(s) {
          const [a, b] = splitPair(s);
          if (hasOnlyPunctSpaceDiff(a, b)) return 'punct';
          if (isAbbrevOrDirectional(a, b)) return 'abbrev';
          const ed = levenshtein(a, b);
          if (ed > 0 && ed <= 2) return 'typo';
          return 'other';
        }

        // Build Notes content
        const notesPieces = [...diffs];
        if (rawCityDiff) { notesPieces.push(`${rawCityMail} vs ${rawCityCRM}`); }

        let conf = 100;

        // Type-weighted penalty
        let nTypo = 0, nAbbrev = 0, nPunct = 0, nOther = 0;
        for (const s of notesPieces) {
          const c = classifyDiff(s);
          if (c === 'typo') nTypo++; else if (c === 'abbrev') nAbbrev++; else if (c === 'punct') nPunct++; else nOther++;
        }
        // Base penalties
        let diffPenalty = nTypo * 3 + nOther * 2 + (nAbbrev + nPunct) * 1;
        // Multi-typo escalation: extra -1% per typo beyond the first
        if (nTypo >= 2) { diffPenalty += (nTypo - 1); }
        // Cap total diff penalty at 12%
        diffPenalty = Math.min(diffPenalty, 12);
        conf -= diffPenalty;

        // One-sided unit mismatch penalty (-8%), additive
        if (unit_case === "one_sided") conf -= 8;

        // Apply fuzzy cap/floor if fuzzy was used
        if (fuzzyUsed && FZ.enabled) { conf = Math.min(conf, FZ.base); conf = Math.max(FZ.floor, conf); }
        // If only benign diffs (abbrev/punct) and no 'other' or 'typo', bypass fuzzy cap
        if (fuzzyUsed && nTypo === 0 && nOther === 0) {
          const unitPenalty = (unit_case === "one_sided") ? 8 : 0;
          const recompute = 100 - diffPenalty - unitPenalty;
          if (recompute > conf) conf = recompute;
        }

        // If no diffs (including raw city) and no one-sided unit, clamp back to 100
        if (notesPieces.length === 0 && unit_case !== "one_sided") { conf = 100; }

        let notesText = '';
        if (Math.round(conf) === 100) {
          notesText = 'perfect match';
        } else {
          notesText = notesPieces.join('; ');
          if (!notesText) notesText = 'difference detected';
        }

        const allMailDates = Array.from(md[mr._key] || []);



        const cdk = dateKey(cr[C_DATE]);
        const filtered = allMailDates.filter(d => dateKey(d) <= cdk).sort((a, b) => dateKey(a) - dateKey(b));

        matches.push({
          "Mail Address 1": nz(mr[M_ADDR1]), "Mail Unit": nz(mr._unit),
          "CRM Address 1": nz(cr[C_ADDR1]), "CRM Unit": cu,
          "City": nz(cr[C_CITY]), "State": nz(cr[C_STATE]), "ZIP": zip5(cr[C_ZIP]), "Mail City": nz(mr[M_CITY]), "Mail ZIP": zip5(mr[M_ZIP]),
          "Mail Dates": fmtDates(filtered),
          "CRM Date": fmtDate(nz(cr[C_DATE])), "Amount": fmtAmount(nz(cr[C_AMT])),
          "Confidence": (Math.round(conf) + "%"),
          "Notes": notesText
        });
      }

      // Top lists
      const cCity = new Map(), cZip = new Map();
      // Mail totals by City/ZIP (all mail rows)
      const mailCityTotals = new Map(), mailZipTotals = new Map();
      for (const r of mail.rows) {
        const city = nz(r[M_CITY]); if (city) mailCityTotals.set(city, (mailCityTotals.get(city) || 0) + 1);
        const zp = zip5(r[M_ZIP]); if (zp) mailZipTotals.set(zp, (mailZipTotals.get(zp) || 0) + 1);
      }
      for (const m of matches) { const mc = m["Mail City"], mz = m["Mail ZIP"]; if (mc) cCity.set(mc, (cCity.get(mc) || 0) + 1); if (mz) cZip.set(mz, (cZip.get(mz) || 0) + 1); }
      const topCities = Array.from(cCity.entries()).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([City, Count]) => ({ City, Count }));
      const topZips = Array.from(cZip.entries()).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([ZIP, Count]) => ({ ZIP, Count }));

      // Time series
      const monthsSet = new Set(); const mMonths = [], cMonths = [], mtMonths = [];
      for (const k in md) { for (const d of md[k] || []) { const mk = monthKey(d); if (mk) { mMonths.push(mk); monthsSet.add(mk); } } }
      for (const r of crmExt) { const mk = monthKey(r[C_DATE]); if (mk) { cMonths.push(mk); monthsSet.add(mk); } }
      for (const m of matches) { const mk = monthKey(m["CRM Date"]); if (mk) { mtMonths.push(mk); monthsSet.add(mk); } }
      const xLabs = Array.from(monthsSet).sort();
      function countSeries(list) { const map = new Map(); for (const k of list) { map.set(k, (map.get(k) || 0) + 1); } return xLabs.map(k => map.get(k) || 0); }
      const mSeries = countSeries(mMonths), cSeries = countSeries(cMonths), mtSeries = countSeries(mtMonths);
      function prevMonthKey(ym) { const [y, m] = ym.split('-').map(Number); return `${y - 1}-${String(m).padStart(2, '0')}`; }
      const mSeriesY = xLabs.map(k => mMonths.filter(x => x === prevMonthKey(k)).length);
      const cSeriesY = xLabs.map(k => cMonths.filter(x => x === prevMonthKey(k)).length);
      const mtSeriesY = xLabs.map(k => mtMonths.filter(x => x === prevMonthKey(k)).length);


      const __uniqMailCount = Object.keys(byKey).length;
      return { uniqueMailAddrs: __uniqMailCount, mailRows: mail.rows, crmRows: crm.rows, matches, mailCityTotals, mailZipTotals, topCities, topZips, xLabs, mSeries, cSeries, mtSeries, mSeriesY, cSeriesY, mtSeriesY };
    }


    function renderSummary(state) {
      try {
        const tbody = document.querySelector('[data-summary="rows"]');
        if (!tbody) return;
        const esc = (v) => String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        const rows = state.matches || [];
        tbody.innerHTML = rows.slice().sort((a, b) => parseCRMDate(b["CRM Date"]) - parseCRMDate(a["CRM Date"])).map(m => (
          `<tr>
        <td>${esc(m["Mail Address 1"])}</td>
        <td>${esc(m["Mail Unit"])}</td>
        <td>${esc(m["CRM Address 1"])}</td>
        <td>${esc(m["CRM Unit"])}</td>
        <td>${esc(m["City"])}</td>
        <td>${esc(m["State"])}</td>
        <td>${esc(m["ZIP"])}</td>
        <td>${esc(m["Mail Dates"])}</td>
        <td>${esc(m["CRM Date"])}</td>
        <td>${esc(m["Amount"])}</td>
        <td class="conf" style="text-align:right">${esc(m["Confidence"])}</td>
        <td>${esc(m["Notes"])}</td>
      </tr>`
        )).join("");
        colorizeConfidence();
        // Rebuild sticky header clone/alignment after render
        if (typeof setupSummarySticky === "function") {
          try { setupSummarySticky(); } catch (e) { console.warn("[MailTrace] sticky skip", e); }
        }
      } catch (e) {
        console.error("[MailTrace] renderSummary failed:", e);
      }
    }

    // ---------- Renderers ----------
    function renderKPIs(state) {
      try {
        const mailCount = state.mailRows.length;
        const crmCount = state.crmRows.length;
        const matchCount = state.matches.length;
        // Fill counts
        const q = sel => document.querySelector(sel);
        if (q('[data-kpi="mail"]')) q('[data-kpi="mail"]').textContent = String(mailCount);
        if (q('[data-kpi="crm"]')) q('[data-kpi="crm"]').textContent = String(crmCount);
        if (q('[data-kpi="matches"]')) q('[data-kpi="matches"]').textContent = String(matchCount);
        // Match Rate = matches / total mailers sent
        const rate = mailCount ? ((matchCount / mailCount) * 100).toFixed(1) + '%' : '0%';
        if (q('[data-kpi="rate"]')) q('[data-kpi="rate"]').textContent = rate;
        // Revenue from matches = sum of Amount on matched rows
        const revenueNum = (state.matches || []).reduce((sum, m) => {
          const raw = (m && m['Amount']) ? String(m['Amount']) : '0';
          const val = parseFloat(raw.replace(/[^0-9.-]/g, '')) || 0;
          return sum + val;
        }, 0);
        const fmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
        if (q('[data-kpi="revenue"]')) q('[data-kpi="revenue"]').textContent = fmt.format(revenueNum);
      } catch (e) { console.error('[MailTrace] renderKPIs failed:', e); }


      // --- Expanded KPIs: RPM, Avg Ticket, Median Days, 30/60/90 ---
      try {
        const q = (sel) => document.querySelector(sel);

        // Ensure tiles exist (or adjust existing ones)
        (function ensureTiles() {
          const host = document.getElementById('cmp-kpis');
          if (!host) return;

          // Helper to ensure a KPI tile exists
          const ensureTile = (key, label) => {
            let v = q('[data-kpi="' + key + '"]');
            if (!v) {
              const tile = document.createElement('div');
              tile.className = 'kpi';
              tile.innerHTML = '<div class="v" data-kpi="' + key + '">–</div><div class="l">' + label + '</div>';
              host.appendChild(tile);
            } else {
              // refresh label in case we’re repurposing a previous tile
              const l = v.parentElement.querySelector('.l');
              if (l) l.textContent = label;
            }
          };

          // If an old Avg Days tile exists, repurpose to "Median Days to Convert"
          const oldAvg = q('[data-kpi="avgdays"]');
          if (oldAvg) {
            oldAvg.setAttribute('data-kpi', 'meddays');
            const l = oldAvg.parentElement.querySelector('.l');
            if (l) l.textContent = 'Median Days to Convert';
          }

          ensureTile('rpm', 'Revenue per Mailer');
          ensureTile('avgticket', 'Avg Ticket (per Match)');
          ensureTile('meddays', 'Median Days to Convert');
          ensureTile('d30', 'Converted ≤30 Days');
          ensureTile('d60', 'Converted ≤60 Days');
          ensureTile('d90', 'Converted ≤90 Days');
          ensureTile('uniqmail', 'Unique Mail Addresses');
        })();



        // --- Advanced KPIs: collapse/expand UI and placement (inline in KPI box) ---
        (function ensureAdvancedKPIsUI() {
          const host = document.getElementById('cmp-kpis');
          if (!host) return;

          let toggle = document.getElementById('kpi-adv-toggle');
          if (!toggle) {
            toggle = document.createElement('button');
            toggle.id = 'kpi-adv-toggle';
            toggle.type = 'button';
            toggle.textContent = 'Show advanced KPIs';
            host.appendChild(toggle);
          }

          let adv = document.getElementById('cmp-kpis-advanced');
          if (!adv) {
            adv = document.createElement('div');
            adv.id = 'cmp-kpis-advanced';
            adv.className = 'kpis kpis-advanced';
            host.appendChild(adv);
          }

          // Move advanced tiles into the advanced container
          const advKeys = ['rpm', 'avgticket', 'meddays', 'd30', 'd60', 'd90'];
          advKeys.forEach(k => {
            const v = document.querySelector('[data-kpi="' + k + '"]');
            if (v) {
              const tile = v.parentElement;
              if (tile && tile.parentElement !== adv) adv.appendChild(tile);
            }
          });

          if (!toggle._bound) {
            toggle._bound = true;
            toggle.addEventListener('click', () => {
              const show = adv.style.display === 'none' || !adv.style.display;
              adv.style.display = show ? 'grid' : 'none';
              toggle.textContent = show ? 'Hide advanced KPIs' : 'Show advanced KPIs';
            });
          }
        })();
        // --- end Advanced KPIs UI ---
        // Rename labels for base KPIs
        (function renameBaseKPIlabels() {
          const host = document.getElementById('cmp-kpis');
          if (!host) return;
          const crmV = host.querySelector('[data-kpi="crm"]');
          if (crmV) {
            const l = crmV.parentElement.querySelector('.l');
            if (l) l.textContent = 'Total Jobs';
          }
          const uniqV = host.querySelector('[data-kpi="uniqmail"]');
          if (uniqV) {
            const l2 = uniqV.parentElement.querySelector('.l');
            if (l2) l2.textContent = 'Unique Mail Addresses';
          }
        })();

        // Reposition: place Unique Mail Addresses immediately after Total Mail
        (function placeUniqAfterMail() {
          const host = document.getElementById('cmp-kpis');
          if (!host) return;
          const mailV = host.querySelector('[data-kpi="mail"]');
          const uniqV = host.querySelector('[data-kpi="uniqmail"]');
          if (host && mailV && uniqV) {
            const mailTile = mailV.parentElement;
            const uniqTile = uniqV.parentElement;
            const next = mailTile.nextSibling;
            if (next !== uniqTile) {
              host.insertBefore(uniqTile, next);
            }
          }
        })();
        // Parse MM-DD-YYYY (e.g., 01-31-2024)
        const parseMDY = (s) => {
          if (!s) return null;
          s = String(s).trim();
          if (s === '—' || s === '-' || s.toLowerCase() === 'na') return null;
          const parts = s.split('-').map(x => parseInt(x, 10));
          if (parts.length !== 3 || parts.some(isNaN)) return null;
          const [mm, dd, yyyy] = parts;
          const d = new Date(yyyy, mm - 1, dd);
          return isNaN(d.getTime()) ? null : d;
        };

        // Collect day diffs per match + revenue
        let dayDiffs = [];
        let c30 = 0, c60 = 0, c90 = 0;
        let validCount = 0;

        // Revenue recompute (in case not available as a variable here)
        let revenueNum = 0;
        for (const m of (state.matches || [])) {
          const raw = (m && m['Amount']) ? String(m['Amount']) : '0';
          const val = parseFloat(raw.replace(/[^0-9.-]/g, '')) || 0;
          revenueNum += val;
        }

        for (const row of (state.matches || [])) {
          const mailStr = String(row['Mail Dates'] || '').trim();
          const crmStr = String(row['CRM Date'] || '').trim();
          if (!mailStr || !crmStr || mailStr === '—' || crmStr === '—') continue;

          const parts = mailStr.split(/,\s*/).map(s => s.trim()).filter(Boolean);
          let earliest = null;
          for (const ds of parts) {
            const d = parseMDY(ds);
            if (d && (!earliest || d < earliest)) earliest = d;
          }
          const crmD = parseMDY(crmStr);
          if (earliest && crmD) {
            const diff = Math.round((crmD - earliest) / (1000 * 60 * 60 * 24));
            if (Number.isFinite(diff)) {
              dayDiffs.push(diff);
              validCount++;
              if (diff <= 30) c30++;
              if (diff <= 60) c60++;
              if (diff <= 90) c90++;
            }
          }
        }

        // Median days
        let med = 0;
        if (dayDiffs.length) {
          dayDiffs.sort((a, b) => a - b);
          const mid = Math.floor(dayDiffs.length / 2);
          med = (dayDiffs.length % 2 === 0) ? Math.round((dayDiffs[mid - 1] + dayDiffs[mid]) / 2) : dayDiffs[mid];
        }

        // RPM & Avg Ticket
        const fmtC = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
        const mailCount = (state.mailRows || []).length;
        const matchCount = (state.matches || []).length;
        const rpm = mailCount ? revenueNum / mailCount : 0;
        const avgTicket = matchCount ? revenueNum / matchCount : 0;

        // Fill tiles
        const setText = (k, v) => { const n = document.querySelector('[data-kpi="' + k + '"]'); if (n) n.textContent = v; };
        setText('rpm', fmtC.format(rpm));
        setText('avgticket', fmtC.format(avgTicket));
        setText('meddays', String(med));

        const pct = (num, den) => (den ? Math.round((num / den) * 100) : 0) + '%';
        setText('d30', pct(c30, validCount));
        setText('d60', pct(c60, validCount));
        setText('d90', pct(c90, validCount));
      } catch (e) { console.error('Expanded KPI block failed:', e); }
      // --- end expanded KPIs ---

      (function () { const n = document.querySelector('[data-kpi="uniqmail"]'); if (n) n.textContent = String(state.uniqueMailAddrs || 0); })();
    }

    function renderTop(state) {
      const cBody = document.querySelector('[data-top="cities"]'); const zBody = document.querySelector('[data-top="zips"]');
      cBody.innerHTML = state.topCities.map(r => { const denom = state.mailCityTotals.get(r.City) || 0; const rate = denom ? (Math.round(100 * r.Count / denom) + "%") : "–"; return `<tr><td>${r.City}</td><td style="text-align:center">${r.Count}</td><td style="text-align:center">${rate}</td></tr>`; }).join("");
      zBody.innerHTML = state.topZips.map(r => { const denom = state.mailZipTotals.get(r.ZIP) || 0; const rate = denom ? (Math.round(100 * r.Count / denom) + "%") : "–"; return `<tr><td>${r.ZIP}</td><td style="text-align:center">${r.Count}</td><td style="text-align:center">${rate}</td></tr>`; }).join("");
    }
    function lineChart(canvas, labels, series, overlay) {
      // Hi-DPI: size backing store to devicePixelRatio but draw in CSS pixels
      const cssW = canvas.clientWidth || canvas.width;
      const cssH = canvas.clientHeight || canvas.height;
      const dpr = Math.max(1, window.devicePixelRatio || 1);
      const needResize =
        canvas.width !== Math.round(cssW * dpr) ||
        canvas.height !== Math.round(cssH * dpr);

      if (needResize) {
        canvas.width  = Math.round(cssW * dpr);
        canvas.height = Math.round(cssH * dpr);
      }

      const ctx = canvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0); // paint in CSS pixels

      const W = cssW, H = cssH, pad = 30;
      ctx.clearRect(0, 0, W, H);

      const all = [...series.flatMap(s => s.data), ...(overlay || []).flatMap(s => s.data)];
      const min = 0, max = Math.max(1, Math.max(...all));
      const xFor = i => pad + i * (W - 2 * pad) / Math.max(1, labels.length - 1);
      const yFor = v => H - pad - ((v - min) / (max - min)) * (H - 2 * pad);

      // grid + axes
      ctx.strokeStyle = "#233049"; ctx.lineWidth = 1; ctx.beginPath();
      for (let i = 0; i < labels.length; i++) { const x = xFor(i); ctx.moveTo(x, H - pad); ctx.lineTo(x, pad); }
      ctx.stroke();
      ctx.strokeStyle = "#2f3b57"; ctx.lineWidth = 1.5; ctx.beginPath();
      ctx.moveTo(pad, pad); ctx.lineTo(pad, H - pad); ctx.lineTo(W - pad, H - pad); ctx.stroke();

      const colors = ["#60a5fa", "#34d399", "#f59e0b", "#f472b6", "#a78bfa"];

      const drawMarkers = (pts, r, col, alpha = 1) => {
        ctx.save(); ctx.globalAlpha = alpha; ctx.fillStyle = col;
        for (const [x, y] of pts) { ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fill(); }
        ctx.restore();
      };

      // main series (solid lines + dots)
      series.forEach((s, si) => {
        const col = colors[si % colors.length];
        ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.beginPath();
        const pts = [];
        s.data.forEach((v, i) => {
          const x = xFor(i), y = yFor(v); pts.push([x, y]);
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();
        drawMarkers(pts, 2.5, col, 1);
      });

      // YoY overlay (dashed + smaller dots)
      if (overlay && overlay.length) {
        overlay.forEach((s, si) => {
          const col = colors[si % colors.length];
          ctx.save(); ctx.setLineDash([5, 4]); ctx.strokeStyle = col; ctx.globalAlpha = 0.7; ctx.lineWidth = 2;
          ctx.beginPath();
          const pts = [];
          s.data.forEach((v, i) => {
            const x = xFor(i), y = yFor(v); pts.push([x, y]);
            if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
          });
          ctx.stroke(); ctx.restore();
          drawMarkers(pts, 2, col, 0.7);
        });
      }

      // ticks
      ctx.fillStyle = "#9fb2c8"; ctx.font = "11px system-ui";
      const tickStep = Math.max(1, Math.ceil(labels.length / 12));
      labels.forEach((lab, i) => {
        if (i % tickStep !== 0) return;
        const x = pad + i * (W - 2 * pad) / Math.max(1, labels.length - 1);
        ctx.fillText(lab, x - 14, H - pad + 14);
      });
    }
    window.lineChart = lineChart;
    function renderGraph(state) {
      const canvas = document.getElementById("chart");
      const _yoy = document.getElementById("yoyToggle"); const overlayOn = !!(_yoy && _yoy.checked);
      const overlay = overlayOn ? [{ label: "Mail (YoY)", data: state.mSeriesY }, { label: "CRM (YoY)", data: state.cSeriesY }, { label: "Matches (YoY)", data: state.mtSeriesY }] : [];
      (window.lineChart || lineChart)(canvas, state.xLabs, [{ label: "Mail volume", data: state.mSeries }, { label: "CRM jobs", data: state.cSeries }, { label: "Matches", data: state.mtSeries }], overlay);
    }

    function setupSummarySticky() {
      try {
        const wrap = document.querySelector('#cmp-summary .table-wrap');
        if (!wrap) return;
        const table = wrap.querySelector('table');
        if (!table) return;
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');
        if (!thead || !tbody) return;

        // Remove previous floating head
        const prev = wrap.querySelector('.floating-head');
        if (prev) prev.remove();

        // Ensure a <colgroup id="sum-cols"> exists on main table with correct number of cols
        const thCount = thead.querySelectorAll('th').length;
        let cgMain = table.querySelector('colgroup#sum-cols');
        if (!cgMain) {
          cgMain = document.createElement('colgroup');
          cgMain.id = 'sum-cols';
          table.insertBefore(cgMain, thead);
        }
        if (cgMain.children.length !== thCount) {
          cgMain.innerHTML = new Array(thCount).fill('<col>').join('');
        }

        // Build floating head table with its own colgroup
        const clone = document.createElement('table');
        clone.className = 'floating-head';
        clone.setAttribute('aria-hidden', 'true');
        const cgClone = document.createElement('colgroup');
        cgClone.id = 'sum-cols-clone';
        cgClone.innerHTML = new Array(thCount).fill('<col>').join('');
        clone.appendChild(cgClone);
        const theadClone = thead.cloneNode(true);
        clone.appendChild(theadClone);
        wrap.prepend(clone);

        // Sync widths from first body row cells to both colgroups
        function syncCols() {
          const row = tbody.querySelector('tr');
          const cells = row ? row.children : thead.querySelectorAll('th');
          const n = Math.min(cells.length, thCount);
          const mainCols = cgMain.querySelectorAll('col');
          const cloneCols = cgClone.querySelectorAll('col');
          for (let i = 0; i < n; i++) {
            const w = cells[i].getBoundingClientRect().width || cells[i].offsetWidth || 0;
            mainCols[i].style.width = w + 'px';
            cloneCols[i].style.width = w + 'px';
          }
          clone.style.width = table.scrollWidth + 'px';
        }

        // Initial sync (twice to catch fonts/layout)
        syncCols();
        setTimeout(syncCols, 0);
        setTimeout(syncCols, 250);

        // Hide native thead (clone will be visible)
        thead.style.visibility = 'hidden';

        // Keep clone aligned on scroll/resize
        window.addEventListener('resize', syncCols);
      } catch (e) {
        console.warn('[MailTrace] sticky header colgroup sync skipped:', e);
      }
    }

    setupSummarySticky();

    window.MailTraceUI = {
      state: null,
      computeState,
      render: {
        kpis: () => renderKPIs(window.MailTraceUI.state),
        top: () => renderTop(window.MailTraceUI.state),
        graph: () => renderGraph(window.MailTraceUI.state),
        summary: () => renderSummary(window.MailTraceUI.state),
        all: () => { const S = window.MailTraceUI.state; renderKPIs(S); renderTop(S); renderGraph(S); renderSummary(S); }
      }
    };

    document.getElementById("yoyToggle").addEventListener("change", () => MailTraceUI.render.graph());
    document.getElementById("runBtn").addEventListener("click", async () => {
      try {
        const mf = document.getElementById("mailCsv").files[0];
        const cf = document.getElementById("crmCsv").files[0];
        if (!mf || !cf) return showError("Please pick both CSV files first.");
        const mailText = await mf.text();
        const crmText = await cf.text();
        try {
          window.MailTraceUI.state = computeState(mailText, crmText);
          window.MailTraceUI.render.all();
        } catch (e) {
          if (e && e.__isMappingError) {
            const mapping = await openColumnMapper(e.payload);
            if (mapping) {
              window.MailTraceUI.state = computeState(mailText, crmText, mapping);
              window.MailTraceUI.render.all();
            } else {
              return;
            }
          } else {
            throw e;
          }
        }
      } catch (e) { console.error(e); showError(e.stack || String(e)); }
    });

    function colorizeConfidence() {
      const tds = document.querySelectorAll('[data-summary="rows"] td.conf');
      for (const td of tds) {
        td.classList.remove('conf-green', 'conf-yellow', 'conf-red');
        const n = parseInt(String(td.textContent || '').replace(/[^0-9]/g, ''), 10) || 0;
        if (n > 95) td.classList.add('conf-green');
        else if (n <= 85) td.classList.add('conf-red');
        else td.classList.add('conf-yellow');
      }
    }

    window.addEventListener('load', colorizeConfidence);

    function parseCRMDate(v) {
      if (!v) return 0;
      const s = String(v).trim();
      const m = s.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{2,4})$/);
      if (!m) return Date.parse(s) || 0;
      let mm = parseInt(m[1], 10), dd = parseInt(m[2], 10), yy = parseInt(m[3], 10);
      if (yy < 100) yy += 2000;
      return Date.UTC(yy, Math.max(0, mm - 1), dd);
    }

    // Optional: guard DOM listeners if elements are missing (keeps file idempotent across pages)
    const safeBind = (sel, type, fn) => {
    const el = document.querySelector(sel);
    if (el && !el._mtBound) { el.addEventListener(type, fn); el._mtBound = true; }
    };
    safeBind('#yoyToggle', 'change', () => window.MailTraceUI?.render?.graph?.());

})();