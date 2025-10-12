from __future__ import annotations

import io, re, base64
from datetime import datetime
from typing import List, Tuple, Optional
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def _to_num(x) -> float:
    if x is None: return 0.0
    s = str(x).strip()
    if not s: return 0.0
    s = s.replace("$","").replace(",","")
    try: return float(s)
    except: return 0.0

def _fmt_money(n: float) -> str:
    try: return f"${n:,.2f}"
    except: return f"${_to_num(n):,.2f}"

def _fmt_conf(c: Optional[int]) -> str:
    if c is None: return ""
    c = max(0, min(100, int(c)))
    if c >= 94: cls = "conf-high"
    elif c >= 88: cls = "conf-mid"
    else: cls = "conf-low"
    return f'<span class="conf {cls}">{c}%</span>'

def _norm_str(x) -> str:
    return "" if x is None else str(x).strip()

_DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y",
    "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%y", "%d-%m-%y",
    "%d/%m/%Y", "%b %d, %Y", "%b %Y", "%Y-%m"
]

def _parse_date_any(s: str):
    if not isinstance(s, str) or not s.strip(): return None
    z = s.strip().replace(".", "/").replace("_", "/").replace("\\", "/")
    for fmt in _DATE_FORMATS:
        try: return datetime.strptime(z, fmt).date()
        except: continue
    try:
        d = pd.to_datetime(z, errors="coerce")
        if pd.isna(d): return None
        return d.date()
    except: return None

def _parse_many_dates(csvish: str) -> List[str]:
    if not isinstance(csvish, str) or not csvish.strip(): return []
    parts = [p.strip() for p in re.split(r"[;,]+", csvish) if p.strip()]
    parsed = [_parse_date_any(p) for p in parts]
    parsed = [d for d in parsed if d is not None]
    parsed.sort()
    return [d.strftime("%m-%d-%y") for d in parsed]

def _fmt_crm_date(s) -> str:
    d = _parse_date_any(str(s) if s is not None else "")
    return d.strftime("%m-%d-%y") if d else ""

def _city_state_zip(city: str, state: str, zip5: str) -> str:
    city = _norm_str(city); state = _norm_str(state); zip5 = _norm_str(zip5)
    if city or state or zip5:
        left = f"{city}, {state}".strip(", ")
        return f"{left} {zip5}".strip()
    return ""

_UNIT_WORDS = ("apt","apartment","unit","suite","ste","bldg","building","floor","fl","#")

def _split_mail_address(full: str):
    s = _norm_str(full)
    if not s: return "",""
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for i in range(len(parts)-1, -1, -1):
            tail = ", ".join(parts[i:])
            if re.search(r"\\b[A-Z]{2}\\b", tail) and re.search(r"\\b\\d{5}(-\\d{4})?\\b", tail):
                left = ", ".join(parts[:i]); return left, tail
    m = re.search(r"^(.*\\S)\\s+([A-Z]{2})\\s+(\\d{5}(?:-\\d{4})?)$", s)
    if m:
        left, state, zipc = m.group(1).strip(), m.group(2), m.group(3)
        if "," in left:
            return left, f"{state} {zipc}"
        tokens = left.split()
        if len(tokens) >= 3:
            city_guess = tokens[-1]; street = " ".join(tokens[:-1])
            return street, f"{city_guess}, {state} {zipc}"
        else:
            return left, f"{state} {zipc}"
    return s, ""

def _compose_mail_street_with_unit(street_like: str) -> str:
    s = _norm_str(street_like)
    if not s: return ""
    if re.search(r",\\s*(?:apt|suite|ste|unit|bldg|#)\\b", s, flags=re.I):
        return s
    tokens = s.split()
    for i,t in enumerate(tokens):
        t_clean = t.lower().strip(",.")
        if t_clean in _UNIT_WORDS:
            return " ".join(tokens[:i]) + ", " + " ".join(tokens[i:])
    return s

def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "mail_dates", "crm_job_date", "amount",
            "mail_street", "mail_city_state_zip",
            "crm_street", "crm_city_state_zip",
            "confidence", "match_notes"
        ])
    d = df.copy()
    crm_a1 = _pick_col(d, ["crm_address1","crm_address1_original","crm_street","crm_address"])
    crm_a2 = _pick_col(d, ["crm_address2","crm_address2_original","crm_unit","crm_line2"])
    crm_city = _pick_col(d, ["crm_city","city"])
    crm_state = _pick_col(d, ["crm_state","state"])
    crm_zip = _pick_col(d, ["crm_zip","postal_code","zip","zipcode","zip_code"])
    crm_date = _pick_col(d, ["crm_job_date","job_date","date","created_at"])
    amt_col = _pick_col(d, ["amount","job_value","revenue","value"])
    if amt_col not in d.columns:
        d["amount"] = 0.0; amt_col = "amount"
    mail_full = _pick_col(d, ["matched_mail_full_address","mail_full_address","mail_address","mail_addr_full"])
    mail_dates = _pick_col(d, ["mail_dates_in_window","mail_dates","mail_history"])
    conf_col = _pick_col(d, ["confidence_percent","confidence","score"])
    notes_col = _pick_col(d, ["match_notes","notes"])

    d["crm_street"] = d.get(crm_a1, "").astype(str).str.strip()
    if crm_a2 in d.columns:
        extra = d[crm_a2].fillna("").astype(str).str.strip()
        d.loc[extra.ne(""), "crm_street"] = d.loc[extra.ne(""), "crm_street"] + ", " + extra[extra.ne("")]
    d["crm_city_state_zip"] = d.apply(lambda r: _city_state_zip(r.get(crm_city,""), r.get(crm_state,""), str(r.get(crm_zip,""))), axis=1)

    d["mail_street"] = ""
    d["mail_city_state_zip"] = ""
    if mail_full in d.columns:
        splitted = d[mail_full].fillna("").astype(str).map(_split_mail_address)
        d["mail_street"] = splitted.map(lambda t: _compose_mail_street_with_unit(t[0]))
        d["mail_city_state_zip"] = splitted.map(lambda t: t[1])

    d["mail_dates"] = ""
    if mail_dates in d.columns:
        d["mail_dates"] = d[mail_dates].fillna("").astype(str).map(_parse_many_dates).map(lambda L: ", ".join(L) if L else "")

    d["crm_job_date"] = d[crm_date].map(_fmt_crm_date)
    d["amount_num"] = d[amt_col].map(_to_num)
    d["confidence"] = d[conf_col].map(lambda x: int(round(float(x)))) if conf_col in d.columns else 0
    d["match_notes"] = d[notes_col] if notes_col in d.columns else ""

    out = d[[
        "mail_dates","crm_job_date","amount_num",
        "mail_street","mail_city_state_zip",
        "crm_street","crm_city_state_zip",
        "confidence","match_notes"
    ]].copy()
    return out

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols: return cols[c.lower()]
    return ""

def _calc_kpis(d: pd.DataFrame, mail_count_total: int) -> dict:
    total_matches = len(d) if d is not None else 0
    total_revenue = float(d["amount_num"].sum()) if total_matches else 0.0

    def _count_dates(s: str) -> int:
        if not s: return 0
        return len([p for p in s.split(",") if p.strip()])

    avg_mailers_before = 0.0
    if total_matches:
        avg_mailers_before = sum(_count_dates(x) for x in d["mail_dates"]) / total_matches
    mailers_per_acq = (mail_count_total / total_matches) if (mail_count_total and total_matches) else 0.0
    return {
        "mail_total": int(mail_count_total or 0),
        "matches": int(total_matches),
        "revenue": total_revenue,
        "avg_mailers_before": avg_mailers_before,
        "mailers_per_acq": mailers_per_acq,
    }

def _top_counts(d: pd.DataFrame, city_col: str, state_col: str, zip_col: str, limit: int = 5):
    if d is None or d.empty: return [], []
    import re as _re
    tmp = pd.DataFrame({"city_state": d["crm_city_state_zip"].fillna(""),
                        "zip": d[zip_col] if zip_col in d.columns else ""})
    def _just_city_state(s: str) -> str:
        if not s: return ""
        m = _re.match(r"^(.*?\\b[A-Za-z\\.\\s]+,\\s*[A-Z]{2})\\b", s)
        if m: return m.group(1).strip()
        parts = s.split()
        for i in range(len(parts)-1, -1, -1):
            if _re.match(r"\\d{5}(-\\d{4})?$", parts[i]): return " ".join(parts[:i]).strip()
        return s
    tmp["cs_only"] = tmp["city_state"].map(_just_city_state)
    city_counts = tmp["cs_only"].value_counts().head(limit)
    zip_counts = tmp["zip"].value_counts().head(limit)
    city_items = [(idx, int(val)) for idx, val in city_counts.items() if idx]
    zip_items = [(idx, int(val)) for idx, val in zip_counts.items() if idx]
    return city_items, zip_items

def _month_series(d: pd.DataFrame):
    if d is None or d.empty: return pd.Series(dtype=int)
    dates = d["crm_job_date"].map(_parse_date_any)
    ser = pd.Series(dates).dropna()
    if ser.empty: return pd.Series(dtype=int)
    months = ser.map(lambda dt: dt.replace(day=1))
    counts = months.value_counts().sort_index()
    counts.index = counts.index.map(lambda dt: dt.strftime("%b %Y"))
    return counts

def _plot_month_chart(counts: pd.Series) -> str:
    if counts is None or counts.empty: return ""
    fig = plt.figure(figsize=(10.5,3.6), dpi=150)
    ax = fig.add_subplot(111)
    ax.bar(counts.index, counts.values)
    ax.set_xlabel("Month"); ax.set_ylabel("Matches"); ax.set_title("Matched Jobs by Month")
    plt.xticks(rotation=35, ha="right"); plt.tight_layout()
    import io, base64
    buf = io.BytesIO(); plt.savefig(buf, format="png", bbox_inches="tight"); plt.close(fig); buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")

def render_full_dashboard_v17(summary_df: pd.DataFrame, mail_count_total: int) -> str:
    d = finalize_summary_for_export_v17(summary_df)
    kpi = _calc_kpis(d, mail_count_total)
    d["_crm_zip_only"] = d["crm_city_state_zip"].str.extract(r"(\\d{5}(?:-\\d{4})?)", expand=False).fillna("")
    top_cities, top_zips = _top_counts(d, "crm_city_state_zip", "", "_crm_zip_only", limit=5)
    month_counts = _month_series(d); month_png = _plot_month_chart(month_counts)

    rows_html = []
    view_limit = min(200, len(d))
    if view_limit:
        def _latest_mail_date(csvish: str):
            parts = _parse_many_dates(csvish)
            if not parts: return None
            last = parts[-1]
            return _parse_date_any(last)
        d["_crm_dt"] = d["crm_job_date"].map(_parse_date_any)
        d["_fallback_dt"] = d["mail_dates"].map(_latest_mail_date)
        d["_sort_dt"] = d.apply(lambda r: r["_crm_dt"] or r["_fallback_dt"], axis=1)
        d = d.sort_values(by="_sort_dt", ascending=False, na_position="last").head(view_limit).copy()
        for _, r in d.iterrows():
            mail_dates = _norm_str(r.get("mail_dates",""))
            crm_date = _norm_str(r.get("crm_job_date",""))
            amt = _fmt_money(r.get("amount_num",0.0))
            mail_street = _compose_mail_street_with_unit(_norm_str(r.get("mail_street","")))
            mail_place = _norm_str(r.get("mail_city_state_zip",""))
            crm_street = _norm_str(r.get("crm_street",""))
            crm_place = _norm_str(r.get("crm_city_state_zip",""))
            conf = _fmt_conf(r.get("confidence",0))
            notes = _norm_str(r.get("match_notes",""))
            if mail_place == ",": mail_place = ""
            rows_html.append(f"""
            <tr>
              <td class="td-date">{mail_dates or ""}</td>
              <td class="td-date">{crm_date or ""}</td>
              <td class="td-money">{amt}</td>
              <td>{mail_street}</td>
              <td>{mail_place}</td>
              <td>{crm_street}</td>
              <td>{crm_place}</td>
              <td class="td-conf">{conf}</td>
              <td>{notes}</td>
            </tr>""")
    def _list_block(items: List[Tuple[str,int]]) -> str:
        if not items: return '<div class="muted">No data</div>'
        lis = "".join([f"<li><span>{name}</span><b>{cnt}</b></li>" for name,cnt in items])
        return f'<ul class="toplist">{lis}</ul>'
    cities_html=_list_block(top_cities); zips_html=_list_block(top_zips)
    chart_html = (f'<img class="month-chart" alt="Matched Jobs by Month" src="data:image/png;base64,{month_png}"/>' if month_png else '<div class="muted">No monthly data</div>')

    html = f"""
<div class="container">
  <div class="grid kpi-grid">
    <div class="card kpi"><div class="k">Total mail records</div><div class="v">{kpi['mail_total']:,}</div></div>
    <div class="card kpi"><div class="k">Matches</div><div class="v">{kpi['matches']:,}</div></div>
    <div class="card kpi"><div class="k">Total revenue generated</div><div class="v">{_fmt_money(kpi['revenue'])}</div></div>
    <div class="card kpi"><div class="k">Avg mailers before engagement</div><div class="v">{kpi['avg_mailers_before']:.2f}</div></div>
    <div class="card kpi"><div class="k">Mailers per acquisition</div><div class="v">{kpi['mailers_per_acq']:.2f}</div></div>
  </div>

  <div class="row wrap">
    <div class="card flex-1">
      <h3>Top Cities (matches)</h3>
      <div class="scroller">{cities_html}</div>
    </div>
    <div class="card flex-1">
      <h3>Top ZIP Codes (matches)</h3>
      <div class="scroller">{zips_html}</div>
    </div>
  </div>

  <div class="card">
    <h3>Matched Jobs by Month</h3>
    {chart_html}
  </div>

  <div class="card">
    <h3>Sample of Matches</h3>
    <div class="note">Sorted by most recent CRM date (falls back to mail date). Showing up to 200 rows.</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Mail Dates</th><th>CRM Date</th><th>Amount</th>
            <th>Mail Address</th><th>Mail City/State/Zip</th>
            <th>CRM Address</th><th>CRM City/State/Zip</th>
            <th>Confidence</th><th>Notes</th>
          </tr>
        </thead>
        <tbody>{''.join(rows_html) if rows_html else '<tr><td colspan="9" class="muted">No matches to display.</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>
<style>
.kpi-grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin: 0 0 16px; }}
.kpi .k {{ color:#64748b; font-size:13px; font-weight:700; }}
.kpi .v {{ font-size:28px; font-weight:900; }}
.row.wrap {{ display:flex; gap:16px; flex-wrap:wrap; margin-bottom:16px; }}
.flex-1 {{ flex:1 1 300px; min-width:260px; }}
.scroller {{ max-height: 220px; overflow:auto; }}
.toplist {{ list-style:none; margin:0; padding:0; }}
.toplist li {{ display:flex; justify-content:space-between; padding:8px 0; border-bottom:1px solid #f1f5f9; }}
.toplist li b {{ font-weight:800; }}
.table-wrap {{ overflow:auto; }}
table {{ width:100%; border-collapse: collapse; }}
th, td {{ text-align:left; padding:10px 12px; border-bottom:1px solid #eef2f7; font-size:14px; vertical-align:top; }}
th {{ background:#f8fafc; position:sticky; top:0; z-index:1; }}
.td-money {{ white-space:nowrap; font-variant-numeric: tabular-nums; }}
.conf {{ font-weight:800; padding:4px 8px; border-radius:10px; font-size:12px; display:inline-block; }}
.conf-high {{ background:#e8f5e9; color:#1e7e34; }}
.conf-mid  {{ background:#fff6e5; color:#a36100; }}
.conf-low  {{ background:#fde8e8; color:#b91c1c; }}
.month-chart {{ width:100%; max-height:360px; object-fit:contain; }}
.muted {{ color:#64748b; }}
</style>
"""
    return html
