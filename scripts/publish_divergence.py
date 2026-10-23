#!/usr/bin/env python3
"""Publish DIVERGENCE CW vs EW cho analyze_market (O-2 — mo schema
G6, ca nong M-1 "xanh vo do long"). Ban nap chinh thuc B.0 +
Phu luc I khoi 4: cw_ew_spread = index_pct - ew_pct (diem %).

Nguon: warehouse screener_history (pct_change tung ma + von hoa).
- ew_pct: trung binh cong % thay doi cac ma HOSE co pct_change.
- cw_self_pct: tu tinh theo trong so von hoa DAU phien suy nguoc
  w_{t-1} ~ mcap_t/(1+r_i) (xap xi full-mcap nhu khoi dong gop —
  thieu f_i/c_i cua HOSE, khai trong method).
- index_pct: % thay doi VNINDEX chinh thuc — tu R2 ohlcv VNINDEX
  (close ke tiep); phien moi nhat chua co nen thi lay tu
  indices.json khi ngay khop.
- leader_laggard_spread: loi suat von-hoa-tung-nganh (ICB, thanh
  vien tu R2 sectors/<code>.json) — max tru min, kem sectors_up.

Publish 1 file market/divergence.json {updated, sessions: [...]}.
Chay sau screener_history_capture (can >= 1 phien co pct_change):
  .venv5x/bin/python scripts/publish_divergence.py
"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))
from publish_snapshot import post_chunk  # noqa: E402

DB = "data/warehouse.db"
SECRET_FILE = Path.home() / ".stockapp_publish"

_spec = importlib.util.spec_from_file_location(
    "shc", str(_HERE / "screener_history_capture.py"))
_shc = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_shc)
except SystemExit:
    pass
cf_req = _shc.cf_req


def _r2_json(key):
    st, raw = cf_req("GET", key)
    if st != 200 or not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def sector_map() -> dict[str, str]:
    """symbol -> ma nganh ICB tu R2 sectors/<code>.json (members)."""
    out: dict[str, str] = {}
    ov = _r2_json("stock/market/sectors.json") or {}
    codes = []
    for r in (ov.get("rows") or ov.get("sectors") or []):
        c = r.get("code") or r.get("sector_code")
        if c:
            codes.append(str(c))
    if not codes:
        codes = ["0500", "1300", "1700", "2300", "2700", "3300",
                 "3500", "3700", "4500", "5300", "5500", "5700",
                 "6500", "7500", "8300", "8500", "8600", "8700", "9500"]
    for code in codes:
        d = _r2_json(f"stock/market/sectors/{code}.json") or {}
        members = d.get("members") or d.get("rows") or d.get("symbols") or []
        for m in members:
            sym = m.get("symbol") if isinstance(m, dict) else m
            if sym:
                out[str(sym).upper()] = code
    return out


def index_pcts() -> dict[str, float]:
    """date -> % thay doi VNINDEX chinh thuc (tu nen ngay)."""
    d = _r2_json("stock/market/ohlcv/VNINDEX.json") or {}
    rows = sorted(d.get("rows") or [], key=lambda r: r.get("t") or "")
    out: dict[str, float] = {}
    prev = None
    for r in rows:
        c = r.get("c")
        if prev and c:
            out[r["t"]] = round((c / prev - 1) * 100, 4)
        if c:
            prev = c
    # Phien moi nhat: indices.json (neu ngay khop phien screener)
    idx = _r2_json("stock/market/indices.json") or {}
    upd = str(idx.get("updated") or "")[:10]
    for r in (idx.get("rows") or []):
        if (r.get("code") or r.get("symbol")) == "VNINDEX":
            pc = r.get("pct_change", r.get("change_pct"))
            if upd and pc is not None:
                out.setdefault(upd, round(float(pc), 4))
    return out


def main() -> None:
    secret = SECRET_FILE.read_text().strip()
    wh = sqlite3.connect(DB, timeout=60)
    dates = [r[0] for r in wh.execute(
        "SELECT DISTINCT trade_date FROM screener_history "
        "ORDER BY trade_date")]
    smap = sector_map()
    ipct = index_pcts()
    sessions = []
    for day in dates:
        rows = wh.execute(
            "SELECT symbol, pct_change, market_cap_vnd FROM "
            "screener_history WHERE trade_date=? AND exchange='HOSE'",
            (day,)).fetchall()
        rs = [(s, pc / 100.0, mc) for s, pc, mc in rows if pc is not None]
        if not rs:
            continue
        adv = sum(1 for _, r, _ in rs if r > 0)
        dec = sum(1 for _, r, _ in rs if r < 0)
        ew = sum(r for _, r, _ in rs) / len(rs) * 100
        wsum = cnum = 0.0
        n_mcap = 0
        sec_w: dict[str, float] = {}
        sec_n: dict[str, float] = {}
        for s, r, mc in rs:
            if mc and r > -1:
                w = mc / (1 + r)
                wsum += w
                cnum += w * r
                n_mcap += 1
                code = smap.get(s)
                if code:
                    sec_w[code] = sec_w.get(code, 0.0) + w
                    sec_n[code] = sec_n.get(code, 0.0) + w * r
        cw = (cnum / wsum * 100) if wsum else None
        sec_ret = {c: sec_n[c] / sec_w[c] * 100 for c in sec_w if sec_w[c]}
        ll = None
        top = bottom = None
        if sec_ret:
            top = max(sec_ret.items(), key=lambda kv: kv[1])
            bottom = min(sec_ret.items(), key=lambda kv: kv[1])
            ll = round(top[1] - bottom[1], 4)
        ip = ipct.get(day)
        sessions.append({
            "date": day,
            "index_pct": ip,
            "ew_pct": round(ew, 4),
            "cw_self_pct": round(cw, 4) if cw is not None else None,
            "cw_ew_spread_pp": (round(ip - ew, 4) if ip is not None
                                else None),
            "pct_advancers": (round(adv / (adv + dec) * 100, 2)
                              if (adv + dec) else None),
            "advancers": adv, "decliners": dec,
            "n_symbols": len(rs),
            "mcap_coverage_pct": round(n_mcap / len(rs) * 100, 1),
            "leader_laggard_spread_pp": ll,
            "sectors_up": sum(1 for v in sec_ret.values() if v > 0),
            "sectors_total": len(sec_ret),
            "top_sector": ({"code": top[0], "ret_pct": round(top[1], 4)}
                           if top else None),
            "bottom_sector": ({"code": bottom[0],
                               "ret_pct": round(bottom[1], 4)}
                              if bottom else None),
        })
    wh.close()
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "updated": now,
        "_prov": {"source": "warehouse screener_history + R2 ohlcv "
                            "VNINDEX + R2 sectors",
                  "retrieved_at": now},
        "universe": "HOSE (screener_history)",
        "method": {
            "cw_ew_spread": "index_pct - ew_pct (diem %)",
            "cw_self": "trong so von hoa dau phien suy nguoc "
                       "mcap_t/(1+r_i) — xap xi full-mcap, kho khong "
                       "co f_i/c_i cua HOSE (QD 714/QD-SGDHCM)",
        },
        "sessions": sessions[-25:],
    }
    rr = post_chunk(secret, {"divergence": doc}, source="divergence")
    print(f"divergence: {rr}; {len(doc['sessions'])} phien, "
          f"moi nhat: {doc['sessions'][-1] if doc['sessions'] else None}",
          flush=True)


if __name__ == "__main__":
    main()
