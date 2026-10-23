#!/usr/bin/env python3
"""Chup lich su screener theo phien vao warehouse.db (bang
screener_history) + ban sao nguyen trang tren R2.

Dac ta: goals/vietnam-stock-web-app/files/
  screener-history-spec-2026-10-23.md (MỚI-1)
Bo ca: screener-history-testcases-2026-10-23.md (SH-1..SH-11)

Nguon chup: ban screener TOAN VU TRU da publish tren R2
(stock/market/screener.json). Cong vu tru: < 1000 dong -> FAIL
(SH-11: ban lam giau ~500 dong khong duoc tinh la phien hoan chinh).

Truong suy tai luc chup:
- shares_outstanding = NPAT_TTM / EPS (dung cach publisher nganh
  publish_sectors.py dang dung: NPAT = IS_NET_PROFIT_AFTER_TAX TTM
  4 quy day du tu statements_raw; EPS = fundamentals_cache.eps > 0;
  chi suy khi NPAT_TTM > 0).
- FALLBACK (23/10, theo diem (a) dau moi): ma khong du NPAT_TTM
  theo 4 quy (UPCOM chu yeu chi nop BCTC nam) thi dung LNST nam
  GAN NHAT (period 'YYYY', value > 0) / EPS -> shares, ghi
  source_note='derived_annual_npat' (sai so vintage ~±20% theo
  kiem chung: median ti le annual/qTTM = 0,817, n=17 — dung duoc
  cho muc dich dong gop theo ma, KHONG dung cho dinh gia).
- Ma khong suy duoc -> shares/market_cap de NULL +
  source_note='shares_unavailable' (khong bia so).
- market_cap_vnd = close (nghin dong) * 1000 * shares_outstanding,
  source_note='derived_close_x_shares'.

Idempotent theo khoa (trade_date, symbol): INSERT ... ON CONFLICT
DO UPDATE (SH-2). Ngay khong giao dich / khong co snapshot: ket
thuc em, 0 dong moi (SH-4).

Che do chup bu: --backfill YYYY-MM-DD — close lay tu nen ngay cua
chinh phien do (cache ohlcv cua publisher Dot C), backfilled=1,
source_note='backfill_ohlcv' (SH-7). pct_change/volume/value_vnd
khong co nguon rieng cua phien do -> NULL (khong dap so phien khac).

Chay:
  .venv5x/bin/python scripts/screener_history_capture.py
  .venv5x/bin/python scripts/screener_history_capture.py --backfill 2026-10-22
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.expanduser("~/workspace/skills/cloudflare/bin"))
sys.path.insert(1, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DB = os.path.join(ROOT, "data", "app.db")
WAREHOUSE_DB = os.path.join(ROOT, "data", "warehouse.db")
OHLCV_CACHE = os.path.expanduser(
    "~/workspace/goals/vietnam-stock-web-app/hidden_files/dotc-cache/ohlcv")
API = "https://api.cloudflare.com/client/v4"
ACCT = "25b2327e932d26e5487c2ba8ad058932"
BUCKET = "english-lab-data"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 "
      "Safari/537.36")
SRC_KEY = "stock/market/screener.json"
MIN_UNIVERSE = 1000  # cong du vu tru (SH-11)

DDL = """
CREATE TABLE IF NOT EXISTS screener_history (
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT,
    close REAL,
    pct_change REAL,
    volume INTEGER,
    value_vnd REAL,
    shares_outstanding REAL,
    market_cap_vnd INTEGER,
    pe REAL,
    pb REAL,
    captured_at TEXT NOT NULL,
    backfilled INTEGER NOT NULL DEFAULT 0,
    source_note TEXT,
    PRIMARY KEY (trade_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_screener_history_symbol
    ON screener_history (symbol, trade_date);
"""

UPSERT = """
INSERT INTO screener_history (
    trade_date, symbol, exchange, close, pct_change, volume,
    value_vnd, shares_outstanding, market_cap_vnd, pe, pb,
    captured_at, backfilled, source_note)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(trade_date, symbol) DO UPDATE SET
    exchange=excluded.exchange, close=excluded.close,
    pct_change=excluded.pct_change, volume=excluded.volume,
    value_vnd=excluded.value_vnd,
    shares_outstanding=excluded.shares_outstanding,
    market_cap_vnd=excluded.market_cap_vnd,
    pe=excluded.pe, pb=excluded.pb,
    captured_at=excluded.captured_at,
    backfilled=excluded.backfilled,
    source_note=excluded.source_note;
"""


def cf_req(method, key, data=None):
    url = f"{API}/accounts/{ACCT}/r2/buckets/{BUCKET}/objects/{key}"
    last = None
    for att in range(6):
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("User-Agent", UA)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        add_surrogate_to_request(req, "custom.cloudflare",
                                 entry_name="access_token",
                                 allowed_hosts=["api.cloudflare.com"])
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return 404, b""
            last = e
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(2 * (att + 1))
    raise RuntimeError(f"CF {method} {key} fail: {last}")


def derive_shares(app_db_path, warehouse_db_path=WAREHOUSE_DB):
    """(shares, annual_syms) — sao dung publish_sectors.py:
    - Ky quy 'hoan chinh' xac dinh tu do phu sector_financials
      (warehouse.db): ky co tong n_members >= 80% dinh phu.
    - NPAT_TTM = SUM(IS_NET_PROFIT_AFTER_TAX) tren 4 ky hoan chinh
      gan nhat (period IN last4, GROUP BY symbol; thieu ky thi
      tong tren ky co mat — dung hanh vi ban goc).
    - shares = NPAT_TTM / EPS (fundamentals_cache.eps > 0),
      chi khi NPAT_TTM > 0.
    - FALLBACK: ma chua co shares va eps > 0 thi lay LNST nam
      gan nhat (period ~ '^YYYY$', value > 0): shares =
      annual_npat / eps; dua vao annual_syms de capture ghi
      source_note='derived_annual_npat'."""
    import re as _re
    wh = sqlite3.connect(warehouse_db_path)
    all_periods = [r[0] for r in wh.execute(
        "SELECT DISTINCT period FROM sector_financials")]
    nmem = {}
    for c, p, n in wh.execute(
            "SELECT sector_code, period, n_members "
            "FROM sector_financials"):
        nmem[(c, p)] = n
    wh.close()
    q_periods = sorted(p for p in all_periods
                       if _re.fullmatch(r"\d{4}-Q[1-4]", p or ""))
    cover = {p: sum(n for (c, pp), n in nmem.items() if pp == p)
             for p in q_periods}
    peak = max(cover.values()) if cover else 0
    complete = [p for p in q_periods if cover[p] >= 0.8 * peak]
    latest_complete = complete[-1] if complete else q_periods[-1]
    periods = [p for p in q_periods if p <= latest_complete]
    last4 = periods[-4:]
    app = sqlite3.connect(app_db_path)
    eps = {s: e for s, e in app.execute(
        "SELECT symbol, eps FROM fundamentals_cache "
        "WHERE eps IS NOT NULL AND eps > 0")}
    shares = {}
    annual_syms = set()
    if len(last4) == 4:
        q = ",".join("?" * 4)
        for sym, tot in app.execute(
                "SELECT symbol, SUM(value) FROM statements_raw "
                f"WHERE field_id='IS_NET_PROFIT_AFTER_TAX' "
                f"AND period IN ({q}) AND value IS NOT NULL "
                "GROUP BY symbol", last4):
            if tot and tot > 0 and sym in eps:
                shares[sym] = tot / eps[sym]
    # Fallback annual cho ma chi co BCTC nam (UPCOM)
    for sym, e in eps.items():
        if sym in shares:
            continue
        row = app.execute(
            "SELECT value FROM statements_raw "
            "WHERE symbol=? AND field_id='IS_NET_PROFIT_AFTER_TAX' "
            "AND period GLOB '2???' AND period NOT GLOB '*-Q*' "
            "AND value IS NOT NULL AND value > 0 "
            "ORDER BY period DESC LIMIT 1", (sym,)).fetchone()
        if row:
            shares[sym] = row[0] / e
            annual_syms.add(sym)
    app.close()
    return shares, annual_syms


def session_date_of(doc):
    upd = doc.get("updated") or doc.get("as_of") or ""
    dt = datetime.fromisoformat(str(upd).replace("Z", "+00:00"))
    return (dt.astimezone(timezone.utc)
            + timedelta(hours=7)).strftime("%Y-%m-%d")


def capture_snapshot(doc, wh, shares, captured_at,
                     annual_syms=frozenset()):
    """Chup 1 snapshot (dict co rows + updated). Tra (trade_date, n)."""
    rows = doc.get("rows") or []
    if len(rows) < MIN_UNIVERSE:
        raise ValueError(
            f"cong vu tru: snapshot chi co {len(rows)} dong "
            f"(< {MIN_UNIVERSE}) — khong phai ban toan vu tru (SH-11)")
    day = session_date_of(doc)
    n = 0
    n_derived = 0
    for r in rows:
        sym = r.get("symbol")
        if not sym:
            continue
        close = r.get("close")
        sh = shares.get(sym)
        if sh is not None and close is not None:
            mcap = int(round(close * 1000 * sh))
            note = ("derived_annual_npat" if sym in annual_syms
                    else "derived_close_x_shares")
            n_derived += 1
        else:
            sh = None
            mcap = None
            note = "shares_unavailable"
        wh.execute(UPSERT, (
            day, sym, r.get("exchange"), close, r.get("pct_change"),
            r.get("volume"), r.get("value_vnd"), sh, mcap,
            r.get("pe_ttm") or r.get("pe"), r.get("pb"),
            captured_at, 0, note))
        n += 1
    wh.commit()
    return day, n, n_derived


def backfill_day(day, wh, shares, captured_at, cache_dir=OHLCV_CACHE):
    """Chup bu 1 phien tu nen ngay cua chinh phien do (SH-7)."""
    n = 0
    for fn in sorted(os.listdir(cache_dir)):
        if not fn.endswith(".json"):
            continue
        sym = fn[:-5]
        try:
            obj = json.load(open(os.path.join(cache_dir, fn)))
        except Exception:  # noqa: BLE001
            continue
        close = None
        for row in obj.get("rows") or []:
            if row.get("t") == day and row.get("c") is not None:
                close = float(row["c"])
                break
        if close is None:
            continue
        sh = shares.get(sym)
        mcap = int(round(close * 1000 * sh)) if sh else None
        wh.execute(UPSERT, (
            day, sym, None, close, None, None, None, sh, mcap,
            None, None, captured_at, 1, "backfill_ohlcv"))
        n += 1
    wh.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", metavar="YYYY-MM-DD")
    ap.add_argument("--no-r2-copy", action="store_true",
                    help="bo qua ban sao R2 screener_history/<date>.json")
    args = ap.parse_args()
    captured_at = datetime.now(timezone.utc).isoformat()
    wh = sqlite3.connect(WAREHOUSE_DB)
    wh.executescript(DDL)
    shares, annual_syms = derive_shares(APP_DB)
    print(f"screener_history: suy duoc shares cho {len(shares)} ma "
          f"(trong do annual-fallback {len(annual_syms)})")

    if args.backfill:
        n = backfill_day(args.backfill, wh, shares, captured_at)
        print(f"screener_history: backfill {args.backfill}: {n} dong "
              f"(backfilled=1, source_note=backfill_ohlcv)")
        wh.close()
        return 0

    status, raw = cf_req("GET", SRC_KEY)
    if status != 200:
        print(f"screener_history: khong co snapshot nguon "
              f"(HTTP {status}) — 0 dong moi")
        wh.close()
        return 0
    doc = json.loads(raw.decode())
    try:
        day, n, n_derived = capture_snapshot(doc, wh, shares,
                                             captured_at,
                                             annual_syms=annual_syms)
    except ValueError as e:
        print(f"screener_history: FAIL — {e}")
        wh.close()
        return 1
    print(f"screener_history: {day}: {n} dong "
          f"(suy von hoa {n_derived}, thieu shares {n - n_derived})")
    if not args.no_r2_copy:
        dst = f"stock/market/screener_history/{day}.json"
        st, existing = cf_req("GET", dst)
        if st == 200 and len(existing) == len(raw):
            print(f"screener_history: R2 {dst} da co — bo qua")
        else:
            st, _ = cf_req("PUT", dst, data=raw)
            print(f"screener_history: R2 {dst} put={st} "
                  f"({len(raw)} B)")
    wh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
