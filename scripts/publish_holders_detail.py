#!/usr/bin/env python3
"""Publish CHI TIET CO DONG THEO MA cho MCP analyze_holders (O-1, GD2).

Nguon: warehouse.db — shareholder_holding (snapshot moi nhat) +
shareholder_holding_history (snapshot ngay thu truoc do cua chinh ma).
Moi ma 1 file market/holders_detail/<SYM>.json:
  {symbol, updated, current: {snapshot_date, rows:[...]},
   previous: {snapshot_date, rows:[...]} | null,
   shares_outstanding}
Row: {name, norm, type, position, qty, pct (PHAN SO 0..1),
      record_date}. pct giu nguyen phan so nhu kho; Worker tinh %.

Kem shares_outstanding tu screener_history moi nhat (phuc vu doi
chieu pha loang). Chay sau publish_holders.py trong holder_cycle.sh:
  .venv5x/bin/python scripts/publish_holders_detail.py [--limit N]
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))
from publish_snapshot import post_chunk  # noqa: E402

DB = "data/warehouse.db"
SECRET_FILE = Path.home() / ".stockapp_publish"
CHUNK = 25


def _rows(con, table, sym, snap_date=None):
    q = (f"SELECT owner_name, owner_name_norm, owner_type, position_name,"
         f" quantity, pct, record_date FROM {table} WHERE symbol=?")
    args = [sym]
    if snap_date:
        q += " AND snapshot_date=?"
        args.append(snap_date)
    q += " ORDER BY quantity DESC"
    out = []
    for name, norm, typ, pos, qty, pct, rd in con.execute(q, args):
        out.append({"name": name, "norm": norm, "type": typ,
                    "position": pos, "qty": qty, "pct": pct,
                    "record_date": rd})
    return out


def build_payloads(con):
    now = datetime.now(timezone.utc).isoformat()
    payloads = {}
    syms = [r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM shareholder_holding ORDER BY symbol")]
    shares = dict(con.execute(
        "SELECT symbol, shares_outstanding FROM screener_history "
        "WHERE (symbol, trade_date) IN (SELECT symbol, MAX(trade_date) "
        "FROM screener_history GROUP BY symbol)").fetchall())
    for sym in syms:
        dates = [r[0] for r in con.execute(
            "SELECT DISTINCT snapshot_date FROM "
            "shareholder_holding_history WHERE symbol=? "
            "ORDER BY snapshot_date DESC LIMIT 2", (sym,))]
        cur_date = dates[0] if dates else None
        cur_rows = _rows(con, "shareholder_holding", sym)
        prev = None
        if len(dates) >= 2:
            prev = {"snapshot_date": dates[1],
                    "rows": _rows(con, "shareholder_holding_history",
                                  sym, dates[1])}
        payloads[sym] = {
            "symbol": sym, "updated": now,
            "_prov": {"source": "vci_iq via warehouse",
                      "retrieved_at": now},
            "current": {"snapshot_date": cur_date, "rows": cur_rows},
            "previous": prev,
            "shares_outstanding": shares.get(sym),
        }
    return payloads


def main() -> None:
    limit = 0
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    secret = SECRET_FILE.read_text().strip()
    con = sqlite3.connect(DB, timeout=60)
    payloads = build_payloads(con)
    con.close()
    syms = sorted(payloads)
    if limit:
        syms = syms[:limit]
    tasks = []
    for i in range(0, len(syms), CHUNK):
        chunk = {s: payloads[s] for s in syms[i:i + CHUNK]}
        tasks.append((f"holders_detail chunk {i // CHUNK + 1} "
                      f"({len(chunk)} ma)", {"holders_detail": chunk}))
    lock = threading.Lock()
    failed, ok = [], 0

    def _one(task):
        nonlocal ok
        label, payload = task
        rr = post_chunk(secret, payload, source="warehouse-holders-detail")
        with lock:
            if rr is None:
                failed.append(label)
                print(f"{label}: THAT BAI (het retry)", flush=True)
            else:
                ok += 1
                print(f"{label}: {rr}", flush=True)

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(_one, t) for t in tasks]
        for f in as_completed(futs):
            f.result()
    n_prev = sum(1 for s in syms if payloads[s]["previous"])
    print(f"XONG holders_detail: {ok}/{len(tasks)} chunk ok, "
          f"{len(syms)} ma ({n_prev} ma co ky truoc), "
          f"that bai: {failed or 'khong'}", flush=True)


if __name__ == "__main__":
    main()
