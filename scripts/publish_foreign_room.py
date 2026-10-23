#!/usr/bin/env python3
"""Publish ROOM NGOAI THEO MA cho MCP analyze_holders (O-1, khoi
foreign_room — H-3: tran theo MA da thong bao, cam suy tu nganh).

Nguon tran: bang gia KBS — FO = han muc so huu NN theo ma (CP),
FR = room con lai (CP). Da doi chieu 21/10/2026: FO/shares khop
tran cong bo tung ma (FPT 49,00%, VCB 30,00% — khop tung CP).
fol_max_pct = FO / shares_outstanding * 100 (shares tu
screener_history moi nhat cua kho). FO=0 la du lieu that (tran 0%),
chi bo ma khi KBS khong tra FO hoac thieu shares.

Publish 1 file market/foreign_room.json: {updated, as_of, rows:
{SYM: {limit_shares, room_shares, owned_shares, shares_outstanding,
fol_max_pct, foreign_owned_pct, exchange}}, coverage}.

Chay: .venv5x/bin/python scripts/publish_foreign_room.py
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))
from direct_fetch import kbs_price_board  # noqa: E402
from publish_snapshot import post_chunk  # noqa: E402

DB = "data/warehouse.db"
APP_DB = "data/app.db"
SECRET_FILE = Path.home() / ".stockapp_publish"
CHUNK = 100


def _iso_date(board_date: str | None) -> str | None:
    if not board_date:
        return None
    parts = str(board_date).split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    return str(board_date)[:10]


def main() -> None:
    secret = SECRET_FILE.read_text().strip()
    app = sqlite3.connect(APP_DB)
    listing = {r[0]: r[1] for r in app.execute(
        "SELECT symbol, exchange FROM listing_cache")}
    app.close()
    wh = sqlite3.connect(DB, timeout=60)
    shares = dict(wh.execute(
        "SELECT symbol, shares_outstanding FROM screener_history "
        "WHERE (symbol, trade_date) IN (SELECT symbol, MAX(trade_date) "
        "FROM screener_history GROUP BY symbol)").fetchall())
    wh.close()

    syms = sorted(listing)
    board: dict[str, dict] = {}
    for i in range(0, len(syms), CHUNK):
        part = syms[i:i + CHUNK]
        for attempt in range(3):
            try:
                board.update(kbs_price_board(part))
                break
            except Exception as e:
                print(f"board chunk {i // CHUNK + 1}: loi lan "
                      f"{attempt + 1}: {str(e)[:80]}", flush=True)
                time.sleep(3)
        time.sleep(0.2)

    rows: dict[str, dict] = {}
    as_of = None
    n_no_fo = n_no_shares = 0
    for sym in syms:
        q = board.get(sym) or {}
        fo = q.get("foreign_limit_shares")
        fr = q.get("foreign_room_shares")
        if not isinstance(fo, (int, float)) or not isinstance(fr, (int, float)):
            n_no_fo += 1
            continue
        sh = shares.get(sym)
        if not sh:
            n_no_shares += 1
            continue
        as_of = as_of or _iso_date(q.get("date"))
        rows[sym] = {
            "limit_shares": int(fo),
            "room_shares": int(fr),
            "owned_shares": int(fo - fr),
            "shares_outstanding": int(sh),
            "fol_max_pct": round(fo / sh * 100, 4),
            "foreign_owned_pct": round((fo - fr) / sh * 100, 4),
            "exchange": listing.get(sym),
        }
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "updated": now, "as_of": as_of,
        "_prov": {"source": "kbs price board (FO/FR) + warehouse "
                            "screener_history shares",
                  "retrieved_at": now},
        "rows": rows,
        "coverage": {"with_data": len(rows), "total": len(syms),
                     "no_board_fo": n_no_fo, "no_shares": n_no_shares},
    }
    rr = post_chunk(secret, {"foreign_room": doc}, source="foreign-room")
    print(f"foreign_room: {rr}; {len(rows)}/{len(syms)} ma "
          f"(thieu FO: {n_no_fo}, thieu shares: {n_no_shares}), "
          f"as_of={as_of}", flush=True)


if __name__ == "__main__":
    main()
