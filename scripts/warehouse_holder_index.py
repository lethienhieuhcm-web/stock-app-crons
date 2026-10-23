#!/usr/bin/env python3
"""Lop dao chi muc co dong (HOLDERSTOCKHOLD) - co che THAM SO HOA
(quyet dinh 8, checklist Dot 1 muc c): mot bang holdings chuan hoa
tu VCI IQ /shareholder, dao nguoc theo ten co dong chuan hoa.

- warehouse_holder_collect.py: thu dien rong 1.519 ma, chunk +
  resumable (state JSON), commit tung ma.
- warehouse_holder_index.py (file nay): rebuild summary + truy van:
  holdings_of(name) / holders_of(symbol) / top_holders(n).

Chuan hoa ten: bo dau + viet hoa + gon khoang trang (khop bai hoc
parity Dot 4: cung mot nguoi co the xuat hien 2 dong khac so luong
- giu nguyen tung dong, khong gop).
"""
import re
import sqlite3
import sys
import unicodedata

DB = "data/warehouse.db"
APP_DB = "data/app.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS shareholder_holding(
  symbol TEXT NOT NULL,
  owner_name TEXT NOT NULL,
  owner_name_norm TEXT NOT NULL,
  owner_code TEXT,
  owner_type TEXT,
  position_name TEXT,
  quantity INTEGER,
  pct REAL,
  update_date TEXT,
  record_date TEXT,
  fetched_at TEXT NOT NULL,
  PRIMARY KEY(symbol, owner_name_norm, quantity, update_date)
);
CREATE TABLE IF NOT EXISTS shareholder_holding_history(
  snapshot_date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  owner_name TEXT NOT NULL,
  owner_name_norm TEXT NOT NULL,
  owner_code TEXT,
  owner_type TEXT,
  position_name TEXT,
  quantity INTEGER,
  pct REAL,
  update_date TEXT,
  record_date TEXT,
  fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hist_symbol
  ON shareholder_holding_history(snapshot_date, symbol);
CREATE INDEX IF NOT EXISTS idx_hist_owner
  ON shareholder_holding_history(owner_name_norm, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_holding_owner
  ON shareholder_holding(owner_name_norm);
CREATE INDEX IF NOT EXISTS idx_holding_symbol
  ON shareholder_holding(symbol);
CREATE TABLE IF NOT EXISTS holder_summary(
  owner_name_norm TEXT PRIMARY KEY,
  owner_name TEXT NOT NULL,
  owner_type TEXT,
  n_symbols INTEGER NOT NULL,
  total_quantity INTEGER NOT NULL,
  built_at TEXT NOT NULL
);
"""


def norm_name(name: str) -> str:
    s = unicodedata.normalize("NFD", name or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"\s+", " ", s.strip().upper())


def _record_date(update_date) -> str | None:
    """record_date = phan ngay cua updateDate nguon (YYYY-MM-DD).

    updateDate cua VCI IQ la moc nguon cua tung dong so huu (2 dang:
    ngay thuan 2015-06-15T00:00:00 va timestamp 2016-08-26T14:38:47.8);
    phan ngay la ngay chot cua dong. Khong hop le -> None.
    """
    if not update_date:
        return None
    s = str(update_date)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return None


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(SCHEMA)
    cols = [r[1] for r in con.execute(
        "PRAGMA table_info(shareholder_holding)")]
    if "record_date" not in cols:
        con.execute("ALTER TABLE shareholder_holding "
                    "ADD COLUMN record_date TEXT")
    # backfill idempotent cho dong cu
    con.execute(
        "UPDATE shareholder_holding SET record_date = "
        "substr(update_date, 1, 10) WHERE record_date IS NULL "
        "AND update_date IS NOT NULL AND length(update_date) >= 10 "
        "AND substr(update_date, 5, 1) = '-' "
        "AND substr(update_date, 8, 1) = '-'")
    con.commit()


def upsert_symbol(con: sqlite3.Connection, symbol: str,
                  rows: list[dict], fetched_at: str) -> int:
    """Thay toan bo dong cua 1 ma bang bo moi (nguon la snapshot
    moi nhat cua IQ). Tra so dong da ghi."""
    con.execute("DELETE FROM shareholder_holding WHERE symbol=?",
                (symbol,))
    snap_date = (fetched_at or "")[:10]
    con.execute("DELETE FROM shareholder_holding_history "
                "WHERE snapshot_date=? AND symbol=?",
                (snap_date, symbol))
    n = 0
    for r in rows:
        name = (r.get("ownerName") or "").strip()
        if not name:
            continue
        ud = r.get("updateDate")
        rd = _record_date(ud)
        vals = (symbol, name, norm_name(name), r.get("ownerCode"),
                r.get("ownerType"), r.get("positionName"),
                r.get("quantity"), r.get("percentage"), ud, rd,
                fetched_at)
        con.execute(
            "INSERT OR REPLACE INTO shareholder_holding "
            "(symbol, owner_name, owner_name_norm, owner_code, "
            "owner_type, position_name, quantity, pct, update_date, "
            "record_date, fetched_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?)", vals)
        con.execute(
            "INSERT INTO shareholder_holding_history "
            "(snapshot_date, symbol, owner_name, owner_name_norm, "
            "owner_code, owner_type, position_name, quantity, pct, "
            "update_date, record_date, fetched_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?)", (snap_date,) + vals)
        n += 1
    con.commit()
    return n


def rebuild_summary(con: sqlite3.Connection, built_at: str) -> int:
    con.execute("DELETE FROM holder_summary")
    con.execute(
        "INSERT INTO holder_summary SELECT owner_name_norm, "
        "MAX(owner_name), MAX(owner_type), COUNT(DISTINCT symbol), "
        "COALESCE(SUM(quantity),0), ? FROM shareholder_holding "
        "GROUP BY owner_name_norm", (built_at,))
    con.commit()
    return con.execute("SELECT COUNT(*) FROM holder_summary"
                       ).fetchone()[0]


# ---- Tang truy van tham so hoa (15 ham HOLDERSTOCKHOLD cua Wi
# deu la bien the tham so cua 3 ham nay: theo loai co dong, top N,
# nguong so huu) ----

def holdings_of(con: sqlite3.Connection, name_query: str,
                owner_type: str | None = None) -> list[dict]:
    q = norm_name(name_query)
    sql = ("SELECT symbol, owner_name, owner_type, quantity, pct, "
           "update_date FROM shareholder_holding "
           "WHERE owner_name_norm LIKE ?")
    args: list = [f"%{q}%"]
    if owner_type:
        sql += " AND owner_type=?"
        args.append(owner_type)
    sql += " ORDER BY quantity DESC"
    return [dict(zip(
        ("symbol", "owner_name", "owner_type", "quantity", "pct",
         "update_date"), r))
        for r in con.execute(sql, args)]


def holders_of(con: sqlite3.Connection, symbol: str,
               owner_type: str | None = None) -> list[dict]:
    sql = ("SELECT owner_name, owner_type, position_name, quantity, "
           "pct, update_date FROM shareholder_holding "
           "WHERE symbol=?")
    args: list = [symbol.upper()]
    if owner_type:
        sql += " AND owner_type=?"
        args.append(owner_type)
    sql += " ORDER BY quantity DESC"
    return [dict(zip(
        ("owner_name", "owner_type", "position_name", "quantity",
         "pct", "update_date"), r))
        for r in con.execute(sql, args)]


def top_holders(con: sqlite3.Connection, min_symbols: int = 5,
                limit: int = 30) -> list[dict]:
    return [dict(zip(
        ("owner_name", "owner_type", "n_symbols", "total_quantity"),
        r)) for r in con.execute(
            "SELECT owner_name, owner_type, n_symbols, "
            "total_quantity FROM holder_summary "
            "WHERE n_symbols>=? ORDER BY n_symbols DESC, "
            "total_quantity DESC LIMIT ?", (min_symbols, limit))]


if __name__ == "__main__":
    con = sqlite3.connect(DB)
    ensure_schema(con)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "stats"
    if cmd == "rebuild":
        from datetime import datetime, timezone
        n = rebuild_summary(
            con, datetime.now(timezone.utc).isoformat())
        print(f"holder_summary: {n} co dong")
    elif cmd == "holdings" and len(sys.argv) > 2:
        for r in holdings_of(con, " ".join(sys.argv[2:])):
            print(r)
    elif cmd == "holders" and len(sys.argv) > 2:
        for r in holders_of(con, sys.argv[2]):
            print(r)
    elif cmd == "top":
        for r in top_holders(con):
            print(r)
    else:
        h = con.execute("SELECT COUNT(*) FROM shareholder_holding"
                        ).fetchone()[0]
        s = con.execute("SELECT COUNT(DISTINCT symbol) FROM "
                        "shareholder_holding").fetchone()[0]
        print(f"holdings: {h} dong / {s} ma")
