#!/usr/bin/env python3
"""Fetch truc tiep tu upstream API, KHONG qua vnstock.

Nguon (dang la public API ma web/app cua chinh cac cong ty chung khoan goi):
- VietCap IQ:  https://iq.vietcap.com.vn/api/iq-insight-service/v1/...
- KBS IIS:     https://kbbuddywts.kbsec.com.vn/iis-server/investment/...

Phat hien tu viec mo binary vnstock 5.1.0 (_core.abi3.so) + source 4.0.9.
Muc dich: tu chu du lieu (chien luoc 4 lop) — vnstock chi la "duong ong thay duoc".

Quy tac:
- Chi GET cac endpoint public, volume thap (nhu trinh duyet thuong).
- Khong vuot auth, khong ne anti-bot: endpoint nao doi auth thi bo qua, ghi ro.
- Tra ve list/dict thuan Python (khong phu thuoc pandas).

Tai lieu chi tiet: ~/workspace/your_files/vnstock-v5-binary-chitiet.md
"""

from __future__ import annotations

import json
import os
import time
import http.client
import urllib.request
import urllib.parse

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
TIMEOUT = 25


def _get(url: str, params: dict | None = None, retries: int = 2) -> dict:
    """GET JSON, tu dong giai nen gzip, retry don gian."""
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                },
            )
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                try:
                    raw = resp.read()
                except http.client.IncompleteRead as e:
                    raw = e.partial  # proxy cat connection — lay phan da nhan
                enc = resp.headers.get("Content-Encoding", "")
                if "gzip" in enc:
                    import gzip

                    try:
                        raw = gzip.decompress(raw)
                    except Exception:
                        # gzip dut doan -> bo qua lan nay, retry
                        raise
                return json.loads(raw.decode("utf-8"))
        except Exception as e:  # noqa: BLE001 - retry moi thu
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET failed after {retries + 1} tries: {url}: {last_err}")


# ---------------------------------------------------------------- VietCap IQ

_VCI_IQ = "https://iq.vietcap.com.vn/api/iq-insight-service/v1"


def vci_index_history(index: str = "VNINDEX", limit: int = 30) -> list[dict]:
    """Lich su chi so: ngay, open/high/low/close, volume, value.

    Tuong duong route: market.index.ohlcv / market.index.price_history
    """
    data = _get(
        f"{_VCI_IQ}/market-indices/history",
        {"index": index, "page": 0, "size": limit},
    )
    content = (data.get("data") or {}).get("content") or []
    out = []
    for r in content:
        out.append(
            {
                "date": r.get("tradingDate"),
                "open": r.get("openIndex"),
                "high": r.get("highestIndex"),
                "low": r.get("lowestIndex"),
                "close": r.get("closeIndex") or r.get("indexValue"),
                "change": r.get("indexChange"),
                "pct_change": r.get("percentIndexChange"),
                "volume": r.get("totalMatchVolume"),
                "value": r.get("totalMatchValue"),
            }
        )
    return out


_DCHART = "https://dchart-api.vndirect.com.vn/dchart/history"
# Chi so app -> ma tren VNDirect dchart (HNX/UPCOM khong co hau to "Index")
_VND_INDEX_SYMBOLS = [
    ("VNINDEX", "VNINDEX"),
    ("VN30", "VN30"),
    ("HNXIndex", "HNX"),
    ("UPCOMIndex", "UPCOM"),
]

# Chi so nganh HOSE tren dchart (ten app = ten dchart)
_VND_SECTOR_SYMBOLS = [
    ("VNFIN", "VNFIN"),
    ("VNREAL", "VNREAL"),
    ("VNIT", "VNIT"),
    ("VNIND", "VNIND"),
    ("VNMAT", "VNMAT"),
    ("VNCONS", "VNCONS"),
]


def _dchart_index_rows(symbols) -> list[dict]:
    """Snapshot chi so tu VNDirect dchart (feed bieu do truc tiep, cap
    nhat theo phut).

    2026-10-20: thay vci_index_history cho khau realtime — VCI IQ
    history la feed EOD (cham 1 ngay, sap xep GIAM dan moi nhat truoc)
    nen poller boc nham so cua phien cu lam so hom nay. dchart tra nen
    ngay (resolution=D): nen cuoi = hom nay (close = gia tri hien tai,
    volume = tong phien), nen truoc = phien hom qua lam moc tinh change.
    KY THUAT: chi gui User-Agent — gui kem "Accept: application/json"
    la server tra 406. Loi tung chi so thi bo qua chi so do.
    Tra ve rows dung contract app: {symbol, close, volume, time,
    change, pct_change} + prev_close/prev_date de poller doi chieu
    cheo voi VCI.
    """
    from datetime import datetime, timezone, timedelta

    now = int(time.time())
    frm = now - 10 * 86400
    out = []
    missing = []
    for app_sym, dc_sym in symbols:
        row = _dchart_one(app_sym, dc_sym, frm, now)
        if row:
            out.append(row)
        else:
            missing.append((app_sym, dc_sym))
    # Luot 2 cho ma rung (do that 23/10: goi lien tiep 10 ma thi moi
    # luot rung ngau nhien 1 ma — VN30/VNIND/VNIT/HNX thay nhau; goi
    # le thi tot). Nghi 1s roi thu lai tung ma con thieu.
    for app_sym, dc_sym in missing:
        time.sleep(1)
        row = _dchart_one(app_sym, dc_sym, frm, now)
        if row:
            out.append(row)
    return out


def _dchart_one(app_sym, dc_sym, frm, now):
    from datetime import datetime, timezone, timedelta

    url = f"{_DCHART}?symbol={dc_sym}&resolution=D&from={frm}&to={now}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            raw = resp.read()
            if "gzip" in resp.headers.get("Content-Encoding", ""):
                import gzip

                raw = gzip.decompress(raw)
        d = json.loads(raw)
        ts, cs, vs = d.get("t") or [], d.get("c") or [], d.get("v") or []
        if len(ts) < 2 or len(cs) < 2:
            return None
        vn = timezone(timedelta(hours=7))
        last_date = datetime.fromtimestamp(ts[-1], vn).date().isoformat()
        prev_date = datetime.fromtimestamp(ts[-2], vn).date().isoformat()
        close = float(cs[-1])
        prev_close = float(cs[-2])
        chg = round(close - prev_close, 2)
        pct = round(chg / prev_close * 100, 2) if prev_close else 0
        return {
            "symbol": app_sym,
            "close": close,
            "volume": float(vs[-1]) if vs else None,
            "time": last_date,
            "change": chg,
            "pct_change": pct,
            "prev_close": prev_close,
            "prev_date": prev_date,
        }
    except Exception:  # noqa: BLE001 — loi 1 chi so khong duoc lam chet cac chi so con lai
        return None


def vnd_index_snapshot() -> list[dict]:
    """Snapshot 4 chi so chinh (VNINDEX/VN30/HNX/UPCOM)."""
    return _dchart_index_rows(_VND_INDEX_SYMBOLS)


def vnd_sector_snapshot() -> list[dict]:
    """Snapshot 6 chi so nganh HOSE (VNFIN/VNREAL/VNIT/VNIND/VNMAT/
    VNCONS) — cung nguon dchart, cung contract voi chi so chinh.
    Them 23/10: poller truoc day khong publish khoa sector_indices nen
    file tren R2 dong cung o cac lan publish tay."""
    return _dchart_index_rows(_VND_SECTOR_SYMBOLS)


def vci_company(symbol: str) -> dict:
    """Snapshot cong ty: gia hien tai, von hoa... (khong auth)."""
    data = _get(f"{_VCI_IQ}/company/{symbol.upper()}")
    return data.get("data") or {}


# ---------------------------------------------------------------- KBS

_KBS_IIS = "https://kbbuddywts.kbsec.com.vn/iis-server/investment"

# Loai bao cao tai chinh KBS
REPORT_TYPES = {
    "CDKT": "balance_sheet",  # Can doi ke toan
    "KQKD": "income_statement",  # Ket qua kinh doanh
    "LCTT": "cash_flow",  # Luu chuyen tien te
    "CSTC": "ratios",  # Chi so tai chinh
}


def kbs_financial(
    symbol: str,
    report_type: str = "KQKD",
    period_type: int = 2,
    page_size: int = 4,
) -> dict:
    """Bao cao tai chinh tu KBS.

    report_type: CDKT | KQKD | LCTT | CSTC
    period_type: 1 = nam, 2 = quy
    Tra ve: {"periods": [...], "rows": [{"name", "name_en", "level", "values": [...]}, ...]}

    Moi values[i] ung voi periods[i] (Value1..ValueN theo thu tu Head).
    Don vi: nghin VND (unit=1000) — giong vnstock 4.0.9.
    """
    rt = report_type.upper()
    if rt not in REPORT_TYPES:
        raise ValueError(f"report_type phai la mot trong {list(REPORT_TYPES)}")
    params: dict = {
        "page": 1,
        "pageSize": page_size,
        "type": rt,
        "unit": 1000,
        "termtype": period_type,
        "languageid": 1,
    }
    if rt == "LCTT":
        # Cash flow dung ten param khac (theo source 4.0.9)
        params.pop("languageid")
        params["code"] = symbol.upper()
        params["termType"] = period_type
    data = _get(f"{_KBS_IIS}/stock/finance-info/{symbol.upper()}", params)

    periods = []
    for h in data.get("Head") or []:
        periods.append(
            {
                "term": h.get("TermCode"),
                "year": h.get("YearPeriod"),
                "begin": h.get("PeriodBegin"),
                "end": h.get("PeriodEnd"),
                "report_date": (h.get("ReportDate") or "")[:10],
                "united": h.get("United"),
                "audited": h.get("AuditedStatus"),
            }
        )
    rows = []
    content = data.get("Content") or {}
    for _component, items in content.items():
        for it in items or []:
            values = [it.get(f"Value{i + 1}") for i in range(len(periods))]
            rows.append(
                {
                    "name": (it.get("Name") or "").strip(),
                    "name_en": (it.get("NameEn") or "").strip(),
                    "level": it.get("Levels"),
                    "parent_id": it.get("ParentReportNormID"),
                    "values": values,
                }
            )
    return {"periods": periods, "rows": rows}


def kbs_find_row(fin: dict, keyword: str) -> dict | None:
    """Tim dong bao cao theo tu khoa (tieng Viet hoac Anh)."""
    kw = keyword.lower()
    for r in fin["rows"]:
        if kw in r["name"].lower() or kw in r["name_en"].lower():
            return r
    return None


# ---------------------------------------------------------------- CLI test nhanh

# ---------------------------------------------------------------- KBS OHLCV / price board / company

_KBS_INTERVAL = {
    "1m": "1P", "5m": "5P", "15m": "15P", "30m": "30P",
    "1h": "60P", "1d": "day", "1w": "week", "1M": "month",
}


def kbs_ohlcv(
    symbol: str,
    start: str,
    end: str,
    interval: str = "1d",
) -> list[dict]:
    """OHLCV co phieu tu KBS. start/end: 'YYYY-MM-DD'.

    Tuong duong route: market.equity.ohlcv
    """
    suffix = _KBS_INTERVAL.get(interval, "day")

    def fmt(d: str) -> str:  # YYYY-MM-DD -> DD-MM-YYYY
        y, m, dd = d.split("-")
        return f"{dd}-{m}-{y}"

    data = _get(
        f"{_KBS_IIS}/stocks/{symbol.upper()}/data_{suffix}",
        {"sdate": fmt(start), "edate": fmt(end)},
    )
    rows = data.get(f"data_{suffix}") or []
    return [
        {
            "time": r.get("t"),
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "close": r.get("c"),
            "volume": r.get("v"),
            "value": r.get("va"),
        }
        for r in rows
    ]


def _kbs_depth(r: dict, pk: str, vk: str):
    """3 buoc gia KBS (B/V mua, S/U ban) -> [[gia, KL], ...] chuan so.

    Sua goc 22/10/2026: truoc day adapter tra tuple tho (gia co the la
    chuoi "34150.0") va giu ca buoc chet (gia None/0) -> frontend
    Number(tuple) = NaN in thang ra trang chi tiet. Chuan moi: gia/KL
    la so that, buoc khong gia bi loai, ca phia rong -> None
    (missing_policy='na': thieu thi ghi thieu, khong bia so).
    """
    steps = []
    for i in (1, 2, 3):
        try:
            p = float(r.get(f"{pk}{i}"))
        except (TypeError, ValueError):
            continue
        if p <= 0:
            continue
        try:
            v = int(float(r.get(f"{vk}{i}")))
        except (TypeError, ValueError):
            v = None
        steps.append([int(p) if p == int(p) else p, v])
    return steps or None


def kbs_price_board(symbols: list[str]) -> dict[str, dict]:
    """Bang gia realtime tu KBS (POST /stock/iss).

    Tuong duong route: market.equity.quote (+ foreign_flow qua FB/FS)
    Tra ve dict {symbol: quote}, quote gom cac field chuan.
    """
    import urllib.request as _urlreq

    url = f"{_KBS_IIS}/stock/iss"
    body = json.dumps({"code": ",".join(s.upper() for s in symbols)}).encode()
    req = _urlreq.Request(
        url,
        data=body,
        headers={
            "User-Agent": UA,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "x-lang": "vi",
        },
    )
    last_err = None
    rows: list = []
    for _ in range(3):
        try:
            with _urlreq.urlopen(req, timeout=TIMEOUT) as resp:
                try:
                    raw = resp.read()
                except http.client.IncompleteRead as e:
                    raw = e.partial  # proxy cat connection — lay phan da nhan
                if "gzip" in resp.headers.get("Content-Encoding", ""):
                    import gzip

                    raw = gzip.decompress(raw)
                rows = json.loads(raw.decode("utf-8"))
            last_err = None
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2)
    if last_err is not None:
        raise RuntimeError(f"kbs_price_board failed: {last_err}")
    out: dict[str, dict] = {}
    for r in rows or []:
        sym = r.get("SB")
        if not sym:
            continue
        out[sym] = {
            "symbol": sym,
            "exchange": r.get("EX"),
            "date": r.get("TD"),
            "time": r.get("IT"),
            "reference": r.get("RE"),
            "floor": r.get("FL"),
            "ceiling": r.get("CL"),
            "open": r.get("OP"),
            "high": r.get("HI"),
            "low": r.get("LO"),
            "close": r.get("CP"),
            "change": r.get("CH"),
            "pct_change": r.get("CHP"),
            "volume": r.get("TT"),
            "value": r.get("TV"),
            "avg_price": r.get("AP"),
            "bid": _kbs_depth(r, "B", "V"),
            "ask": _kbs_depth(r, "S", "U"),
            # Khoi ngoai: FB/FS la KL mua/ban that cua phien.
            # FO/FR KHONG phai gia tri giao dich (sua goc 21/10/2026, doi
            # chieu so that: FPT FO = 49% x co phieu luu hanh, VCB FO =
            # 30% x luu hanh — khop tung co phieu): FO = han muc so huu NN
            # (CP), FR = room con lai (CP). Bang gia KBS khong co gia tri
            # mua/ban NN -> de None; noi tieu thu dien tu VCI.
            "foreign_buy_volume": r.get("FB"),
            "foreign_sell_volume": r.get("FS"),
            "foreign_buy_value": None,
            "foreign_sell_value": None,
            "foreign_limit_shares": r.get("FO"),
            "foreign_room_shares": r.get("FR"),
            "foreign_owned_shares": (
                r.get("FO") - r.get("FR")
                if isinstance(r.get("FO"), (int, float))
                and isinstance(r.get("FR"), (int, float))
                else None
            ),
        }
    return out


def kbs_company_profile(symbol: str) -> dict:
    """Ho so cong ty tu KBS. Tuong duong: fundamental.company.info"""
    data = _get(f"{_KBS_IIS}/stockinfo/profile/{symbol.upper()}")
    return data if isinstance(data, dict) else {}


def kbs_company_events(symbol: str, page_size: int = 10) -> list[dict]:
    """Su kien doanh nghiep tu KBS. Tuong duong: fundamental.company.events"""
    data = _get(
        f"{_KBS_IIS}/stockinfo/event/{symbol.upper()}",
        {"l": 1, "p": 1, "s": page_size},
    )
    return data if isinstance(data, list) else []


# ---------------------------------------------------------------- VietCap trading (POST API)

_VCI_TRADING = "https://trading.vietcap.com.vn"


def vci_symbols(symbols: list[str]) -> dict[str, dict]:
    """Thong tin + bang gia + khoi ngoai tu VietCap trading (POST).

    Tuong duong route: market.equity.quote, market.equity.foreign_flow
    Tra ve {symbol: {listing, quote, foreign, shares_outstanding}}.
    """
    import urllib.request as _urlreq

    url = f"{_VCI_TRADING}/api/price/symbols/getList"
    body = json.dumps({"symbols": [s.upper() for s in symbols]}).encode()
    headers = {
        "User-Agent": UA,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://trading.vietcap.com.vn/",
    }
    import http.client as _httpc

    last_err: Exception | None = None
    rows: list = []
    for _ in range(3):
        try:
            req = _urlreq.Request(url, data=body, headers=headers)
            with _urlreq.urlopen(req, timeout=TIMEOUT) as resp:
                try:
                    raw = resp.read()
                except _httpc.IncompleteRead as e:
                    raw = e.partial  # proxy cat connection giua chung — lay phan da nhan
                if "gzip" in resp.headers.get("Content-Encoding", ""):
                    import gzip

                    raw = gzip.decompress(raw)
                rows = json.loads(raw.decode("utf-8"))
            last_err = None
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2)
    if last_err is not None:
        raise RuntimeError(f"vci_symbols failed: {last_err}")
    out: dict[str, dict] = {}
    for r in rows or []:
        info = r.get("listingInfo") or {}
        mp = r.get("matchPrice") or {}
        sym = info.get("symbol")
        if not sym:
            continue
        out[sym] = {
            "symbol": sym,
            "name": info.get("organName"),
            "name_en": info.get("enOrganName"),
            "exchange": "HOSE" if info.get("board") == "HSX" else info.get("board"),
            "reference": info.get("refPrice"),
            "floor": info.get("floor"),
            "ceiling": info.get("ceiling"),
            "shares_outstanding": info.get("listedShare"),
            "open": mp.get("openPrice"),
            "high": mp.get("highestPrice"),
            "low": mp.get("lowestPrice"),
            "close": mp.get("lastPrice"),
            "avg_price": mp.get("avgPrice"),
            "volume": mp.get("totalVolume"),
            "value": mp.get("totalValue"),
            # khoi ngoai — tuong duong market.equity.foreign_flow
            "foreign_buy_volume": mp.get("foreignBuyVolume"),
            "foreign_sell_volume": mp.get("foreignSellVolume"),
            "foreign_buy_value": mp.get("foreignBuyValue"),
            "foreign_sell_value": mp.get("foreignSellValue"),
        }
    return out


if __name__ == "__main__":
    import sys

    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "index":
        rows = vci_index_history(sys.argv[2] if len(sys.argv) > 2 else "VNINDEX", 3)
        for r in rows:
            print(r["date"], r["close"], r["volume"])
    elif cmd == "company":
        c = vci_company(sys.argv[2])
        print(json.dumps(c, indent=1, ensure_ascii=False)[:800])
    elif cmd == "fin":
        sym = sys.argv[2] if len(sys.argv) > 2 else "FPT"
        rt = sys.argv[3] if len(sys.argv) > 3 else "KQKD"
        fin = kbs_financial(sym, rt)
        print("periods:", [(p["term"], p["year"]) for p in fin["periods"]])
        for kw in ("doanh thu bán hàng", "lợi nhuận sau thuế", "tổng cộng tài sản"):
            r = kbs_find_row(fin, kw)
            if r:
                print(r["name"][:50], "->", r["values"])
    elif cmd == "ohlcv":
        sym = sys.argv[2] if len(sys.argv) > 2 else "FPT"
        for r in kbs_ohlcv(sym, "2026-10-14", "2026-10-16"):
            print(r)
    elif cmd == "board":
        syms = sys.argv[2].split(",") if len(sys.argv) > 2 else ["FPT"]
        for s, q in kbs_price_board(syms).items():
            print(s, q["close"], q["change"], "FB:", q["foreign_buy_volume"],
                  "FS:", q["foreign_sell_volume"])
    elif cmd == "events":
        sym = sys.argv[2] if len(sys.argv) > 2 else "FPT"
        evs = kbs_company_events(sym)
        print(f"{len(evs)} events")
        for e in evs[:3]:
            print(json.dumps(e, ensure_ascii=False)[:200])
    else:
        print("usage: direct_fetch.py [index [SYM] | company SYM | fin SYM [TYPE] |"
              " ohlcv SYM | board SYM1,SYM2 | events SYM]")
