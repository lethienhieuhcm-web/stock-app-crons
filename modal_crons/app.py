"""
Modal cron jobs for Pocket Personal (k1 Section 2).

4 scheduled functions:
1. book_processor  - every 5 min: process PDF uploads from R2 (English Lab Books)
2. publish_snapshot - DISABLED 2026-10-19 (thay bang realtime_poller).
   Giuchuc nang de chay tay khi can.
3. realtime_poller - every 1 min trong phien HOSE (9:00-14:45 +07, T2-T6):
   lay gia live tu VCI/KBS (tu chu, khong qua vnstock), publish len R2.
   Payload DUNG contract Worker ({"indices": [...]} -> market/indices.json,
   {"quotes": {SYM: {...}}} -> market/quotes.json).
4. d1_backup        - daily: backup D1 to Modal Volume (keep 7 versions)

Secrets (Modal dashboard -> Secrets -> "pocket-personal"):
  - CF_API_TOKEN
  - STOCK_PUBLISH_SECRET
  - VNSTOCK_API_KEY

Deploy:
  modal deploy app.py

Test single run:
  modal run app.py::book_processor
  modal run app.py::publish_snapshot
  modal run app.py::d1_backup
"""
import modal

# Persistent volume for state (D1 backups, temp files)
volume = modal.Volume.from_name("pocket-personal-vol", create_if_missing=True)

# Image with all dependencies
book_image = (
    modal.Image.debian_slim(python_version="3.13")
    .pip_install("pymupdf", "requests")
)

stock_image = (
    modal.Image.debian_slim(python_version="3.13")
    .pip_install("requests")
    .pip_install("vnstock>=5,<6", extra_index_url="https://vnstocks.com/api/packages")
    .add_local_file("./modal_crons/direct_fetch.py", "/opt/direct_fetch.py")
)

# Realtime image: KHONG can vnstock, chi can direct_fetch (tu chu du lieu)
# VCI trading + KBS board la feed live trong phien, polling 60s
realtime_image = (
    modal.Image.debian_slim(python_version="3.13")
    .pip_install("requests")
    .add_local_file("./modal_crons/direct_fetch.py", "/opt/direct_fetch.py")
)

app = modal.App("pocket-personal-crons")


@app.function(
    image=book_image,
    secrets=[modal.Secret.from_name("pocket-personal")],
    volumes={"/data": volume},
    schedule=modal.Cron("*/5 * * * *"),  # every 5 minutes
    timeout=600,
)
def book_processor():
    """Process pending PDF book uploads from R2 every 5 minutes."""
    import json, re, os, tempfile, urllib.request
    from datetime import datetime

    CF_TOKEN = os.environ["CF_API_TOKEN"]
    API = "https://api.cloudflare.com/client/v4"
    ACCOUNT = "25b2327e932d26e5487c2ba8ad058932"
    BUCKET = "english-lab-data"

    def api_req(method, path, data=None, raw=False):
        url = API + path
        body = json.dumps(data).encode() if data is not None else None
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Authorization", f"Bearer {CF_TOKEN}")
        if not raw:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=120) as resp:
            if raw:
                return resp.read()
            return json.loads(resp.read().decode())

    def r2_download(key):
        return api_req("GET", f"/accounts/{ACCOUNT}/r2/buckets/{BUCKET}/objects/{key}", raw=True)

    def r2_list(prefix):
        out = api_req("GET", f"/accounts/{ACCOUNT}/r2/buckets/{BUCKET}/objects?prefix={prefix}")
        result = out.get("result", [])
        return result if isinstance(result, list) else result.get("objects", [])

    def r2_delete(key):
        api_req("DELETE", f"/accounts/{ACCOUNT}/r2/buckets/{BUCKET}/objects/{key}")

    def r2_put(key, obj):
        data = json.dumps(obj).encode()
        req = urllib.request.Request(
            f"{API}/accounts/{ACCOUNT}/r2/buckets/{BUCKET}/objects/{key}",
            data=data, method="PUT")
        req.add_header("Authorization", f"Bearer {CF_TOKEN}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=120):
            pass

    # Find pending PDFs
    pending = r2_list("books/pending/")
    if not pending:
        print(f"[{datetime.now().isoformat()}] book_processor: no pending PDFs")
        return

    import fitz  # pymupdf

    for obj in pending:
        key = obj["key"]
        print(f"Processing {key}...")
        try:
            pdf_bytes = r2_download(key)
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
                tf.write(pdf_bytes)
                pdf_path = tf.name

            doc = fitz.open(pdf_path)
            full_text = ""
            for page in doc:
                full_text += page.get_text() + "\n"
            doc.close()
            os.unlink(pdf_path)

            # Basic chapter detection (lines that look like headings)
            lines = full_text.split("\n")
            chapters = []
            current = {"title": "Chapter 1", "text": ""}
            for line in lines:
                stripped = line.strip()
                if re.match(r"^(chapter|chương)\s+\d+", stripped, re.I) and len(stripped) < 80:
                    if current["text"].strip():
                        chapters.append(current)
                    current = {"title": stripped, "text": ""}
                else:
                    current["text"] += line + "\n"
            if current["text"].strip():
                chapters.append(current)

            # Simple CEFR estimation by avg word length + sentence length
            words = full_text.split()
            avg_wl = sum(len(w) for w in words) / max(len(words), 1) if words else 0
            if avg_wl < 4.5:
                cefr = "A1"
            elif avg_wl < 5.0:
                cefr = "A2"
            elif avg_wl < 5.5:
                cefr = "B1"
            elif avg_wl < 6.0:
                cefr = "B2"
            else:
                cefr = "C1"

            book_id = os.path.basename(key).replace(".pdf", "")
            result = {
                "book_id": book_id,
                "source_key": key,
                "processed_at": datetime.now().isoformat(),
                "cefr": cefr,
                "word_count": len(words),
                "chapters": [
                    {"title": c["title"], "word_count": len(c["text"].split())}
                    for c in chapters
                ],
                "full_text_key": f"books/text/{book_id}.txt",
            }

            # Upload extracted text and metadata
            r2_put(f"books/text/{book_id}.txt", {"text": full_text})
            r2_put(f"books/meta/{book_id}.json", result)
            r2_delete(key)  # remove from pending

            print(f"  -> {book_id}: {len(words)} words, {len(chapters)} chapters, CEFR {cefr}")
        except Exception as e:
            print(f"  ERROR {key}: {e}")

    volume.commit()


@app.function(
    image=stock_image,
    secrets=[modal.Secret.from_name("pocket-personal")],
    volumes={"/data": volume},
    # DISABLED 2026-10-19: thay bang realtime_poller (1 phut/lan, tu chu VCI/KBS)
    # schedule=modal.Cron("*/30 2-7 * * 1-5", timezone="UTC"),
    timeout=1200,
)
def publish_snapshot():
    """Publish stock snapshot every 30 min during HOSE trading hours."""
    import json, math, os, sys, time, urllib.request
    from datetime import date, timedelta

    PUBLISH_URL = "https://lethanhdong.one/api/internal/stock-publish"
    ALERT_URL = "https://lethanhdong.one/api/internal/modal-alert"
    SECRET = os.environ["STOCK_PUBLISH_SECRET"]
    VNSTOCK_KEY = os.environ.get("VNSTOCK_API_KEY")

    # vnstock 5.x: dang ky device bang key Community (Henrry yeu cau).
    # Luu auth tren volume persistent /data de khong register lai moi run.
    import subprocess
    _vnhome = "/data/.vnstock"
    os.makedirs(_vnhome, exist_ok=True)
    os.environ["HOME"] = "/data"  # ~/.vnstock -> /data/.vnstock (persistent)
    # Kiem tra device da register THAT chua (file ton tai chua du;
    # lan truoc register that bai van de lai file stale)
    def _device_ok():
        try:
            _sp = subprocess.run(["vnstock", "status"], capture_output=True,
                                 timeout=30, env={**os.environ, "HOME": "/data"})
            _out = _sp.stdout.decode() + _sp.stderr.decode()
            return "Thiết bị:" in _out and "chưa" not in _out.lower().split("thiết bị:")[1][:50]
        except Exception:
            return False
    if not _device_ok() and VNSTOCK_KEY:
        print("vnstock: registering device with Community key...", flush=True)
        try:
            # Xoa auth stale truoc khi register lai
            import shutil
            shutil.rmtree(os.path.join(_vnhome, "auth"), ignore_errors=True)
            _rp = subprocess.run(
                ["vnstock", "register", "--stdin"],
                input=VNSTOCK_KEY.encode(), capture_output=True, timeout=90,
                env={**os.environ, "HOME": "/data"},
            )
            print("vnstock register: " + _rp.stdout.decode()[:300], flush=True)
            if _rp.returncode != 0:
                print("vnstock register FAILED: " + _rp.stderr.decode()[:300], flush=True)
        except Exception as _re:
            print(f"vnstock register error: {_re}", flush=True)

    from vnstock import Market
    import vnstock as _vnmod  # doc version runtime de gan vao _prov

    def vn_call(fn, label, retries=3):
        for attempt in range(retries):
            try:
                return fn()
            except SystemExit:
                print(f"[{label}] rate limit, sleep 70s (try {attempt+1})", flush=True)
                time.sleep(70)
            except Exception as e:
                print(f"[{label}] error: {e} (try {attempt+1})", flush=True)
                time.sleep(5)
        print(f"[{label}] skipped after {retries} tries", flush=True)
        return None

    def publish(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(PUBLISH_URL, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Publish-Secret", SECRET)
        req.add_header("X-Internal-Secret", SECRET)
        req.add_header("User-Agent", "Mozilla/5.0 (compatible; PocketPersonal/1.0)")
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())

    def _prov(source):
        """Provenance (hoc tu vnstock 5): gan version vnstock THAT dang chay
        trong image, de biet du lieu tu stock4 hay stock5."""
        from datetime import datetime, timezone
        ver = "?"
        try:
            from importlib.metadata import version as _pkg_version
            ver = _pkg_version("vnstock")
        except Exception:
            ver = getattr(_vnmod, "__version__", "?")
        return {"source": f"{source}/{ver}",
                "retrieved_at": datetime.now(timezone.utc).isoformat()}

    def send_fail_alert(detail):
        """Bao Henrry qua Telegram khi run that bai hoan toan (kieu ong lon:
        alert on meaningful failure, tranh alert fatigue khi chi loi 1 phan)."""
        from datetime import datetime, timezone
        try:
            data = json.dumps({
                "app": "pocket-personal-crons",
                "fn": "publish_snapshot",
                "time": datetime.now(timezone.utc).isoformat(),
                "detail": detail,
            }).encode()
            req = urllib.request.Request(ALERT_URL, data=data, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("X-Publish-Secret", SECRET)
            req.add_header("X-Internal-Secret", SECRET)
            req.add_header("User-Agent", "Mozilla/5.0 (compatible; PocketPersonal/1.0)")
            with urllib.request.urlopen(req, timeout=30) as resp:
                print(f"  fail alert sent: HTTP {resp.status}", flush=True)
        except Exception as e:
            print(f"  fail alert error: {e}", flush=True)

    def clean_num(v):
        """So sach: None/NaN/inf -> None (tranh loi serialize nhu script VM)."""
        if v is None:
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return None if math.isnan(f) or math.isinf(f) else f

    def clean_rec(r):
        return {k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
                for k, v in r.items()}

    print(f"[{date.today().isoformat()}] publish_snapshot starting...")
    market = Market()
    ok_indices = False
    ok_quotes = False

    # Indices: pattern nhu script VM (dung ohlcv tung index, lay 2 bar cuoi
    # de tinh change/pct_change). market.indices() khong con ton tai o vnstock v3.
    try:
        end = date.today().isoformat()
        start = (date.today() - timedelta(days=10)).isoformat()
        idx_rows = []
        for sym in ["VNINDEX", "VN30", "HNXIndex", "UPCOMIndex"]:
            recs = vn_call(
                lambda s=sym: [clean_rec(r) for r in
                               market.index(symbol=s).ohlcv(start=start, end=end).to_dict("records")],
                f"indices-{sym}", retries=2)
            item = {"symbol": sym}
            if recs:
                last = recs[-1]
                item["close"] = clean_num(last.get("close"))
                item["volume"] = clean_num(last.get("volume"))
                t = last.get("time")
                # 5.x: time la pandas Timestamp -> lay date; 4.x: str -> cat 10 ky tu
                item["time"] = t[:10] if isinstance(t, str) else str(t)[:10]
                if len(recs) >= 2:
                    prev, cur = clean_num(recs[-2].get("close")), item["close"]
                    if prev and cur:
                        item["change"] = round(cur - prev, 2)
                        item["pct_change"] = round((cur - prev) / prev * 100, 2)
            idx_rows.append(item)
            time.sleep(2)  # respect rate limit
        if idx_rows:
            # Contract Worker: {"indices": [...]} -> market/indices.json {rows: [...]}
            result = publish({"indices": idx_rows, "_prov": _prov("vnstock")})
            print(f"  indices: {len(idx_rows)} -> {result}")
            ok_indices = True
    except Exception as e:
        print(f"  indices publish error: {e}")

    # Quotes: map field that cua vnstock sang dung contract frontend
    # {close, change, pct_change, volume, value_vnd, exchange} theo ma CK.
    # vnstock 5.x: dung price_board (1 call cho 20 ma) thay vi 20x quote().
    # 5.x khong co san price_change/percent_change/volume_accumulated/total_value
    # -> change/pct_change tu tinh tu reference_price; volume/value_vnd = None
    # (frontend khong dung 2 field nay tu quotes; AI context co fallback "—").
    symbols = ["VCB", "BID", "CTG", "TCB", "MBB", "VPB", "HPG", "VHM", "VIC", "VNM",
               "MSN", "FPT", "MWG", "SSI", "VND", "HCM", "STB", "ACB", "SHB", "POW"]
    by_symbol = {}
    board_recs = vn_call(lambda: market.price_board(symbols).to_dict("records"),
                         "quote-board", retries=2)
    if board_recs:
        for r in board_recs:
            sym = r.get("symbol")
            close = clean_num(r.get("close_price"))
            ref = clean_num(r.get("reference_price"))
            chg = round(close - ref, 2) if (close is not None and ref) else None
            pct = round(chg / ref * 100, 2) if (chg is not None and ref) else None
            by_symbol[sym] = {
                "close": close,
                "change": chg,
                "pct_change": pct,
                "volume": None,
                "value_vnd": None,
                "exchange": r.get("exchange"),
            }

    if by_symbol:
        try:
            # Contract Worker: {"quotes": {SYM: {...}}} -> market/quotes.json {by_symbol: {...}}
            result = publish({"quotes": by_symbol, "_prov": _prov("vnstock")})
            print(f"  quotes: {len(by_symbol)} symbols -> {result}")
            ok_quotes = True
        except Exception as e:
            print(f"  quotes publish error: {e}")

    # That bai hoan toan (khong publish duoc gi) -> alert + raise de
    # dashboard Modal hien Failed. Loi 1 phan thi chi log, run sau tu retry.
    if not ok_indices and not ok_quotes:
        detail = "indices: FAILED, quotes: FAILED"
        send_fail_alert(detail)
        raise RuntimeError(f"publish_snapshot failed: {detail}")

    print("publish_snapshot done.")
    volume.commit()


@app.function(
    image=realtime_image,
    secrets=[modal.Secret.from_name("pocket-personal")],
    schedule=modal.Cron("* 2-7 * * 1-5", timezone="UTC"),  # every 1 min, 09:00-14:59 +07, Mon-Fri
    timeout=120,
)
def realtime_poller():
    """Realtime poller: lay gia live tu VCI/KBS (tu chu, khong qua vnstock), publish len R2 moi 60s.

    Chay trong phien HOSE (9:00-14:45 +07, T2-T6). Ngoai gio tu thoat nhanh.
    Nguon: direct_fetch.py — quotes: kbs_price_board; indices: vnd_index_snapshot
    (VNDirect dchart, doi chieu cheo + fallback bang vci_index_history).
    """
    import sys
    sys.path.insert(0, "/opt")
    import json, urllib.request
    from datetime import datetime, timezone, timedelta

    PUBLISH_URL = "https://lethanhdong.one/api/internal/stock-publish"
    import os
    SECRET = os.environ.get("STOCK_PUBLISH_SECRET", "")
    if not SECRET:
        raise RuntimeError("[realtime] thieu STOCK_PUBLISH_SECRET trong Modal Secrets 'pocket-personal'")

    # Kiem tra gio giao dich (VN time)
    vn_now = datetime.now(timezone.utc) + timedelta(hours=7)
    wd = vn_now.weekday()  # 0=Mon
    mins = vn_now.hour * 60 + vn_now.minute
    # 9:00-14:45, T2-T6
    if wd >= 5 or not (9 * 60 <= mins <= 14 * 60 + 45):
        print(f"[realtime] ngoai gio GD ({vn_now.strftime('%H:%M')} VN), skip")
        return

    import direct_fetch as df

    def publish(payload):
        data = json.dumps(payload).encode()
        req = urllib.request.Request(PUBLISH_URL, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Publish-Secret", SECRET)
        req.add_header("X-Internal-Secret", SECRET)
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())

    # 1. Bang gia realtime tu KBS (1 call cho tat ca ma)
    symbols = ["VCB", "BID", "CTG", "TCB", "MBB", "VPB", "HPG", "VHM", "VIC", "VNM",
               "MSN", "FPT", "MWG", "SSI", "VND", "HCM", "STB", "ACB", "SHB", "POW",
               "GAS", "PLX", "BVH", "VRE", "VJC", "PNJ", "SAB", "GVR", "BCM", "CTG"]
    symbols = list(dict.fromkeys(symbols))  # dedup
    try:
        board = df.kbs_price_board(symbols)
        quotes = {}
        for sym, q in board.items():
            quotes[sym] = {
                "close": q.get("close"),
                "change": q.get("change"),
                "pct_change": q.get("pct_change"),
                "volume": q.get("volume"),
                "value_vnd": q.get("value"),
                "exchange": q.get("exchange"),
                "bid": q.get("bid"),
                "ask": q.get("ask"),
                # Khoi ngoai (KBS tra san FB/FS/FO/FR) — frontend hien "NN mua/ban".
                # Phai publish o day vi realtime ghi de toan bo quotes.json moi phut.
                "foreign_buy_volume": q.get("foreign_buy_volume"),
                "foreign_sell_volume": q.get("foreign_sell_volume"),
                "foreign_buy_value": q.get("foreign_buy_value"),
                "foreign_sell_value": q.get("foreign_sell_value"),
            }
        r = publish({"quotes": quotes,
                     "_prov": {"source": "direct:kbs", "retrieved_at": datetime.now(timezone.utc).isoformat()}})
        print(f"[realtime] quotes: {len(quotes)} -> {r}")
    except Exception as e:
        print(f"[realtime] quotes error: {e}")

    # 2. Chi so: nguon chinh VNDirect dchart (truc tiep), doi chieu cheo VCI.
    # 2026-10-20: bo vci_index_history lam nguon chinh — feed EOD cham 1 ngay
    # + sap xep GIAM dan (moi nhat truoc), code cu lay hist[-1] nen dang so
    # phien cu nhat trong cua so lam "hom nay" ca ngay (VNINDEX dung o so
    # 16/10 trong phien 20/10). VCI chi con vai tro doi chieu + fallback
    # (fallback gan dung ngay cua so nen app khong hieu nham la truc tiep).
    def _vci_close_on(sym, day):
        """Close cua phien `day` theo VCI IQ history (feed giam dan)."""
        try:
            for h in df.vci_index_history(sym, limit=7):
                if h.get("date") == day:
                    return h.get("close")
        except Exception:
            pass
        return None

    try:
        idx_rows = []
        for row in df.vnd_index_snapshot():
            vci_pc = _vci_close_on(row["symbol"], row["prev_date"])
            if vci_pc and row["prev_close"]:
                lech = abs(vci_pc - row["prev_close"]) / row["prev_close"] * 100
                if lech > 0.5:
                    print(f"[realtime] indices {row['symbol']}: LECH doi chieu "
                          f"VND prev={row['prev_close']} vs VCI={vci_pc} ({lech:.2f}%) "
                          f"-> bo qua chi so nay, giu so cu")
                    continue
            idx_rows.append({k: row[k] for k in
                             ("symbol", "close", "volume", "time",
                              "change", "pct_change")})
        if idx_rows:
            r = publish({"indices": idx_rows,
                         "_prov": {"source": "direct:vndirect", "retrieved_at": datetime.now(timezone.utc).isoformat()}})
            print(f"[realtime] indices (vnd): {len(idx_rows)} -> {r}")
        else:
            raise RuntimeError("vnd_index_snapshot rong hoac lech het")
    except Exception as e:
        print(f"[realtime] indices vnd error: {e} -> fallback VCI (EOD, gan dung ngay)")
        try:
            idx_rows = []
            for sym in ["VNINDEX", "VN30", "HNXIndex", "UPCOMIndex"]:
                hist = df.vci_index_history(sym, limit=2)
                if hist:
                    last = hist[0]  # feed GIAM dan: phan tu dau moi la moi nhat
                    pct = last.get("pct_change")  # VCI tra dang phan so (0.01 = 1%)
                    idx_rows.append({
                        "symbol": sym,
                        "close": last.get("close"),
                        "volume": last.get("volume"),
                        "time": last.get("date"),
                        "change": last.get("change"),
                        "pct_change": round(pct * 100, 2) if pct is not None else None,
                    })
            if idx_rows:
                r = publish({"indices": idx_rows,
                             "_prov": {"source": "direct:vci-fallback", "retrieved_at": datetime.now(timezone.utc).isoformat()}})
                print(f"[realtime] indices (vci-fallback): {len(idx_rows)} -> {r}")
        except Exception as e2:
            print(f"[realtime] indices fallback error: {e2} -> giu nguyen so cu tren R2")

    print("[realtime] done.")


@app.function(
    image=book_image,
    secrets=[modal.Secret.from_name("pocket-personal")],
    volumes={"/data": volume},
    schedule=modal.Cron("0 17 * * *"),  # daily at 17:00 UTC (midnight +07)
    timeout=600,
)
def d1_backup():
    """Daily D1 backup to Modal Volume. Keeps 7 versions."""
    import json, os, glob, datetime, urllib.request

    CF_TOKEN = os.environ["CF_API_TOKEN"]
    API = "https://api.cloudflare.com/client/v4"
    ACCT = "25b2327e932d26e5487c2ba8ad058932"
    D1_ID = "e856db4d-1ee1-42fd-9c1d-1807ca31afb5"
    KEEP = 7

    def api(method, path, data=None):
        url = API + path
        body = json.dumps(data).encode() if data is not None else None
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Authorization", f"Bearer {CF_TOKEN}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())

    today = datetime.date.today().isoformat()
    tables_r = api("POST", f"/accounts/{ACCT}/d1/database/{D1_ID}/query",
                   {"sql": "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"})
    if not tables_r.get("success"):
        print("Failed to list tables")
        return
    tables = [r["name"] for r in tables_r["result"][0]["results"]]

    backup = {"date": today, "tables": {}}
    for t in tables:
        try:
            r = api("POST", f"/accounts/{ACCT}/d1/database/{D1_ID}/query",
                    {"sql": f'SELECT * FROM "{t}" LIMIT 10000'})
            if r.get("success"):
                backup["tables"][t] = r["result"][0]["results"]
        except Exception as e:
            print(f"  skip {t}: {e}")

    data = json.dumps(backup, ensure_ascii=False).encode()
    os.makedirs("/data/backups/d1", exist_ok=True)
    key = f"/data/backups/d1/{today}.json"
    with open(key, "wb") as f:
        f.write(data)
    print(f"Backup: {len(tables)} tables, {len(data)} bytes -> {key}")

    olds = sorted(glob.glob("/data/backups/d1/*.json"))
    for old_key in olds[:-KEEP]:
        os.remove(old_key)
        print(f"  deleted old: {old_key}")

    volume.commit()
