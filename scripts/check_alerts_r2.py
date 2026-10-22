#!/usr/bin/env python3
"""Kiem tra price alerts + TP/SL targets tu R2 (qua worker internal API),
gui Telegram khi cham nguong. One-shot: danh dau done sau khi bao.

Chay bang cron moi 10 phut trong gio giao dich.
"""
import os
import sys
import json
import subprocess
import tempfile
import time

WORKER = "https://lethanhdong.one"
DH_ENV = os.path.expanduser("~/workspace/shopee-deal-hunter/.env")
SECRET_FILE = os.path.expanduser("~/.stockapp_publish")

UA_STR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
# 2026-10-15: urllib bi edge Cloudflare dong ket noi (RemoteDisconnected);
# curl qua proxy moi qua duoc -> route moi HTTP qua curl subprocess.
# 2026-10-18 (Actions): proxy lay tu env http_proxy/https_proxy; khong co -> di thang.


def get_secret():
    with open(SECRET_FILE) as f:
        return f.read().strip()


def _proxy():
    return (os.environ.get("https_proxy") or os.environ.get("http_proxy")
            or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY"))


def curl_json(url, method="GET", headers=None, body=None):
    cmd = ["curl", "-sS", "--max-time", "30", "-A", UA_STR]
    _p = _proxy()
    if _p:
        cmd += ["-x", _p]
    cmd += ["-X", method, url]
    for k, v in (headers or {}).items():
        cmd += ["-H", f"{k}: {v}"]
    if body is not None:
        cmd += ["-d", "@-"]
    out = subprocess.run(cmd, input=body if body is not None else None,
                         capture_output=True, text=True)
    out.check_returncode()
    return json.loads(out.stdout)


def api(path, method="GET", body=None):
    sec = get_secret()
    return curl_json(
        WORKER + path, method,
        {"X-Publish-Secret": sec, "X-Internal-Secret": sec,
         "Content-Type": "application/json"},
        json.dumps(body) if body is not None else None)


def load_dh_env():
    env = {}
    try:
        with open(DH_ENV) as f:
            for line in f:
                line = line.strip()
                if line and "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip("'\"")
    except FileNotFoundError:
        pass
    return env


def tg_send(text, attempts=3):
    """Gui Telegram co retry backoff (proxy MITM hay cat TLS giua chung
    — tien le 09:00 22/10). Caller chi danh dau alert-done khi True."""
    env = load_dh_env()
    token = env.get("TELEGRAM_BOT_TOKEN") or env.get("DEALHUNTER_TELEGRAM_BOT_TOKEN")
    chat_id = env.get("TELEGRAM_CHAT_ID") or env.get("DEALHUNTER_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("thieu Telegram config", flush=True)
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = json.dumps({"chat_id": chat_id, "text": text,
                       "parse_mode": "HTML"})
    for i in range(attempts):
        try:
            if curl_json(url, "POST", {"Content-Type": "application/json"},
                         body).get("ok", False):
                return True
        except Exception as e:
            print(f"Telegram loi lan {i + 1}/{attempts}: {e}", flush=True)
            if i < attempts - 1:
                time.sleep(5 * (i + 1))
    return False


def fmt_vnd(x):
    return f"{x:,.0f}".replace(",", ".")


def main():
    data = api("/api/internal/stock-report-data")
    users = data.get("users", [])
    quotes = data.get("quotes", {})
    if not users:
        print("khong co user nao", flush=True)
        print("xong.", flush=True)
        return

    for u in users:
        email = u["email"]
        # Price alerts
        for a in u.get("alerts", []):
            if not a.get("active"):
                continue
            sym = a["symbol"]
            q = quotes.get(sym)
            if not q:
                continue
            price = q.get("close") or q.get("price")
            if not price:
                continue
            hit = (a["direction"] == "ABOVE" and price >= a["price"]) or \
                  (a["direction"] == "BELOW" and price <= a["price"])
            if hit:
                arrow = "🔼" if a["direction"] == "ABOVE" else "🔽"
                msg = (f"{arrow} <b>{sym}</b> cham nguong!\n"
                       f"Gia hien tai: <b>{fmt_vnd(price)}</b>\n"
                       f"Nguong: {a['direction']} {fmt_vnd(a['price'])}")
                if tg_send(msg):
                    api("/api/internal/stock-alert-done", "POST",
                        {"email": email, "id": a["id"]})
                    print(f"alert {sym} -> da bao", flush=True)

        # TP/SL targets: % lai/lo chua thuc hien tinh tu gia von theo
        # dung cong thuc sCalcPositions cua worker (mua: cong ca phi vao
        # von; ban: tru theo gia von binh quan tai thoi diem ban).
        pos = {}
        for tx in sorted(u.get("txns", []),
                         key=lambda x: ((x.get("date") or ""), x.get("id") or 0)):
            p = pos.setdefault(tx["symbol"], {"qty": 0.0, "cost": 0.0})
            qty = tx.get("qty") or 0
            prc = tx.get("price") or 0
            fee = tx.get("fee") or 0
            if tx.get("side") == "BUY":
                p["qty"] += qty
                p["cost"] += prc * qty + fee
            else:
                avg0 = p["cost"] / p["qty"] if p["qty"] else 0
                p["qty"] -= qty
                p["cost"] -= avg0 * qty
        for t in u.get("targets", []):
            sym = t["symbol"]
            p = pos.get(sym)
            if not p or p["qty"] <= 1e-9 or not p["cost"]:
                continue  # khong con nam giu ma nay -> khong danh gia TP/SL
            q = quotes.get(sym)
            if not q:
                continue
            price = q.get("close") or q.get("price")
            if not price:
                continue
            avg = p["cost"] / p["qty"]
            pnl = (price - avg) / avg * 100
            tp = t.get("take_profit_pct")
            sl = t.get("stop_loss_pct")
            if tp is not None and not t.get("tp_done") and pnl >= tp:
                msg = (f"🎯 <b>{sym}</b> cham muc chot loi!\n"
                       f"Gia hien tai: <b>{fmt_vnd(price)}</b>\n"
                       f"Lai: <b>{pnl:+.1f}%</b> (nguong +{tp}%)\n"
                       f"Gia von: {fmt_vnd(avg)}")
                if tg_send(msg):
                    api("/api/internal/stock-target-done", "POST",
                        {"email": email, "symbol": sym, "field": "tp_done"})
                    print(f"target TP {sym} -> da bao", flush=True)
            if sl is not None and not t.get("sl_done") and pnl <= -abs(sl):
                msg = (f"🛑 <b>{sym}</b> cham muc cat lo!\n"
                       f"Gia hien tai: <b>{fmt_vnd(price)}</b>\n"
                       f"Lo: <b>{pnl:+.1f}%</b> (nguong -{abs(sl)}%)\n"
                       f"Gia von: {fmt_vnd(avg)}")
                if tg_send(msg):
                    api("/api/internal/stock-target-done", "POST",
                        {"email": email, "symbol": sym, "field": "sl_done"})
                    print(f"target SL {sym} -> da bao", flush=True)

    print("xong.", flush=True)


if __name__ == "__main__":
    main()
