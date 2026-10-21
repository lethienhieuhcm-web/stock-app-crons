#!/usr/bin/env python3
"""Bo thu tick DA CONG (Chot Henrry 19:39 21/10/2026 qua dau moi) —
chay tren Modal, TANG A: 4 cong chay DONG THOI, hop nhat theo luat.

Kien truc da cong (thay ban failover 1-cong 21/10 sang):
- DNSE (chinh): tick khop lenh + khoi ngoai DU BO nhat (KL + GT +
  room, chuan hoa: KL/room x10 CP, GT ty dong x10^9).
- KBS (doi chieu): TRADE tung lenh + STOCK snapshot (FB/FS = KL
  mua/ban ngoai CP goc, FO = tong room, FR = room con lai — mapping
  da chot o REPORT-LIVE-2110; KHONG co GT ngoai).
- VNDirect (doi chieu): tick T/ + SMA (KL ngoai + room, moi field
  x10 CP; GT tong = field 8 x10^6 dong; KHONG co GT ngoai).
- VPS (chan bonus): board 3250 (KL ngoai x10, GT nghin dong x1000,
  room x10) + stock 3220 tick + tong hop toan thi truong/phai
  sinh/lo le (aggregatemarket/aggregateps/soddlot — luu RAW kem
  nhan nguon, chap nhan tinh mong manh cua kenh nac danh).
- SSI giu nguyen adapter nhung KHONG khoi dong o Tang A (Tang C
  chua quyet — diem ra dan cu). VCI giu adapter, Tang B dung o
  ha tang OVH/Botkeep (Modal bi VCI chan 403 — ma tran egress).

Hop nhat: moi loai du lieu 1 cong chinh (DNSE), cac cong con lai
doi chieu cheo; nen 1m gom theo tung phut voi uu tien nguon
dnse > kbs > vndirect > vps (phut nao cong chinh thieu thi lay
cong ke tiep — failover o muc phut, khong cat ca phien).
Thong ke doi chieu (lech KL mua/ban/room/tong KL giua cac cong)
ghi vao status moi luot publish — lech bat thuong nhin thay ngay.

Dau ra:
- tick_shadow (audit nhu cu): prefix market/shadow_tick/ — REST
  doi chieu giu it nhat 1 tuan sau lat (tick_shadow_reconcile.py).
- tick_live (MOI — ban LAT): intraday hop nhat -> market/intraday/
  production + khoi ngoai hop nhat merge vao market/foreign_flow.
  json cho 40 ma. Nhan nguon tung ban ghi qua _prov + truong src.

Ky luat: dung 1 ket noi/cong, PINGREQ keepalive 25s (broker dong
ket noi neu client im lang 2-3 phut — bai hoc probe 20/10).
"""
import modal

app = modal.App("stock-tick-collector")

image = (modal.Image.debian_slim(python_version="3.12")
         .pip_install("websockets>=12"))


@app.function(image=image,
              secrets=[modal.Secret.from_name("pocket-personal")],
              schedule=modal.Cron("55 1 * * 1-5", timezone="UTC"),
              timeout=60 * 60 * 7, max_containers=1)
def tick_session(max_minutes: int = 0):
    import asyncio
    asyncio.run(_collect(max_minutes))


@app.local_entrypoint()
def main(max_minutes: int = 0):
    tick_session.spawn(max_minutes)


# =====================================================================
# Collector (chay ben trong Modal function)
# =====================================================================
import asyncio, json, os, random, struct, time, urllib.request
from datetime import datetime, timedelta, timezone

ICT = timezone(timedelta(hours=7))
PUBLISH_URL = "https://lethanhdong.one/api/internal/stock-publish"

CONFIG = {
    # Chu ky publish (giay)
    "publish_sec": 30,
    # Cong khoi dong o Tang A (Modal). SSI = Tang C (chua quyet),
    # VCI = Tang B (OVH/Botkeep) — adapter con trong code, khong chay.
    "sources": ["dnse", "kbs", "vndirect", "vps"],
    "universe": [
        "VCB", "BID", "CTG", "TCB", "MBB", "VPB", "HPG", "VHM",
        "VIC", "VNM", "MSN", "FPT", "MWG", "SSI", "VND", "HCM",
        "STB", "ACB", "SHB", "POW", "GAS", "PLX", "BVH", "VRE",
        "VJC", "PNJ", "SAB", "GVR", "BCM", "HDB", "TPB", "LPB",
        "VIB", "EIB", "NLG", "KDH", "DGC", "FRT", "DGW", "GEX",
    ],
}

# Uu tien hop nhat (cao -> thap)
SRC_PRIORITY = ["dnse", "kbs", "vndirect", "vps", "vci", "ssi-mqtt"]


def now_ict() -> datetime:
    return datetime.now(ICT)


def in_trading_window(dt: datetime) -> bool:
    if dt.weekday() >= 5:
        return False
    hm = dt.hour * 60 + dt.minute
    return (9 * 60 <= hm < 11 * 60 + 30) or (13 * 60 <= hm < 15 * 60)


def in_lunch(dt: datetime) -> bool:
    hm = dt.hour * 60 + dt.minute
    return 11 * 60 + 30 <= hm < 13 * 60


def session_epoch(dt: datetime, hhmmss: str) -> float:
    """'HH:MM:SS' (gio ICT cua phien) -> epoch giay."""
    try:
        h, m, s = (int(x) for x in hhmmss.split(":")[:3])
    except Exception:
        return time.time()
    base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return base.timestamp() + h * 3600 + m * 60 + s


def _f(v):
    """Chuoi/so -> float; rong/None -> None."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------- protobuf toi thieu (tu probe) ----------------

def _varint(buf, pos):
    shift = 0
    val = 0
    while True:
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, pos
        shift += 7


def pb_decode(buf, fields):
    """fields: {no: (name, kind)} kind s/i/u/z/Z/f(float32)/d(double)/
    n (nested, spec[2]=fields con)."""
    out = {}
    pos = 0
    while pos < len(buf):
        tag, pos = _varint(buf, pos)
        no, wt = tag >> 3, tag & 7
        spec = fields.get(no)
        if wt == 0:
            v, pos = _varint(buf, pos)
            if spec is None:
                out[f"f{no}"] = v
                continue
            name, kind = spec[0], spec[1]
            if kind in ("z", "Z"):
                v = (v >> 1) ^ -(v & 1)
            out[name] = v
        elif wt == 2:
            ln, pos = _varint(buf, pos)
            raw = buf[pos:pos + ln]
            pos += ln
            if spec is None:
                # co the la message long: giu dang dict f-no
                try:
                    out[f"f{no}"] = pb_decode(raw, {})
                except Exception:
                    out[f"f{no}"] = f"<{ln}b>"
                continue
            name, kind = spec[0], spec[1]
            if kind == "s":
                out[name] = raw.decode("utf-8", "replace")
            elif kind == "n":
                out.setdefault(name, []).append(
                    pb_decode(raw, spec[2]))
            else:
                out[name] = raw
        elif wt == 5:
            v = struct.unpack("<f", buf[pos:pos + 4])[0]
            pos += 4
            out[spec[0] if spec else f"f{no}"] = v
        elif wt == 1:
            v = struct.unpack("<d", buf[pos:pos + 8])[0]
            pos += 8
            out[spec[0] if spec else f"f{no}"] = v
        else:
            break
    return out


# ---------------- MQTT 3.1.1 over WebSocket (tu probe) ----------------

def _enc_str(s):
    b = s.encode()
    return struct.pack(">H", len(b)) + b


def _enc_rem(n):
    out = b""
    while True:
        b = n % 128
        n //= 128
        if n:
            out += bytes([b | 0x80])
        else:
            out += bytes([b])
            return out


def _mqtt_connect(cid, user, pw, keepalive=60):
    vh = _enc_str("MQTT") + bytes([4, 0xC2]) + struct.pack(
        ">H", keepalive)
    body = vh + _enc_str(cid) + _enc_str(user) + _enc_str(pw)
    return b"\x10" + _enc_rem(len(body)) + body


def _mqtt_subscribe(pkt_id, topics):
    body = struct.pack(">H", pkt_id)
    for t in topics:
        body += _enc_str(t) + b"\x00"
    return b"\x82" + _enc_rem(len(body)) + body


def _mqtt_parse_publish(msg):
    pos = 1
    mult = 1
    rem = 0
    while True:
        b = msg[pos]
        pos += 1
        rem += (b & 0x7F) * mult
        if not (b & 0x80):
            break
        mult *= 128
    tlen = struct.unpack(">H", msg[pos:pos + 2])[0]
    pos += 2
    topic = msg[pos:pos + tlen].decode()
    pos += tlen
    if msg[0] & 0x06:  # QoS > 0: bo qua packet id
        pos += 2
    return topic, msg[pos:]


class MqttWs:
    """MQTT tren WS: tu reconnect + subscribe lai + PINGREQ 25s."""

    def __init__(self, url, user, pw, topics, on_msg, subprotocol="mqtt",
                 origin=None):
        self.url, self.user, self.pw = url, user, pw
        self.topics, self.on_msg = topics, on_msg
        self.subprotocol = subprotocol
        self.origin = origin
        self.last_msg = 0.0
        self.msgs = 0
        self.connected = False
        self.last_error = ""
        self._stop = False

    async def run(self):
        from websockets.asyncio.client import connect
        backoff = 2
        headers = {"User-Agent": "Mozilla/5.0"}
        if self.origin:
            headers["Origin"] = self.origin
        while not self._stop:
            try:
                cid = "tick-" + "".join(
                    random.choices("abcdef0123456789", k=8))
                async with connect(
                        self.url, subprotocols=[self.subprotocol],
                        open_timeout=20, ping_interval=None,
                        additional_headers=headers,
                        max_size=8 * 1024 * 1024) as ws:
                    await ws.send(_mqtt_connect(cid, self.user, self.pw))
                    msg = await asyncio.wait_for(ws.recv(), 20)
                    if not (isinstance(msg, bytes) and msg[0] >> 4 == 2
                            and msg[3] == 0):
                        raise ConnectionError(f"CONNACK rc={msg!r:.40}")
                    await ws.send(_mqtt_subscribe(1, self.topics))
                    self.connected = True
                    backoff = 2
                    last_ping = time.time()
                    while not self._stop:
                        if time.time() - last_ping > 25:
                            await ws.send(b"\xc0\x00")
                            last_ping = time.time()
                        try:
                            msg = await asyncio.wait_for(ws.recv(), 10)
                        except asyncio.TimeoutError:
                            await ws.send(b"\xc0\x00")
                            last_ping = time.time()
                            continue
                        if not isinstance(msg, bytes) or not msg:
                            continue
                        if msg[0] >> 4 == 3:
                            topic, payload = _mqtt_parse_publish(msg)
                            self.last_msg = time.time()
                            self.msgs += 1
                            self.on_msg(topic, payload)
            except Exception as e:  # noqa: BLE001
                self.connected = False
                self.last_error = f"{type(e).__name__}: {str(e)[:120]}"
                print(f"[mqtt {self.url.split('/')[2]}] loi: "
                      f"{self.last_error}", flush=True)
                if self._stop:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._stop = True


# ---------------- Socket.IO (Engine.IO v4) dung chung KBS/VPS/VCI ---

class SioWs:
    """Socket.IO toi thieu tren websockets: 0->40, 2->3 (pong),
    42[event, payload] -> on_event. Tu reconnect + on_connected."""

    def __init__(self, url, on_event, on_connected, origin=None):
        self.url = url
        self.on_event = on_event
        self.on_connected = on_connected
        self.origin = origin
        self.last_msg = 0.0
        self.msgs = 0
        self.connected = False
        self.last_error = ""
        self._stop = False
        self._ws = None

    async def send_event(self, event, payload):
        if self._ws is not None:
            await self._ws.send("42" + json.dumps(
                [event, payload], ensure_ascii=False))

    async def run(self):
        from websockets.asyncio.client import connect
        backoff = 2
        headers = {"User-Agent": "Mozilla/5.0"}
        if self.origin:
            headers["Origin"] = self.origin
        while not self._stop:
            try:
                async with connect(
                        self.url, ping_interval=None, open_timeout=20,
                        max_size=16 * 1024 * 1024,
                        additional_headers=headers) as ws:
                    self._ws = ws
                    backoff = 2
                    while not self._stop:
                        msg = await asyncio.wait_for(ws.recv(), 30)
                        if isinstance(msg, bytes):
                            continue
                        self.last_msg = time.time()
                        if msg.startswith("0"):
                            await ws.send("40")
                        elif msg == "2":
                            await ws.send("3")
                        elif msg.startswith("40"):
                            self.connected = True
                            await self.on_connected(self)
                        elif msg.startswith("42"):
                            self.msgs += 1
                            try:
                                arr = json.loads(msg[2:])
                                self.on_event(arr[0], arr[1]
                                              if len(arr) > 1 else None)
                            except Exception:  # noqa: BLE001
                                pass
            except Exception as e:  # noqa: BLE001
                self.connected = False
                self._ws = None
                self.last_error = f"{type(e).__name__}: {str(e)[:120]}"
                print(f"[sio {self.url.split('/')[2]}] loi: "
                      f"{self.last_error}", flush=True)
                if self._stop:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._stop = True


# ---------------- SSI (giu nguyen — KHONG chay o Tang A) ----------------

SSI_STOCK = {17: ("matchedPrice", "i"), 18: ("matchedVolume", "i"),
             19: ("matchedTime", "i"), 22: ("openPrice", "i"),
             23: ("highest", "i"), 24: ("lowest", "i"),
             25: ("avgPrice", "i"), 26: ("nmTotalTradedQty", "i"),
             27: ("nmTotalTradedValue", "i"),
             70: ("buyForeignQtty", "i"), 71: ("buyForeignValue", "i"),
             72: ("sellForeignQtty", "i"),
             73: ("sellForeignValue", "i"),
             83: ("remainForeignQtty", "Z"),
             89: ("stockSymbol", "s")}
SSI_LE = {1: ("items", "n", {1: ("price", "u"), 2: ("vol", "u"),
                             3: ("accumulatedVol", "i"),
                             4: ("time", "s"), 6: ("side", "s"),
                             10: ("sendingTime", "i")})}
SSI_PE = {1: ("items", "n", {3: ("stockSymbol", "s"), 4: ("vol", "i"),
                             5: ("val", "i"), 6: ("price", "i"),
                             9: ("time", "s"),
                             14: ("buyForeignQtty", "i"),
                             15: ("buyForeignValue", "i"),
                             16: ("sellForeignQtty", "i"),
                             17: ("sellForeignValue", "i"),
                             18: ("remainForeignQtty", "Z")})}


_SSI_CFG = None

# Ban chup cau hinh cong khai cua SSI tai thoi diem build
# (hang so nhung trong JS cong khai cua FiinTrade; chi dung
# khi khong trich truc tiep duoc tu bundle khi chay).
_SSI_SNAPSHOT = ('wss://price-streaming.ssi.com.vn/mqtt', 'mqtt-ssi', 'mqtt-ssi')


def ssi_config():
    """Thong tin MQTT SSI: env neu co, khong thi trich tu bundle
    cong khai cua FiinTrade (MQTT_USERNAME/PASSWORD/ENDPOINT la
    hang so nhung trong JS cong khai cua SSI — probe 21/10 da
    dung chinh duong nay). Cache trong process."""
    global _SSI_CFG
    if _SSI_CFG:
        return _SSI_CFG
    user = os.environ.get("SSI_USER")
    pw = os.environ.get("SSI_PW")
    ep = "wss://price-streaming.ssi.com.vn/mqtt"
    if not (user and pw):
        import re
        import urllib.request as ur
        hdr = {"User-Agent": "Mozilla/5.0"}
        base = "https://fiintrade.ssi.com.vn/"
        try:
            html = ur.urlopen(ur.Request(base, headers=hdr),
                              timeout=30).read().decode(
                                  "utf-8", "replace")
            assets = re.findall(r'(?:src|href)="([^"]+\.js)"', html)
        except Exception:  # noqa: BLE001
            assets = []
        for a in assets:
            url = a if a.startswith("http") else base + a.lstrip("/")
            try:
                js = ur.urlopen(ur.Request(url, headers=hdr),
                                timeout=30).read().decode(
                                    "utf-8", "replace")
            except Exception:  # noqa: BLE001
                continue
            m1 = re.search(r'MQTT_USERNAME="([^"]*)"', js)
            m2 = re.search(r'MQTT_PASSWORD="([^"]*)"', js)
            if m1 and m2:
                user, pw = m1.group(1), m2.group(1)
                m3 = re.search(r'MQTT_ENDPOINT="([^"]*)"', js)
                if m3:
                    ep = m3.group(1)
                break
    if not (user and pw):
        ep, user, pw = _SSI_SNAPSHOT
    _SSI_CFG = (ep, user, pw)
    return _SSI_CFG


class SsiAdapter:
    name = "ssi-mqtt"

    def __init__(self, agg, universe):
        self.agg = agg
        topics = []
        for s in universe:
            topics += [f"s/{s}/MAIN", f"l/{s}/MAIN"]
        topics += ["pe", "i/VNINDEX", "i/VN30"]
        ep, user, pw = ssi_config()
        self.client = MqttWs(ep, user, pw, topics, self._on_msg,
                             origin="https://fiintrade.ssi.com.vn")

    @property
    def last_msg(self):
        return self.client.last_msg

    @property
    def msgs(self):
        return self.client.msgs

    @property
    def connected(self):
        return self.client.connected

    @property
    def last_error(self):
        return self.client.last_error

    async def run(self):
        await self.client.run()

    def stop(self):
        self.client.stop()

    def _on_msg(self, topic, payload):
        try:
            if topic.startswith("s/"):
                d = pb_decode(payload, SSI_STOCK)
                sym = d.get("stockSymbol") or topic.split("/")[1]
                if d.get("matchedPrice"):
                    self.agg.on_snapshot(self.name, sym, {
                        "last": d["matchedPrice"] / 1000,
                        "total_volume": d.get("nmTotalTradedQty"),
                        "total_value": d.get("nmTotalTradedValue"),
                        "buy_volume": d.get("buyForeignQtty"),
                        "buy_value": d.get("buyForeignValue"),
                        "sell_volume": d.get("sellForeignQtty"),
                        "sell_value": d.get("sellForeignValue"),
                        "room": d.get("remainForeignQtty"),
                    })
            elif topic.startswith("l/"):
                sym = topic.split("/")[1]
                d = pb_decode(payload, SSI_LE)
                for it in d.get("items", []):
                    if it.get("price") and it.get("vol"):
                        self.agg.on_match(
                            self.name, sym,
                            session_epoch(now_ict(), it.get("time", "")),
                            it["price"] / 1000, it["vol"],
                            it.get("accumulatedVol"))
            elif topic == "pe":
                d = pb_decode(payload, SSI_PE)
                for it in d.get("items", []):
                    if it.get("stockSymbol"):
                        self.agg.on_put(self.name, it["stockSymbol"], it)
        except Exception as e:  # noqa: BLE001
            print(f"[ssi] decode loi {topic}: {str(e)[:100]}",
                  flush=True)


# ---------------- DNSE (nguon chinh) ----------------

class DnseAdapter:
    name = "dnse"

    def __init__(self, agg, universe):
        self.agg = agg
        topics = []
        for s in universe:
            topics += [
                f"quotes/krx/mdds/stockinfo/v1/roundlot/symbol/{s}",
                f"quotes/krx/mdds/tick/v1/roundlot/symbol/{s}"]
        # entrade/entrade: tai khoan cong khai trong bundle DNSE
        self.client = MqttWs(
            "wss://datafeed-krx.dnse.com.vn/wss",
            "entrade", "entrade", topics, self._on_msg,
            origin="https://banggia.dnse.com.vn")

    @property
    def last_msg(self):
        return self.client.last_msg

    @property
    def msgs(self):
        return self.client.msgs

    @property
    def connected(self):
        return self.client.connected

    @property
    def last_error(self):
        return self.client.last_error

    async def run(self):
        await self.client.run()

    def stop(self):
        self.client.stop()

    @staticmethod
    def _inner(d, key):
        v = d.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v[0]
        return v if isinstance(v, dict) else {}

    @staticmethod
    def _num(node, key, scale=1):
        v = node.get(key)
        if isinstance(v, list):
            v = v[0] if v else None
        if isinstance(v, (int, float)):
            return v * scale
        return None

    def _on_msg(self, topic, payload):
        try:
            d = pb_decode(payload, {})
            if "stockinfo" in topic:
                m = self._inner(d, "f41")
                sym = self._num(m, "f4")
                if not isinstance(sym, str):
                    sym = topic.rsplit("/", 1)[-1]
                self.agg.on_snapshot(self.name, sym, {
                    "last": self._num(m, "f13"),
                    "total_volume": self._num(m, "f22", 10),
                    "total_value": self._num(m, "f25", 10**9),
                    "buy_volume": self._num(m, "f17", 10),
                    "buy_value": self._num(m, "f58", 10**9),
                    "sell_volume": self._num(m, "f18", 10),
                    "sell_value": self._num(m, "f59", 10**9),
                    "room": self._num(m, "f20", 10),
                    "total_room": self._num(m, "f60", 10),
                })
            elif "/tick/" in topic:
                m = self._inner(d, "f45")
                sym = topic.rsplit("/", 1)[-1]
                ts_node = self._inner(m, "f7")
                ts = self._num(ts_node, "f1") or time.time()
                price = self._num(m, "f5")
                vol = self._num(m, "f6", 10)
                if price and vol:
                    self.agg.on_match(self.name, sym, ts, price, vol,
                                      self._num(m, "f10", 10))
        except Exception as e:  # noqa: BLE001
            print(f"[dnse] decode loi {topic}: {str(e)[:100]}",
                  flush=True)


# ---------------- KBS (doi chieu: TRADE + STOCK, JSON thuan) ----------------
# Giao thuc trich tu bundle cong khai KB Buddy WTS + kiem chung live
# 21/10 (REPORT-kbs + REPORT-LIVE-2110): Socket.IO khong auth, emit
# USER bang dinh danh khach hard-code trong bundle cong khai,
# subscribe "sub+STOCK:<s>+TRADE:<s>", server day event PUB dang
# "TYPE:SYM {json}". Don vi goc: gia dong, KL CP — khong nhan he so.
# Field ngoai (chot REPORT-LIVE-2110): FB/FS = KL mua/ban ngoai,
# FO = TONG room, FR = room CON LAI. Khong co GT ngoai o KBS.

class KbsAdapter:
    name = "kbs"
    URL = ("wss://kbbuddyrt.kbsec.com.vn/socket.io/"
           "?EIO=4&transport=websocket")
    USER_ID = "091C222333"  # dinh danh khach trong bundle cong khai

    def __init__(self, agg, universe):
        self.agg = agg
        self.universe = universe
        self.client = SioWs(self.URL, self._on_event, self._on_connected)

    @property
    def last_msg(self):
        return self.client.last_msg

    @property
    def msgs(self):
        return self.client.msgs

    @property
    def connected(self):
        return self.client.connected

    @property
    def last_error(self):
        return self.client.last_error

    async def run(self):
        await self.client.run()

    def stop(self):
        self.client.stop()

    async def _on_connected(self, client):
        await client.send_event("USER", self.USER_ID)
        batch = []
        for s in self.universe:
            batch += [f"STOCK:{s}", f"TRADE:{s}"]
            if len(batch) >= 16:
                await client.send_event("msg", "sub+" + "+".join(batch))
                batch = []
        if batch:
            await client.send_event("msg", "sub+" + "+".join(batch))
        await client.send_event(
            "msg", "sub+INDEX:VNINDEX+INDEX:VN30")

    def _on_event(self, event, payload):
        if event != "PUB" or not isinstance(payload, str):
            return
        try:
            head, _, body = payload.partition(" ")
            typ, _, sym = head.partition(":")
            d = json.loads(body)
            if typ == "STOCK":
                cp = _f(d.get("CP"))
                self.agg.on_snapshot(self.name, sym, {
                    "last": cp / 1000 if cp else None,
                    "total_volume": _f(d.get("TT")),
                    "total_value": _f(d.get("TV")),
                    "buy_volume": _f(d.get("FB")),
                    "sell_volume": _f(d.get("FS")),
                    "room": _f(d.get("FR")),
                    "total_room": _f(d.get("FO")),
                })
            elif typ == "TRADE":
                price = _f(d.get("FMP"))
                vol = _f(d.get("FV"))
                if price and vol:
                    self.agg.on_match(
                        self.name, sym,
                        session_epoch(now_ict(), d.get("FT", "")),
                        price / 1000, vol, _f(d.get("AVO")))
        except Exception as e:  # noqa: BLE001
            print(f"[kbs] decode loi: {str(e)[:100]}", flush=True)


# ---------------- VNDirect (doi chieu: MQTT 5.0, text positional) --------
# Endpoint FREE cho khach an danh (bundle Lightning khai cong khai);
# username = API key nhung trong bundle — trich truc tiep tu bundle
# khi chay (nhu SSI), khong duoc thi ket noi an danh (matrix probe
# 21/10: broker van cap CONNACK/SUBACK). Payload: d|<room>:["msg"]
# moi msg ma hoa lech ky tu chr(c + i%5) — da giai + kiem chung:
# ST|VCB|09:17:54|10|56.0|10.0|PS. Don vi: moi field KL x10 CP;
# SMA f8 (tong GT) la TRIEU dong x10^6; gia nghin dong.

_VND_KEY = None
_VND_FLOOR = {"SHB": "02"}  # trong universe chi SHB o HNX, con lai HOSE


def vndirect_key():
    """API key FREE cua VNDirect trich tu bundle banggia cong khai."""
    global _VND_KEY
    if _VND_KEY is not None:
        return _VND_KEY
    import re
    import urllib.request as ur
    hdr = {"User-Agent": "Mozilla/5.0"}
    key = ""
    try:
        base = "https://banggia.vndirect.com.vn/"
        html = ur.urlopen(ur.Request(base, headers=hdr),
                          timeout=30).read().decode("utf-8", "replace")
        m = re.search(r'static/js/main\.[a-f0-9]+\.js', html)
        if m:
            js = ur.urlopen(ur.Request(base + m.group(0), headers=hdr),
                            timeout=60).read().decode("utf-8", "replace")
            i = js.find("PRICE_STREAMING_API_KEY")
            seg = js[i:i + 400] if i >= 0 else ""
            keys = re.findall(r'"([0-9a-f]{16,})"', seg)
            if keys:
                key = keys[1] if len(keys) > 1 else keys[0]
    except Exception:  # noqa: BLE001
        key = ""
    _VND_KEY = key
    return key


def _mqtt5_connect(cid, user, keepalive=60):
    flags = 0x82 if user else 0x02  # username + clean start
    vh = _enc_str("MQTT") + bytes([5, flags]) + struct.pack(
        ">H", keepalive) + b"\x00"  # properties len 0
    payload = _enc_str(cid)
    if user:
        payload += _enc_str(user)
    body = vh + payload
    return b"\x10" + _enc_rem(len(body)) + body


def _mqtt5_subscribe(pkt_id, topics):
    body = struct.pack(">H", pkt_id) + b"\x00"
    for t in topics:
        body += _enc_str(t) + b"\x00"
    return b"\x82" + _enc_rem(len(body)) + body


def _read_rem(buf, pos):
    mult = 1
    rem = 0
    while True:
        b = buf[pos]
        pos += 1
        rem += (b & 0x7F) * mult
        if not (b & 0x80):
            return rem, pos
        mult *= 128


def _mqtt5_parse_publish(msg):
    _, pos = _read_rem(msg, 1)
    tlen = struct.unpack(">H", msg[pos:pos + 2])[0]
    pos += 2
    topic = msg[pos:pos + tlen].decode()
    pos += tlen
    if msg[0] & 0x06:  # QoS > 0: bo packet id
        pos += 2
    plen, pos = _read_rem(msg, pos)  # v5 properties
    pos += plen
    return topic, msg[pos:]


class VndirectAdapter:
    name = "vndirect"
    URL = "wss://price-streaming-free.vndirect.com.vn/mqtt"

    def __init__(self, agg, universe):
        self.agg = agg
        topics = []
        for s in universe:
            topics.append(f"T/{_VND_FLOOR.get(s, '10')}/{s}")
        topics += ["10/+", "02/+", "MI/10"]
        self.topics = topics
        self.last_msg = 0.0
        self.msgs = 0
        self.connected = False
        self.last_error = ""
        self._stop = False
        self._seen_ticks = set()  # dedupe ST (khong co accumulated)

    async def run(self):
        from websockets.asyncio.client import connect
        backoff = 2
        while not self._stop:
            try:
                cid = "free-user-" + "".join(
                    random.choices("abcdef0123456789", k=8))
                async with connect(
                        self.URL, subprotocols=["mqtt"],
                        open_timeout=20, ping_interval=None,
                        origin="https://banggia.vndirect.com.vn",
                        additional_headers={"User-Agent": "Mozilla/5.0"},
                        max_size=16 * 1024 * 1024) as ws:
                    await ws.send(_mqtt5_connect(cid, vndirect_key()))
                    msg = await asyncio.wait_for(ws.recv(), 20)
                    if not (isinstance(msg, bytes) and msg[0] >> 4 == 2):
                        raise ConnectionError(f"khong CONNACK {msg!r:.40}")
                    _, p = _read_rem(msg, 1)
                    if msg[p + 1] != 0:
                        raise ConnectionError(
                            f"CONNACK reason={msg[p + 1]}")
                    await ws.send(_mqtt5_subscribe(1, self.topics))
                    self.connected = True
                    backoff = 2
                    last_ping = time.time()
                    while not self._stop:
                        if time.time() - last_ping > 25:
                            await ws.send(b"\xc0\x00")
                            last_ping = time.time()
                        try:
                            msg = await asyncio.wait_for(ws.recv(), 10)
                        except asyncio.TimeoutError:
                            await ws.send(b"\xc0\x00")
                            last_ping = time.time()
                            continue
                        if not isinstance(msg, bytes) or not msg:
                            continue
                        if msg[0] >> 4 == 3:
                            topic, payload = _mqtt5_parse_publish(msg)
                            self.last_msg = time.time()
                            self.msgs += 1
                            self._on_payload(topic, payload)
            except Exception as e:  # noqa: BLE001
                self.connected = False
                self.last_error = f"{type(e).__name__}: {str(e)[:120]}"
                print(f"[vndirect] loi: {self.last_error}", flush=True)
                if self._stop:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._stop = True

    @staticmethod
    def _decode(raw):
        return "".join(chr(ord(c) + i % 5) for i, c in enumerate(raw))

    def _on_payload(self, topic, payload):
        try:
            text = payload.decode("utf-8", "replace")
            if not text.startswith("d|"):
                return
            _, _, body = text[2:].partition(":")
            msgs = json.loads(body)
        except Exception:  # noqa: BLE001
            return
        for raw in msgs if isinstance(msgs, list) else []:
            try:
                self._on_msg(self._decode(raw))
            except Exception:  # noqa: BLE001
                continue

    def _on_msg(self, text):
        parts = text.split("|")
        kind = parts[0]
        if kind == "ST" and len(parts) >= 6:
            sym = parts[1]
            price, vol = _f(parts[4]), _f(parts[5])
            if not price or not vol:
                return
            key = (sym, parts[2], price, vol,
                   parts[6] if len(parts) > 6 else "")
            if key in self._seen_ticks:
                return
            self._seen_ticks.add(key)
            self.agg.on_match(
                self.name, sym, session_epoch(now_ict(), parts[2]),
                price, vol * 10, None)
        elif kind == "SMA" and len(parts) >= 16:
            sym = parts[1]
            tv = _f(parts[8])
            self.agg.on_snapshot(self.name, sym, {
                "last": _f(parts[10]),
                "total_volume": (_f(parts[9]) or 0) * 10
                if _f(parts[9]) is not None else None,
                "total_value": tv * 10**6 if tv is not None else None,
                "buy_volume": (_f(parts[4]) or 0) * 10
                if _f(parts[4]) is not None else None,
                "sell_volume": (_f(parts[5]) or 0) * 10
                if _f(parts[5]) is not None else None,
                "room": (_f(parts[15]) or 0) * 10
                if _f(parts[15]) is not None else None,
                "total_room": (_f(parts[14]) or 0) * 10
                if _f(parts[14]) is not None else None,
            })


# ---------------- VPS (chan bonus: 3250 ngoai + 3220 tick + tong hop) ----
# Socket.IO bgdatafeed — bat tay NAC DANH da kiem chung (host danh cho
# phien dang nhap nhung server chap nhan an danh + day du lieu that;
# chap nhan tinh mong manh theo Chot). Don vi da doi chieu da cong:
# KL x10 CP, GT ngoai NGHIN dong x1000, room x10. Tong hop toan thi
# truong (aggregatemarket...), phai sinh (aggregateps), lo le
# (soddlot), thoa thuan (spt): luu RAW vao Aggregator.aggregates.

class VpsAdapter:
    name = "vps"
    URL = ("wss://bgdatafeed.vps.com.vn/socket.io/"
           "?EIO=4&transport=websocket")

    def __init__(self, agg, universe):
        self.agg = agg
        self.universe = universe
        self.client = SioWs(self.URL, self._on_event, self._on_connected)

    @property
    def last_msg(self):
        return self.client.last_msg

    @property
    def msgs(self):
        return self.client.msgs

    @property
    def connected(self):
        return self.client.connected

    @property
    def last_error(self):
        return self.client.last_error

    async def run(self):
        await self.client.run()

    def stop(self):
        self.client.stop()

    async def _on_connected(self, client):
        await client.send_event("regs", json.dumps(
            {"action": "join", "list": ",".join(self.universe)}))

    def _on_event(self, event, payload):
        try:
            if event == "stock":
                d = (payload or {}).get("data") or {}
                if d.get("id") != 3220 or not d.get("sym"):
                    return
                sym = d["sym"]
                last = _f(d.get("lastPrice"))
                tvol = _f(d.get("totalVol"))
                self.agg.on_snapshot(self.name, sym, {
                    "last": last,
                    "total_volume": tvol * 10
                    if tvol is not None else None,
                })
                lvol = _f(d.get("lastVol"))
                if last and lvol:
                    self.agg.on_match(
                        self.name, sym,
                        session_epoch(now_ict(),
                                      d.get("time") or ""),
                        last, lvol * 10,
                        tvol * 10 if tvol is not None else None)
            elif event == "board":
                d = (payload or {}).get("data") or {}
                if d.get("id") != 3250 or not d.get("symbol"):
                    return
                bv, sv = _f(d.get("fBVol")), _f(d.get("fSVolume"))
                bval, sval = _f(d.get("fBValue")), _f(d.get("fSValue"))
                room = _f(d.get("fRoom"))
                self.agg.on_snapshot(self.name, d["symbol"], {
                    "buy_volume": bv * 10 if bv is not None else None,
                    "sell_volume": sv * 10 if sv is not None else None,
                    "buy_value": bval * 1000
                    if bval is not None else None,
                    "sell_value": sval * 1000
                    if sval is not None else None,
                    "room": room * 10 if room is not None else None,
                })
            elif event in ("aggregatemarket", "aggregateforeignmarket",
                           "aggregateps", "aggregatecw", "aggregateetf"):
                self.agg.on_aggregate(event, payload)
            elif event in ("spt", "soddlot"):
                self.agg.on_aggregate(event, None)  # chi dem, khong luu raw
        except Exception as e:  # noqa: BLE001
            print(f"[vps] decode loi: {str(e)[:100]}", flush=True)


# ---------------- VCI (giu nguyen — Tang B chay o OVH, khong chay Modal) -

VCI_MATCH = {1: ("type", "s"), 2: ("code", "s"), 3: ("symbol", "s"),
             4: ("matchPrice", "d"), 5: ("matchVol", "d"),
             8: ("foreignBuyVolume", "d"),
             9: ("foreignSellVolume", "d"),
             10: ("foreignBuyValue", "d"),
             11: ("foreignSellValue", "d"),
             16: ("accumulatedVolume", "d"), 18: ("time", "s"),
             22: ("currentRoom", "d"), 23: ("totalRoom", "d"),
             24: ("accumulatedValue", "d")}


class VciAdapter:
    name = "vci"
    URL = ("wss://trading.vietcap.com.vn/ws/price/socket.io/"
           "?EIO=4&transport=websocket")

    def __init__(self, agg, universe):
        self.agg = agg
        self.universe = universe
        self.last_msg = 0.0
        self.msgs = 0
        self.connected = False
        self.last_error = ""
        self._stop = False

    async def run(self):
        from websockets.asyncio.client import connect
        backoff = 2
        while not self._stop:
            try:
                async with connect(
                        self.URL, ping_interval=None, open_timeout=20,
                        max_size=8 * 1024 * 1024,
                        additional_headers={
                            "Origin": "https://trading.vietcap.com.vn",
                            "User-Agent": "Mozilla/5.0"}) as ws:
                    self.connected = True
                    backoff = 2
                    pend_ev = []
                    pending_bin = []
                    while not self._stop:
                        msg = await asyncio.wait_for(ws.recv(), 30)
                        if isinstance(msg, bytes):
                            raw = msg[1:] if msg[:1] == b"b" else msg
                            if pend_ev:
                                ev = pend_ev.pop(0)
                                self._handle(ev, raw)
                            else:
                                pending_bin.append(raw)
                            continue
                        if msg.startswith("0"):
                            await ws.send("40")
                        elif msg == "2":
                            await ws.send("3")
                        elif msg.startswith("40"):
                            subs = json.dumps({"symbols": self.universe})
                            for ev, arg in (
                                    ("app-config", '[{"type":"all"}]'),
                                    ("w-match-price", subs),
                                    ("put-through", '{"group":"HOSE"}')):
                                await ws.send("42" + json.dumps(
                                    [ev, arg], ensure_ascii=False))
                        elif msg.startswith("45"):
                            dash = msg.index("-")
                            pend_ev.append(json.loads(msg[dash + 1:])[0])
                        elif msg.startswith("42"):
                            arr = json.loads(msg[2:])
                            ev, payload = arr[0], arr[1] \
                                if len(arr) > 1 else None
                            if isinstance(payload, dict) \
                                    and "_placeholder" in payload:
                                raw = pending_bin.pop(0) \
                                    if pending_bin else b""
                                self._handle(ev, raw)
            except Exception as e:  # noqa: BLE001
                self.connected = False
                self.last_error = f"{type(e).__name__}: {str(e)[:120]}"
                print(f"[vci] loi: {self.last_error}", flush=True)
                if self._stop:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _handle(self, ev, raw):
        if ev != "w-match-price" or not raw:
            return
        try:
            d = pb_decode(raw, VCI_MATCH)
        except Exception:  # noqa: BLE001
            return
        sym = d.get("symbol")
        if not sym:
            return
        self.last_msg = time.time()
        self.msgs += 1
        if d.get("foreignBuyVolume") is not None:
            self.agg.on_snapshot(self.name, sym, {
                "last": d.get("matchPrice"),
                "total_volume": d.get("accumulatedVolume"),
                "total_value": d.get("accumulatedValue"),
                "buy_volume": d.get("foreignBuyVolume"),
                "buy_value": d.get("foreignBuyValue"),
                "sell_volume": d.get("foreignSellVolume"),
                "sell_value": d.get("foreignSellValue"),
                "room": d.get("currentRoom"),
                "total_room": d.get("totalRoom"),
            })
        if d.get("matchPrice") and d.get("matchVol"):
            ts = session_epoch(now_ict(), d.get("time", "")) \
                if d.get("time") else time.time()
            self.agg.on_match(self.name, sym, ts, d["matchPrice"],
                              d["matchVol"], d.get("accumulatedVolume"))

    def stop(self):
        self._stop = True


# ---------------- Tong hop da nguon + hop nhat + doi chieu ----------------

class Aggregator:
    """Giu du lieu THEO TUNG NGUON, hop nhat khi build:
    - Nen: theo tung phut, uu tien SRC_PRIORITY (dnse chinh).
    - Khoi ngoai: snapshot cua cong chinh; thieu field thi dien
      tu cong ke tiep (vd DNSE thieu thi KBS/VPS bo sung).
    - Doi chieu cheo: so sanh snapshot cac cong voi cong chinh."""

    def __init__(self):
        self.candles = {}       # src -> sym -> {minute: [o,h,l,c,v]}
        self.foreign = {}       # src -> sym -> snapshot moi nhat
        self.put_foreign = {}   # sym -> cong don thoa thuan (SSI, neu co)
        self.aggregates = {}    # ten event VPS -> payload raw moi nhat
        self.agg_counts = {}    # ten event VPS -> so tin
        self._last_acc = {}     # (src, sym) -> accumulatedVol da xu ly
        self.matches_by_src = {}
        self.dirty = set()
        self.matches = 0

    def on_match(self, src, sym, ts, price, vol, acc):
        key = (src, sym)
        if acc is not None:
            if acc <= self._last_acc.get(key, -1):
                return  # ban ghi lap (reconnect replay)
            self._last_acc[key] = acc
        minute = int(ts // 60 * 60)
        per = self.candles.setdefault(src, {}).setdefault(sym, {})
        b = per.get(minute)
        if b is None:
            per[minute] = [price, price, price, price, vol]
        else:
            b[1] = max(b[1], price)
            b[2] = min(b[2], price)
            b[3] = price
            b[4] += vol
        self.dirty.add(sym)
        self.matches += 1
        self.matches_by_src[src] = self.matches_by_src.get(src, 0) + 1

    def on_snapshot(self, src, sym, snap):
        cur = self.foreign.setdefault(src, {}).setdefault(sym, {})
        for k, v in snap.items():
            if v is not None:
                cur[k] = v
        cur["src"] = src

    def on_put(self, src, sym, it):
        cur = self.put_foreign.setdefault(sym, {
            "put_buy_volume": 0, "put_buy_value": 0,
            "put_sell_volume": 0, "put_sell_value": 0, "put_deals": 0})
        cur["put_buy_volume"] += it.get("buyForeignQtty") or 0
        cur["put_buy_value"] += it.get("buyForeignValue") or 0
        cur["put_sell_volume"] += it.get("sellForeignQtty") or 0
        cur["put_sell_value"] += it.get("sellForeignValue") or 0
        cur["put_deals"] += 1
        cur["src"] = src

    def on_aggregate(self, name, payload):
        self.agg_counts[name] = self.agg_counts.get(name, 0) + 1
        if payload is not None:
            self.aggregates[name] = payload

    @staticmethod
    def _rows(buckets):
        # Dinh dang t giong het intraday REST noi tieu thu dang doc:
        # chuoi gio ICT "%Y-%m-%dT%H:%M" (source_router._intraday_vnd).
        return [{"t": datetime.fromtimestamp(t, ICT).strftime(
                    "%Y-%m-%dT%H:%M"),
                 "o": b[0], "h": b[1], "l": b[2], "c": b[3], "v": b[4]}
                for t, b in sorted(buckets.items())]

    @staticmethod
    def _resample(rows1m, minutes):
        out = {}
        for r in rows1m:
            t = datetime.strptime(r["t"], "%Y-%m-%dT%H:%M") \
                .replace(tzinfo=ICT)
            k = int(t.timestamp()) // (minutes * 60) * (minutes * 60)
            b = out.get(k)
            if b is None:
                out[k] = [r["o"], r["h"], r["l"], r["c"], r["v"]]
            else:
                b[1] = max(b[1], r["h"])
                b[2] = min(b[2], r["l"])
                b[3] = r["c"]
                b[4] += r["v"]
        return Aggregator._rows(out)

    def _merged_buckets(self, sym):
        """{minute: bucket} — moi phut lay tu cong uu tien cao nhat
        co phut do (failover muc phut)."""
        merged = {}
        for src in reversed(SRC_PRIORITY):
            for minute, b in self.candles.get(src, {}).get(
                    sym, {}).items():
                merged[minute] = b
        return merged

    def build_intraday(self, syms):
        out = {}
        for sym in syms:
            rows1m = self._rows(self._merged_buckets(sym))
            if not rows1m:
                continue
            out[f"{sym}_1m"] = rows1m
            out[f"{sym}_5m"] = self._resample(rows1m, 5)
            out[f"{sym}_15m"] = self._resample(rows1m, 15)
            out[f"{sym}_1H"] = self._resample(rows1m, 60)
        return out

    def _merged_foreign_sym(self, sym):
        snaps = [(src, self.foreign[src][sym]) for src in SRC_PRIORITY
                 if sym in self.foreign.get(src, {})]
        if not snaps:
            return None
        merged = dict(snaps[0][1])
        for src, snap in snaps[1:]:
            for k, v in snap.items():
                if merged.get(k) is None and v is not None:
                    merged[k] = v
        merged["src"] = snaps[0][0]
        merged["sources"] = [s for s, _ in snaps]
        return merged

    def build_foreign(self):
        out = {}
        syms = set()
        for src in self.foreign:
            syms.update(self.foreign[src])
        for sym in syms:
            e = self._merged_foreign_sym(sym)
            if e is None:
                continue
            if sym in self.put_foreign:
                e = dict(e)
                e.update(self.put_foreign[sym])
            out[sym] = e
        return out

    def build_foreign_flow(self):
        """Dang production market/foreign_flow.json: {sym: {buy_volume,
        sell_volume, net_volume, buy_value, sell_value, ...}}."""
        out = {}
        for sym, e in self.build_foreign().items():
            bv, sv = e.get("buy_volume"), e.get("sell_volume")
            if bv is None and sv is None:
                continue
            out[sym] = {
                "buy_volume": bv, "sell_volume": sv,
                "net_volume": (bv or 0) - (sv or 0)
                if bv is not None or sv is not None else None,
                "buy_value": e.get("buy_value"),
                "sell_value": e.get("sell_value"),
                "room": e.get("room"), "total_room": e.get("total_room"),
                "src": e.get("src"),
            }
        return out

    def crosscheck(self):
        """Doi chieu snapshot khoi ngoai giua cac cong voi cong chinh
        (theo tung ma): dem ma khop tuyet doi / lech + lech lon nhat
        cho buy_volume, sell_volume, total_volume, room."""
        fields = ("buy_volume", "sell_volume", "total_volume", "room")
        out = {}
        primary = next((s for s in SRC_PRIORITY if self.foreign.get(s)),
                       None)
        if primary is None:
            return out
        for src in SRC_PRIORITY:
            if src == primary or not self.foreign.get(src):
                continue
            stat = {f: {"compared": 0, "exact": 0, "max_abs_diff": 0}
                    for f in fields}
            for sym, snap in self.foreign[src].items():
                ref = self.foreign[primary].get(sym)
                if not ref:
                    continue
                for f in fields:
                    a, b = snap.get(f), ref.get(f)
                    if a is None or b is None:
                        continue
                    stat[f]["compared"] += 1
                    d = abs(a - b)
                    if d == 0:
                        stat[f]["exact"] += 1
                    stat[f]["max_abs_diff"] = max(
                        stat[f]["max_abs_diff"], d)
            out[f"{src}_vs_{primary}"] = stat
        return out


# ---------------- Publish ----------------

def publish(payload, source):
    secret = os.environ['STOCK_PUBLISH' '_SECRET']
    payload["_prov"] = {
        "source": f"tick:{source}",
        "fetched_at": datetime.now(timezone.utc).isoformat()}
    data = json.dumps(payload).encode()
    req = urllib.request.Request(PUBLISH_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Publish-Secret", secret)
    req.add_header("X-Internal-Secret", secret)
    req.add_header("User-Agent", "Mozilla/5.0")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


# ---------------- Dieu phoi da cong ----------------

ADAPTERS = {"dnse": DnseAdapter, "kbs": KbsAdapter,
            "vndirect": VndirectAdapter, "vps": VpsAdapter,
            "vci": VciAdapter, "ssi-mqtt": SsiAdapter}


async def _collect(max_minutes=0):
    universe = CONFIG["universe"]
    agg = Aggregator()
    started = time.time()
    start_dt = now_ict()
    adapters = {}
    tasks = {}
    for name in CONFIG["sources"]:
        ad = ADAPTERS[name](agg, universe)
        adapters[name] = ad
        tasks[name] = asyncio.create_task(ad.run())
    last_publish = 0.0
    publishes = 0
    print(f"[tick-multi] bat dau {start_dt.isoformat()} "
          f"cong={list(adapters)} ma={len(universe)}", flush=True)

    def session_over():
        dt = now_ict()
        if max_minutes:
            return time.time() - started > max_minutes * 60
        return dt.hour * 60 + dt.minute >= 15 * 60 + 5 \
            or dt.weekday() >= 5

    def build_status(closed=False):
        per_src = {}
        for name, ad in adapters.items():
            per_src[name] = {
                "connected": bool(ad.connected),
                "msgs": ad.msgs,
                "matches": agg.matches_by_src.get(name, 0),
                "last_msg_ago_sec": round(time.time() - ad.last_msg)
                if ad.last_msg else None,
                "last_error": ad.last_error or None,
                "symbols_foreign": len(agg.foreign.get(name, {})),
            }
        return {
            "session_date": start_dt.strftime("%Y-%m-%d"),
            "mode": "multi",
            "sources": per_src,
            "matches_aggregated": agg.matches,
            "symbols_with_candles": len({
                s for src in agg.candles for s in agg.candles[src]}),
            "crosscheck": agg.crosscheck(),
            "vps_aggregates": {
                "counts": agg.agg_counts,
                "aggregatemarket": agg.aggregates.get("aggregatemarket"),
                "aggregateforeignmarket": agg.aggregates.get(
                    "aggregateforeignmarket"),
                "aggregateps": agg.aggregates.get("aggregateps"),
            },
            "publishes": publishes,
            "runtime_sec": round(time.time() - started),
            "closed": closed,
        }

    async def do_publish(closed=False):
        nonlocal publishes
        status = build_status(closed)
        intra = agg.build_intraday(sorted(agg.dirty))
        agg.dirty.clear()
        foreign = agg.build_foreign()
        flow = agg.build_foreign_flow()
        keys = list(intra.keys())
        chunks = [keys[i:i + 10]
                  for i in range(0, len(keys), 10)] or [[]]
        for ci, ckeys in enumerate(chunks):
            chunk = {k: intra[k] for k in ckeys}
            if ci == 0:
                p = {"tick_shadow": {"status": status,
                                     "foreign": foreign}}
                live = {"tick_live": {"foreign": flow}}
                if chunk:
                    p["tick_shadow"]["intraday"] = chunk
                    live["tick_live"]["intraday"] = chunk
                await asyncio.to_thread(publish, p, "multi")
                await asyncio.to_thread(publish, live, "multi")
            elif chunk:
                await asyncio.to_thread(
                    publish, {"tick_shadow": {"intraday": chunk}},
                    "multi")
                await asyncio.to_thread(
                    publish, {"tick_live": {"intraday": chunk}},
                    "multi")
        publishes += 1

    while not session_over():
        await asyncio.sleep(5)
        if time.time() - last_publish >= CONFIG["publish_sec"]:
            last_publish = time.time()
            try:
                await do_publish()
            except Exception as e:  # noqa: BLE001
                print(f"[tick-multi] publish loi: {str(e)[:150]}",
                      flush=True)
    for ad in adapters.values():
        ad.stop()
    for t in tasks.values():
        t.cancel()
    try:
        await do_publish(closed=True)
    except Exception as e:  # noqa: BLE001
        print(f"[tick-multi] publish cuoi loi: {str(e)[:120]}",
              flush=True)
    print(f"[tick-multi] ket thuc phien: matches={agg.matches} "
          f"by_src={agg.matches_by_src} publishes={publishes} "
          f"runtime={round(time.time() - started)}s", flush=True)
