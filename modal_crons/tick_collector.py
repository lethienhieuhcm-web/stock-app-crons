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

THIET KE TONG THE BO THU (Chot Henrry 20:05 22/10, tai lieu
tick-collector-overall-design-2026-10-22) — DOT 1 (nen truong &
cong), ap tu ban fixed vong sua parity (ghi chu thiet ke
tick-multi-design-notes-2026-10-22): (1) FIELD_MAP khai bao mapping
+ co kiem chung ngu nghia; cong hop le hai lop — TAI ADAPTER
(gate_snap_fields, lop 1) va tai Aggregator.on_snapshot (lop 2):
sentinel/khoang vat ly -> None, publish bo truong; truc doi chieu
snapshot-vs-tick-tu-gom ngay trong phien. (2) Dedup tick theo dinh
danh acc (thay watermark don dieu), dem tick trung/lui/muon vao
status; chinh sach muon 120s (restate/late_deep); dedup state duoc
checkpoint vao status moi chu ky publish. (3) Close/open nen theo
ts SU KIEN (tick som/muon nhat trong phut), khong theo thu tu den.
(4) Luot publish dong phien kem khoi "diag" (snapshot theo nguon,
tick totals, volume phut theo nguon, tong thoa thuan) cho
reconcile dinh danh truong. Van chay bong, live=False.
"""
import modal

app = modal.App("stock-tick-collector")

image = (modal.Image.debian_slim(python_version="3.12")
         .pip_install("websockets>=14"))


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
import asyncio, base64, gzip, json, os, random, struct, time
import urllib.parse, urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

ICT = timezone(timedelta(hours=7))
PUBLISH_URL = "https://lethanhdong.one/api/internal/stock-publish"

CONFIG = {
    # Chu ky publish (giay)
    "publish_sec": 30,
    # Cong khoi dong o Tang A (Modal): 4 cong truc tiep + SSI qua
    # chuoi CONNECT proxy dan-cu (Dat 4, Chot 09:47 23/10 — chay
    # bong, vao parity nhu cong thu 5). VCI = chan Botkeep relay-4
    # (adapter con trong code, khong chay trong loi).
    "sources": ["dnse", "kbs", "vndirect", "vps", "ssi-mqtt"],
    # LAT PRODUCTION: False — DINH CHINH lenh 19:40 21/10: giu ban
    # da cong chay BONG them vai phien, bao parity tung phien; chi
    # bat True khi co lenh lat rieng sau (khi do tick_live moi ghi
    # production, tick_shadow van chay lam doi chieu).
    "live": False,
    # Resume giua phien (Dot 2): khoi dong lai -> nap checkpoint +
    # artifact theo nguon cua CHINH phien hom nay tu R2; loi/thieu
    # thi bat dau trang nhu cu, khong chan phien.
    "resume": True,
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

# ---------------- Bang mapping truong snapshot (khai bao) ----------------
# Mau FieldMapper cua vnstock (core/utils/field/): mapping theo nguon
# khai bao mot cho kem CO KIEM CHUNG NGU NGHIA. verified=True chi khi
# da doi chieu bang so that (request that / parity sach). verified=
# False = chua kiem chung: truong van thu thap de DO, nhung khong
# duoc coi la so that khi phan tich. Cap vat ly + cong sentinel o
# _gate_value (2 lop: gate_snap_fields tai adapter + Aggregator).
FIELD_MAP = {
    "dnse": {
        "total_volume": ("stockinfo f22", 10, False),   # dinh danh lai
        "total_value": ("stockinfo f25", 10**9, False),
        "buy_volume": ("stockinfo f17", 10, True),
        "buy_value": ("stockinfo f58", 10**9, True),
        "sell_volume": ("stockinfo f18", 10, True),
        "sell_value": ("stockinfo f59", 10**9, True),
        "room": ("stockinfo f20", 10, False),           # sentinel GVR
        "total_room": ("stockinfo f60", 10, False),
        "last": ("stockinfo f13", 1, True),
    },
    "ssi-mqtt": {
        "total_volume": ("snapshot nmTotalTradedQty", 1, False),
        "total_value": ("snapshot nmTotalTradedValue", 1, False),
        "buy_volume": ("snapshot buyForeignQtty", 1, True),
        "buy_value": ("snapshot buyForeignValue", 1, True),
        "sell_volume": ("snapshot sellForeignQtty", 1, True),
        "sell_value": ("snapshot sellForeignValue", 1, True),
        "room": ("snapshot remainForeignQtty", 1, False),
        "last": ("snapshot matchedPrice/1000", 1, True),
    },
    "kbs": {
        "total_volume": ("STOCK TT", 1, True),
        "total_value": ("STOCK TV", 1, True),
        "buy_volume": ("STOCK FB", 1, True),
        "sell_volume": ("STOCK FS", 1, True),
        "room": ("STOCK FR", 1, True),
        "total_room": ("STOCK FO", 1, True),
        "last": ("STOCK CP/1000", 1, True),
    },
    "vndirect": {
        "total_volume": ("SMA field9", 10, True),
        "total_value": ("SMA field8", 10**6, True),
        "buy_volume": ("SMA field4", 10, True),
        "sell_volume": ("SMA field5", 10, True),
        "room": ("SMA field15", 10, True),
        "total_room": ("SMA field14", 10, True),
        "last": ("SMA field10", 1, True),
    },
    "vps": {
        "total_volume": ("stock 3220 totalVol", 10, True),
        "buy_volume": ("board 3250 fBVol", 10, True),
        "sell_volume": ("board 3250 fSVolume", 10, True),
        "buy_value": ("board 3250 fBValue", 1000, True),
        "sell_value": ("board 3250 fSValue", 1000, True),
        "room": ("board 3250 fRoom", 10, True),
        "last": ("stock 3220 lastPrice", 1, True),
    },
    "vci": {
        "total_volume": ("match accumulatedVolume", 1, True),
        "total_value": ("match accumulatedValue", 1, True),
        "buy_volume": ("match foreignBuyVolume", 1, True),
        "buy_value": ("match foreignBuyValue", 1, True),
        "sell_volume": ("match foreignSellVolume", 1, True),
        "sell_value": ("match foreignSellValue", 1, True),
        "room": ("match currentRoom", 1, True),
        "total_room": ("match totalRoom", 1, True),
        "last": ("match matchPrice", 1, True),
    },
}

# Truong so luong phai qua cong hop le (don vi CP/dong sau scale)
QTY_FIELDS = ("total_volume", "total_value", "buy_volume", "buy_value",
              "sell_volume", "sell_value", "room", "total_room")
# Chan tren tuyet doi: vuot = sentinel/thieu du lieu (vd 2^64 qua
# float). 2^62 du lon cho moi KL/GD hop le cua 1 ma trong 1 phien.
ABSURD_CAP = 2 ** 62
# Nguong tick muon (thiet ke 3.4, Chot 20:05 22/10): tick den sau
# watermark cua nguon <= 120s -> restate phut; > 120s -> late_deep.
LATE_DEEP_SEC = 120


def _gate_value(src, key, v, room_cap, stats):
    """Luat cong hop le dung chung cho ca 2 lop (mau sentinel
    QuestDB/FIX SBE/ITCH + khoang hop le vat ly): NaN/inf/am/vuot
    ABSURD_CAP -> None + dem 'sentinel'; room > room_cap -> None +
    dem 'range'. Gia tri nguyen tra ve dang int (khong qua float).
    stats: dict dem theo nguon (Aggregator.gate_stats)."""
    if v is None:
        return None
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    st = stats.setdefault(src, {"sentinel": 0, "range": 0})
    if fv != fv or fv in (float("inf"), float("-inf")) \
            or fv < 0 or abs(fv) >= ABSURD_CAP:
        st["sentinel"] += 1
        return None
    out = int(fv) if fv == int(fv) else fv
    if key == "room" and room_cap is not None and out > room_cap:
        st["range"] += 1
        return None
    return out


def gate_snap_fields(src, snap, stats):
    """Cong hop le LOP 1 — TAI ADAPTER (thiet ke tong the, muc 3.5):
    adapter ap len dict snapshot TRUOC khi dua vao Aggregator, de
    gia tri hong khong bao gio cham state. Lop 2 o
    Aggregator.on_snapshot chay lai cung luat (cong them kiem tra
    room theo total_room tich luy khi snapshot thieu cap). room_cap
    o lop 1 chi lay tu CHINH snapshot (adapter khong giu state);
    truong bi loai o lop 1 khong bi dem lai o lop 2."""
    cap = snap.get("total_room")
    if cap is not None:
        cap = _gate_value(src, "total_room", cap, None, stats)
    out = {}
    for k, v in snap.items():
        if v is None:
            continue
        if k in QTY_FIELDS:
            v = _gate_value(src, k, v,
                            cap if k == "room" else None, stats)
            if v is None:
                continue
        out[k] = v
    return out


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

def _in_trading_window(dt) -> bool:
    """Khung giao dich T2-T6: 09:00-11:30 + 13:00-15:00 ICT.
    Dung cho watchdog im lang (Dat 4): ngoai khung nay feed SSI
    im lang la binh thuong, khong duoc tinh la ket noi chet."""
    if dt.weekday() >= 5:
        return False
    m = dt.hour * 60 + dt.minute
    return (540 <= m < 690) or (780 <= m < 900)


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
    """MQTT tren WS: tu reconnect + subscribe lai + PINGREQ 25s.

    Tham so Dat 4 (chi SSI dung, cac cong khac giu mac dinh cu):
    - proxy: URL proxy HTTP CONNECT (websockets>=14 tu lam tunnel +
      TLS); None = noi truc tiep nhu truoc.
    - backoff_seq: day giay ngu giua cac lan reconnect, phan tu cuoi
      lap lai; None = luat cu (2s nhan doi, tran 30s).
    - silent_watchdog_sec: >0 thi trong GIO GIAO DICH, qua N giay
      khong nhan tin la coi ket noi chet lang -> dong, noi lai theo
      backoff (su co conn im lang 13:11 22/10)."""

    def __init__(self, url, user, pw, topics, on_msg, subprotocol="mqtt",
                 origin=None, proxy=None, backoff_seq=None,
                 silent_watchdog_sec=0):
        self.url, self.user, self.pw = url, user, pw
        self.topics, self.on_msg = topics, on_msg
        self.subprotocol = subprotocol
        self.origin = origin
        self.proxy = proxy
        self.backoff_seq = backoff_seq
        self.silent_watchdog_sec = silent_watchdog_sec
        self.last_msg = 0.0
        self.msgs = 0
        self.connected = False
        self.last_error = ""
        self._stop = False

    def _backoff_for(self, idx):
        if self.backoff_seq:
            return self.backoff_seq[min(idx, len(self.backoff_seq) - 1)]
        return min(2 * (2 ** idx), 30)

    async def run(self):
        from websockets.asyncio.client import connect
        backoff_idx = 0
        headers = {"User-Agent": "Mozilla/5.0"}
        if self.origin:
            headers["Origin"] = self.origin
        while not self._stop:
            try:
                cid = "tick-" + "".join(
                    random.choices("abcdef0123456789", k=8))
                kw = {}
                if self.proxy:
                    kw["proxy"] = self.proxy
                async with connect(
                        self.url, subprotocols=[self.subprotocol],
                        open_timeout=20, ping_interval=None,
                        additional_headers=headers,
                        max_size=8 * 1024 * 1024, **kw) as ws:
                    await ws.send(_mqtt_connect(cid, self.user, self.pw))
                    msg = await asyncio.wait_for(ws.recv(), 20)
                    if not (isinstance(msg, bytes) and msg[0] >> 4 == 2
                            and msg[3] == 0):
                        raise ConnectionError(f"CONNACK rc={msg!r:.40}")
                    await ws.send(_mqtt_subscribe(1, self.topics))
                    self.connected = True
                    backoff_idx = 0
                    if self.silent_watchdog_sec:
                        # An han watchdog tu luc noi xong (ke ca khi
                        # chua co tin dau tien).
                        self.last_msg = time.time()
                    last_ping = time.time()
                    while not self._stop:
                        if self.silent_watchdog_sec and self.last_msg \
                                and time.time() - self.last_msg \
                                > self.silent_watchdog_sec \
                                and _in_trading_window(now_ict()):
                            raise ConnectionError(
                                "silent watchdog: "
                                f"{round(time.time() - self.last_msg)}s "
                                "khong tin trong gio giao dich")
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
                await asyncio.sleep(self._backoff_for(backoff_idx))
                backoff_idx += 1

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
        # Dat 4 (Chot 09:47 23/10): noi qua proxy dan-cu — URL doc tu
        # Modal secret (SSI_PROXY_URL, ghi 23/10); thieu secret thi
        # noi truc tiep nhu cu (se bi SSI 403 tu Modal, trang thai
        # truoc Dat 4). Backoff rieng 5s->30s->120s + watchdog im
        # lang 90s trong gio giao dich (thiet ke tong the 3.1/3.6;
        # spike phuong an A: proxy= cua websockets>=14, PASS 23/10).
        self.client = MqttWs(
            ep, user, pw, topics, self._on_msg,
            origin="https://fiintrade.ssi.com.vn",
            proxy=os.environ.get("SSI_PROXY_URL") or None,
            backoff_seq=(5, 30, 120), silent_watchdog_sec=90)

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
                    self.agg.on_snapshot(self.name, sym,
                        gate_snap_fields(self.name, {
                        "last": d["matchedPrice"] / 1000,
                        "total_volume": d.get("nmTotalTradedQty"),
                        "total_value": d.get("nmTotalTradedValue"),
                        "buy_volume": d.get("buyForeignQtty"),
                        "buy_value": d.get("buyForeignValue"),
                        "sell_volume": d.get("sellForeignQtty"),
                        "sell_value": d.get("sellForeignValue"),
                        "room": d.get("remainForeignQtty"),
                        }, self.agg.gate_stats))
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
                        self.agg.on_put(
                            self.name, it["stockSymbol"], it,
                            session_epoch(now_ict(),
                                          it.get("time", "")))
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
                self.agg.on_snapshot(self.name, sym,
                    gate_snap_fields(self.name, {
                    "last": self._num(m, "f13"),
                    "total_volume": self._num(m, "f22", 10),
                    "total_value": self._num(m, "f25", 10**9),
                    "buy_volume": self._num(m, "f17", 10),
                    "buy_value": self._num(m, "f58", 10**9),
                    "sell_volume": self._num(m, "f18", 10),
                    "sell_value": self._num(m, "f59", 10**9),
                    "room": self._num(m, "f20", 10),
                    "total_room": self._num(m, "f60", 10),
                    }, self.agg.gate_stats))
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
                self.agg.on_snapshot(self.name, sym,
                    gate_snap_fields(self.name, {
                    "last": cp / 1000 if cp else None,
                    "total_volume": _f(d.get("TT")),
                    "total_value": _f(d.get("TV")),
                    "buy_volume": _f(d.get("FB")),
                    "sell_volume": _f(d.get("FS")),
                    "room": _f(d.get("FR")),
                    "total_room": _f(d.get("FO")),
                    }, self.agg.gate_stats))
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
        self._seen_ticks = OrderedDict()  # dedupe ST (khong co
        # accumulated): khoa dinh danh tick cua nguon nay la
        # (sym, gio, gia, KL, truong phu); LRU 65536 nhu Aggregator
        # de khong phinh vo han giua phien dai.

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
            self._seen_ticks[key] = None
            if len(self._seen_ticks) > 65536:
                self._seen_ticks.popitem(last=False)
            self.agg.on_match(
                self.name, sym, session_epoch(now_ict(), parts[2]),
                price, vol * 10, None)
        elif kind == "SMA" and len(parts) >= 16:
            sym = parts[1]
            tv = _f(parts[8])
            self.agg.on_snapshot(self.name, sym,
                gate_snap_fields(self.name, {
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
                }, self.agg.gate_stats))


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
                self.agg.on_snapshot(self.name, sym,
                    gate_snap_fields(self.name, {
                    "last": last,
                    "total_volume": tvol * 10
                    if tvol is not None else None,
                    }, self.agg.gate_stats))
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
                self.agg.on_snapshot(self.name, d["symbol"],
                    gate_snap_fields(self.name, {
                    "buy_volume": bv * 10 if bv is not None else None,
                    "sell_volume": sv * 10 if sv is not None else None,
                    "buy_value": bval * 1000
                    if bval is not None else None,
                    "sell_value": sval * 1000
                    if sval is not None else None,
                    "room": room * 10 if room is not None else None,
                    }, self.agg.gate_stats))
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
            self.agg.on_snapshot(self.name, sym,
                gate_snap_fields(self.name, {
                "last": d.get("matchPrice"),
                "total_volume": d.get("accumulatedVolume"),
                "total_value": d.get("accumulatedValue"),
                "buy_volume": d.get("foreignBuyVolume"),
                "buy_value": d.get("foreignBuyValue"),
                "sell_volume": d.get("foreignSellVolume"),
                "sell_value": d.get("foreignSellValue"),
                "room": d.get("currentRoom"),
                "total_room": d.get("totalRoom"),
                }, self.agg.gate_stats))
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
        self.candles = {}       # src -> sym -> {minute: [o,h,l,c,v,
                                #  first_ts, last_ts]}
        # Lop volume THOA THUAN theo phut (thiet ke 3.4 — bucket
        # mang 2 lop rieng v / v_pt, khong bao gio cong vao v):
        # src -> sym -> {minute: tong vol thoa thuan}.
        self.candles_pt = {}
        self.foreign = {}       # src -> sym -> snapshot moi nhat
        self.put_foreign = {}   # sym -> cong don thoa thuan (SSI, neu co)
        self.aggregates = {}    # ten event VPS -> payload raw moi nhat
        self.agg_counts = {}    # ten event VPS -> so tin
        # Dedup theo DINH DANH tick (acc cua tung tick) thay cho
        # watermark don dieu (sua parity 22/10 loi 3b): tap acc da
        # thay theo (src, sym), gioi han 65536 muc LRU.
        self._seen_acc = {}     # (src, sym) -> OrderedDict[acc, None]
        self._max_acc = {}      # (src, sym) -> acc lon nhat da thay
        self._max_minute = {}   # (src, sym) -> phut lon nhat da thay
        self._wm = {}           # (src, sym) -> ts su kien lon nhat
                                # (watermark cho chinh sach muon 3.4)
        self._restated = {}     # (src, sym) -> set phut da restate
        # Bo dem do (publish vao status): tick trung bi bo, tick
        # acc lui (that, den muon), tick muon; cong sentinel/range.
        self.tick_stats = {}    # src -> {dup_dropped, acc_regressed,
                                #  late, late_deep, restated_minutes}
        self.gate_stats = {}    # src -> {sentinel, range}
        self.matches_by_src = {}
        self.dirty = set()
        self.dirty_pt = set()   # ma co du lieu thoa thuan moi (Dot 2)
        self.matches = 0
        # Raw store (Dot 2): moi tick di qua on_match/on_put deu ghi
        # 1 dong raw TRUOC khi dedup — drain moi chu ky publish.
        self.raw_buf = []
        # Bang khop lenh chi tiet (tape) — chi chan dnse (chan chinh,
        # day tick nhat). Phia khop M/B SUY THEO LUAT TICK (khong
        # cong nao cua nha mang truong phia that vao ban ghi chuan):
        # gia > tick truoc -> M, < -> B, bang -> giu phia gan nhat,
        # tick dau phien -> khong nhan. sym -> {trades, total, buy,
        # sell, unk, levels, prev, last_side, dirty, started}
        self.tape = {}
        self.tape_started = time.time()

    def tape_rec(self, sym, ts, price, vol):
        t = self.tape.get(sym)
        if t is None:
            t = self.tape[sym] = {
                "trades": [], "total": 0, "buy": 0, "sell": 0,
                "unk": 0, "levels": {}, "prev": None, "last_side": "",
                "dirty": True}
        prev = t["prev"]
        if prev is None:
            side = ""
        elif price > prev:
            side = "M"
        elif price < prev:
            side = "B"
        else:
            side = t["last_side"]
        t["trades"].append((ts, price, vol, side))
        t["total"] += vol
        if side == "M":
            t["buy"] += vol
        elif side == "B":
            t["sell"] += vol
        else:
            t["unk"] += vol
        t["levels"][price] = t["levels"].get(price, 0) + vol
        t["prev"] = price
        if side:
            t["last_side"] = side
        t["dirty"] = True

    def build_tape(self):
        """{sym: tape doc} cho cac ma co tick moi ke tu lan publish
        truoc. Danh sach lenh cat o 1000 lenh gan nhat (tong hop
        tinh tren TOAN BO bang). partial=True neu bo thu khoi dong
        sau 09:05 ICT (bang thieu dau phien do restart)."""
        out = {}
        ict_now = datetime.now(ICT)
        partial = (datetime.fromtimestamp(
            self.tape_started, ICT).strftime("%H:%M") > "09:05")
        day = ict_now.strftime("%Y-%m-%d")
        for sym, t in self.tape.items():
            if not t["dirty"]:
                continue
            t["dirty"] = False
            trades = sorted(t["trades"], key=lambda x: x[0])[-1000:]
            out[sym] = {
                "symbol": sym, "date": day, "source": "dnse",
                "side_method": "tick_rule", "partial": partial,
                "total_vol": t["total"], "buy_vol": t["buy"],
                "sell_vol": t["sell"], "unknown_vol": t["unk"],
                "n_trades": len(t["trades"]),
                "levels": [{"p": p, "v": v} for p, v in
                           sorted(t["levels"].items())],
                "trades": [{
                    "ts": int(ts),
                    "time": datetime.fromtimestamp(ts, ICT).strftime(
                        "%H:%M:%S"),
                    "p": price, "v": vol, "s": side,
                } for ts, price, vol, side in trades],
            }
        return out

    def on_match(self, src, sym, ts, price, vol, acc):
        # Raw (Dot 2): [src, sym, ts_event, ts_recv, price, vol, acc]
        # — ghi truoc dedup de phat lai trung khop phien live.
        self.raw_buf.append([src, sym, ts, round(time.time(), 3),
                             price, vol, acc])
        key = (src, sym)
        st = self.tick_stats.setdefault(
            src, {"dup_dropped": 0, "acc_regressed": 0, "late": 0,
                  "late_deep": 0, "restated_minutes": 0})
        if acc is not None:
            seen = self._seen_acc.setdefault(key, OrderedDict())
            if acc in seen:
                st["dup_dropped"] += 1
                return  # trung lap (reconnect replay) — dinh danh acc
            seen[acc] = None
            if len(seen) > 65536:
                seen.popitem(last=False)
            hi = self._max_acc.get(key)
            if hi is not None and acc < hi:
                # Tick THAT den muon (acc khong don dieu): van xu ly,
                # chi dem de do — watermark cu bo im lang tick nay.
                st["acc_regressed"] += 1
            if hi is None or acc > hi:
                self._max_acc[key] = acc
        minute = int(ts // 60 * 60)
        mk = self._max_minute.get(key)
        if mk is not None and minute < mk:
            st["late"] += 1
        if mk is None or minute > mk:
            self._max_minute[key] = minute
        # Chinh sach muon 120s (thiet ke 3.4): watermark = ts su
        # kien lon nhat da thay cua (src, sym). Tick den sau
        # watermark: lech <= 120s -> van vao dung bucket event-time,
        # phut do bi restate o luot publish ke (dem phut restate);
        # lech > 120s -> van vao bucket, dem rieng late_deep.
        # Khong tick nao bi bo im lang.
        wm = self._wm.get(key)
        if wm is not None and ts < wm:
            if wm - ts > LATE_DEEP_SEC:
                st["late_deep"] += 1
            else:
                rs = self._restated.setdefault(key, set())
                if minute not in rs:
                    rs.add(minute)
                    st["restated_minutes"] += 1
        if wm is None or ts > wm:
            self._wm[key] = ts
        per = self.candles.setdefault(src, {}).setdefault(sym, {})
        b = per.get(minute)
        if b is None:
            per[minute] = [price, price, price, price, vol, ts, ts]
        else:
            b[1] = max(b[1], price)
            b[2] = min(b[2], price)
            if ts >= b[6]:
                # Close = gia cua tick co ts SU KIEN lon nhat trong
                # phut (khong phai tick den sau cung) — sua loi 3d.
                b[3] = price
                b[6] = ts
            if ts < b[5]:
                b[0] = price
                b[5] = ts
            b[4] += vol
        if src == "dnse":
            self.tape_rec(sym, ts, price, vol)
        self.dirty.add(sym)
        self.matches += 1
        self.matches_by_src[src] = self.matches_by_src.get(src, 0) + 1

    def _gate_qty(self, src, key, v, room_cap=None):
        """Cong hop le LOP 2 — tai Aggregator (tuyen phong thu sau
        cong tai adapter): cung luat _gate_value, dem vao gate_stats
        chung; room_cap co the lay tu state tich luy khi snapshot
        thieu total_room."""
        return _gate_value(src, key, v, room_cap, self.gate_stats)

    def on_snapshot(self, src, sym, snap):
        # MOI snapshot cua MOI adapter deu qua cua nay (cong LOP 2 —
        # adapter da ap gate_snap_fields o lop 1 khi phat). Truong so
        # luong hong -> coi nhu thieu (None), khong ghi de, khong
        # thang merge; publish bo truong (missing_policy='na').
        cur = self.foreign.setdefault(src, {}).setdefault(sym, {})
        room_cap = snap.get("total_room")
        if room_cap is None:
            room_cap = cur.get("total_room")
        if room_cap is not None:
            room_cap = self._gate_qty(src, "total_room", room_cap)
        for k, v in snap.items():
            if v is None:
                continue
            if k in QTY_FIELDS:
                v = self._gate_qty(src, k, v, room_cap)
                if v is None:
                    continue
            cur[k] = v
        cur["src"] = src

    def on_put(self, src, sym, it, ts=None):
        # ts = ts su kien cua deal (SSI pe co truong time); thieu
        # thi dung gio nhan. Raw (Dot 2): [src, sym, ts_event,
        # ts_recv, "PUT", {truong rut gon on_put dung}] — phat lai
        # truyen lai ts tu raw nen bucket thoa thuan trung khop.
        ts_ev = ts if ts is not None else time.time()
        self.raw_buf.append([src, sym, ts_ev, round(time.time(), 3),
                             "PUT", {k: it.get(k) for k in (
                                 "vol", "val", "buyForeignQtty",
                                 "buyForeignValue", "sellForeignQtty",
                                 "sellForeignValue")}])
        # v_pt theo phut (thiet ke 3.4): cong vol thoa thuan vao
        # bucket phut cua CHINH nguon nay — lop rieng, merger Dot 3
        # quyet dinh ghep, khong cong vao v o day.
        pvol = it.get("vol") or 0
        if pvol:
            minute = int(ts_ev // 60 * 60)
            per = self.candles_pt.setdefault(src, {}) \
                .setdefault(sym, {})
            per[minute] = per.get(minute, 0) + pvol
            self.dirty_pt.add(sym)
        cur = self.put_foreign.setdefault(sym, {
            "put_buy_volume": 0, "put_buy_value": 0,
            "put_sell_volume": 0, "put_sell_value": 0, "put_deals": 0,
            "put_total_volume": 0, "put_total_value": 0})
        cur["put_buy_volume"] += it.get("buyForeignQtty") or 0
        cur["put_buy_value"] += it.get("buyForeignValue") or 0
        cur["put_sell_volume"] += it.get("sellForeignQtty") or 0
        cur["put_sell_value"] += it.get("sellForeignValue") or 0
        # Tong thoa thuan (ke ca noi dia): ung vien dinh danh loi 1
        # va do phan dinh loi 3a.
        cur["put_total_volume"] += it.get("vol") or 0
        cur["put_total_value"] += it.get("val") or 0
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

    def snapshot_vs_ticks(self):
        """Truc doi chieu MOI (sua loi 1, mau ExactPro): snapshot
        total_volume cua TUNG cong vs tong tick tu gom cua CHINH
        cong do. Chay ngay trong phien (moi luot publish) — truong
        snapshot sai ngu nghia lo ra o day thay vi cho parity cuoi
        ngay. worst: 5 ma lech lon nhat kem ca 2 gia tri de dinh
        danh ngu nghia truong."""
        out = {}
        for src, syms in self.foreign.items():
            stat = {"compared": 0, "exact": 0, "within10": 0,
                    "max_abs_diff": 0, "worst": []}
            worst = []
            for sym, snap in syms.items():
                tv = snap.get("total_volume")
                if tv is None:
                    continue
                tick_sum = sum(
                    b[4] for b in self.candles.get(
                        src, {}).get(sym, {}).values())
                if not tick_sum:
                    continue
                stat["compared"] += 1
                d = abs(tv - tick_sum)
                if d == 0:
                    stat["exact"] += 1
                if d <= 10:
                    stat["within10"] += 1
                stat["max_abs_diff"] = max(stat["max_abs_diff"], d)
                worst.append({"sym": sym, "snapshot": tv,
                              "tick_sum": tick_sum,
                              "diff": tv - tick_sum})
            worst.sort(key=lambda w: -abs(w["diff"]))
            stat["worst"] = worst[:5]
            out[src] = stat
        return out

    def build_diag(self):
        """Du lieu DO dong phien (chi publish o luot closed=True):
        snapshot theo TUNG nguon (chua hop nhat) + tong tick theo
        nguon + volume theo phut theo nguon + tong thoa thuan —
        dau vao cho dinh danh truong loi 1 va phan dinh loi 3 o
        buoc reconcile ngoai tuyen."""
        mv = {}
        for src, syms in self.candles.items():
            for sym, per in syms.items():
                slot = mv.setdefault(sym, {})
                for minute, b in per.items():
                    k = datetime.fromtimestamp(minute, ICT).strftime(
                        "%Y-%m-%dT%H:%M")
                    slot.setdefault(k, {})[src] = b[4]
        mp = {}
        for src, syms in self.candles_pt.items():
            for sym, per in syms.items():
                slot = mp.setdefault(sym, {})
                for minute, vpt in per.items():
                    k = datetime.fromtimestamp(minute, ICT).strftime(
                        "%Y-%m-%dT%H:%M")
                    slot.setdefault(k, {})[src] = vpt
        return {
            "foreign_by_src": self.foreign,
            "tick_totals": {
                src: {sym: sum(b[4] for b in per.values())
                      for sym, per in syms.items()}
                for src, syms in self.candles.items()},
            "minute_vol_by_src": mv,
            "minute_put_by_src": mp,
            "put_totals": self.put_foreign,
        }

    def checkpoint(self):
        """Checkpoint state dedup + bo dem (thiet ke 3.5/5 — state
        khong song trong tien trinh): ghi vao status moi chu ky
        publish. Gom max_acc/max_minute/watermark theo (src, sym),
        duoi tap acc da thay (64 acc gan nhat moi khoa — du hap thu
        replay khi noi lai/restart), tap phut restate, tick_stats/
        gate_stats va tong matches. NEN + snapshot khoi ngoai khong
        nam o day: resume day du (dung lai ca nen tu artifact) la
        pham vi Dot 2."""
        def k2(k):
            return k[0] + "|" + k[1]
        return {
            "max_acc": {k2(k): v for k, v in self._max_acc.items()},
            "max_minute": {k2(k): v
                           for k, v in self._max_minute.items()},
            "wm": {k2(k): v for k, v in self._wm.items()},
            "seen_tail": {k2(k): list(od.keys())[-64:]
                          for k, od in self._seen_acc.items()},
            "restated": {k2(k): sorted(s)
                         for k, s in self._restated.items()},
            "tick_stats": self.tick_stats,
            "gate_stats": self.gate_stats,
            "matches": self.matches,
            "matches_by_src": self.matches_by_src,
        }

    def restore_checkpoint(self, cp):
        """Nap lai state dedup + bo dem tu checkpoint() (dung khi
        khoi dong lai giua phien — duong doc artifact ve la Dot 2).
        Chi khoi phuc dedup/dem; nen va snapshot duoc dung lai tu
        artifact cung Dot 2."""
        if not cp:
            return

        def k1(s):
            src, _, sym = s.partition("|")
            return (src, sym)
        self._max_acc = {k1(k): v
                         for k, v in (cp.get("max_acc") or {}).items()}
        self._max_minute = {
            k1(k): v for k, v in (cp.get("max_minute") or {}).items()}
        self._wm = {k1(k): v for k, v in (cp.get("wm") or {}).items()}
        self._seen_acc = {}
        for k, tail in (cp.get("seen_tail") or {}).items():
            od = OrderedDict()
            for a in tail:
                od[a] = None
            self._seen_acc[k1(k)] = od
        self._restated = {k1(k): set(v) for k, v in
                          (cp.get("restated") or {}).items()}
        self.tick_stats = cp.get("tick_stats") or {}
        self.gate_stats = cp.get("gate_stats") or {}
        self.matches = cp.get("matches") or 0
        self.matches_by_src = cp.get("matches_by_src") or {}

    def drain_raw(self):
        """Lay toan bo raw da ghi tu lan drain truoc (Dot 2)."""
        buf, self.raw_buf = self.raw_buf, []
        return buf

    def export_src_state(self):
        """Trang thai theo nguon -> artifact (Dot 2): buckets dang
        noi bo day du [o,h,l,c,v,fts,lts] theo khoa phut ISO kem
        snapshot khoi ngoai theo nguon. Copy nong tung muc de an
        toan khi publish chay o thread rieng."""
        out = {}
        for src, syms in self.candles.items():
            bk = {}
            for sym, per in syms.items():
                bk[sym] = {
                    datetime.fromtimestamp(m, ICT).strftime(
                        "%Y-%m-%dT%H:%M"): list(b)
                    for m, b in per.items()}
            out[src] = {"buckets": bk}
        for src, syms in self.candles_pt.items():
            pt = {}
            for sym, per in syms.items():
                pt[sym] = {
                    datetime.fromtimestamp(m, ICT).strftime(
                        "%Y-%m-%dT%H:%M"): v
                    for m, v in per.items()}
            out.setdefault(src, {})["buckets_pt"] = pt
        for src, syms in self.foreign.items():
            out.setdefault(src, {})["snapshot"] = {
                s: dict(v) for s, v in syms.items()}
        return out

    def import_src_state(self, state, only_day=None):
        """Nap artifact theo nguon (resume Dot 2): buckets ->
        candles, buckets_pt -> candles_pt, snapshot -> foreign.
        Khoa phut ISO -> epoch giay ICT. only_day='YYYY-MM-DD':
        chi nap phut cua ngay do (artifact buckets.json tich luy
        nhieu phien — khong loc se keo nen phien cu vao phien
        moi). Loi o 1 nguon khong chan cac nguon con lai: moi
        nguon/muc duoc boc try rieng, bucket sai dinh dang bi bo."""
        for src, part in (state or {}).items():
            try:
                bk = (part or {}).get("buckets") or {}
                dst = self.candles.setdefault(src, {})
                for sym, per in bk.items():
                    d2 = dst.setdefault(sym, {})
                    for k, b in per.items():
                        if only_day and not k.startswith(only_day):
                            continue
                        if not isinstance(b, (list, tuple)) \
                                or len(b) != 7:
                            continue
                        try:
                            dt = datetime.strptime(
                                k, "%Y-%m-%dT%H:%M")
                        except (ValueError, TypeError):
                            continue
                        epoch = int(dt.replace(tzinfo=ICT)
                                    .timestamp())
                        d2[epoch] = list(b)
                pt = (part or {}).get("buckets_pt") or {}
                dstp = self.candles_pt.setdefault(src, {})
                for sym, per in pt.items():
                    dp = dstp.setdefault(sym, {})
                    for k, v in per.items():
                        if only_day and not k.startswith(only_day):
                            continue
                        if not isinstance(v, (int, float)):
                            continue
                        try:
                            dt = datetime.strptime(
                                k, "%Y-%m-%dT%H:%M")
                        except (ValueError, TypeError):
                            continue
                        epoch = int(dt.replace(tzinfo=ICT)
                                    .timestamp())
                        dp[epoch] = v
                snap = (part or {}).get("snapshot") or {}
                if snap:
                    self.foreign[src] = {s: dict(v)
                                         for s, v in snap.items()}
            except Exception:  # noqa: BLE001 — 1 nguon hong khong
                continue           # chan resume cac nguon con lai


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


def fetch_tick_state(key=None):
    """Doc artifact shadow_tick tu Worker (route noi bo, secret) —
    phuc vu resume Dot 2. key=None -> status.json; key dang
    'src/<src>/buckets.json' tuong doi duoi market/shadow_tick/.
    Loi -> raise (noi goi tu quyet dinh bat dau trang)."""
    secret = os.environ["STOCK_PUBLISH" "_SECRET"]
    url = "https://lethanhdong.one/api/internal/tick-state"
    if key:
        url += "?key=" + urllib.parse.quote(key, safe="")
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-Publish-Secret", secret)
    req.add_header("X-Internal-Secret", secret)
    req.add_header("User-Agent", "Mozilla/5.0")
    with urllib.request.urlopen(req, timeout=30) as r:
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
    if CONFIG.get("resume"):
        # Resume Dot 2: nap checkpoint + artifact theo nguon cua
        # CHINH phien hom nay; bat ky loi nao -> bat dau trang.
        try:
            st = await asyncio.to_thread(fetch_tick_state)
            if st and st.get("session_date") == \
                    start_dt.strftime("%Y-%m-%d") \
                    and st.get("checkpoint"):
                agg.restore_checkpoint(st["checkpoint"])
                state = {}
                for name in CONFIG["sources"]:
                    bk = await asyncio.to_thread(
                        fetch_tick_state, f"src/{name}/buckets.json")
                    sn = await asyncio.to_thread(
                        fetch_tick_state, f"src/{name}/snapshot.json")
                    state[name] = {
                        "buckets": (bk or {}).get("buckets") or {},
                        "buckets_pt": (bk or {}).get(
                            "buckets_pt") or {},
                        "snapshot": (sn or {}).get("by_symbol") or {}}
                agg.import_src_state(
                    state, only_day=start_dt.strftime("%Y-%m-%d"))
                print(f"[tick-multi] resume OK: matches={agg.matches} "
                      f"nen={ {s: len(v) for s, v in agg.candles.items()} }",
                      flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"[tick-multi] resume loi (bat dau trang): "
                  f"{str(e)[:120]}", flush=True)
    adapters = {}
    tasks = {}
    for name in CONFIG["sources"]:
        ad = ADAPTERS[name](agg, universe)
        adapters[name] = ad
        tasks[name] = asyncio.create_task(ad.run())
    last_publish = 0.0
    publishes = 0
    _prev_msgs = {}
    _prev_t = [time.time()]

    def build_health(name):
        """Suc khoe theo chan (artifact Dot 2, muc 3.1 cua thiet
        ke): tach 3 so — lan noi, nhip tin/phut, lan dut + loi."""
        ad = adapters[name]
        now_t = time.time()
        prev = _prev_msgs.get(name)
        rate = None
        if prev is not None and now_t > _prev_t[0]:
            rate = round((ad.msgs - prev)
                         / (now_t - _prev_t[0]) * 60, 1)
        return {
            "connected": bool(ad.connected),
            "msgs": ad.msgs,
            "msgs_per_min": rate,
            "matches": agg.matches_by_src.get(name, 0),
            "last_msg_ago_sec": round(now_t - ad.last_msg)
            if ad.last_msg else None,
            "last_error": ad.last_error or None,
            "symbols_foreign": len(agg.foreign.get(name, {})),
        }
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
        status = {
            "session_date": start_dt.strftime("%Y-%m-%d"),
            "mode": "multi",
            "live": CONFIG["live"],
            "sources": per_src,
            "matches_aggregated": agg.matches,
            "symbols_with_candles": len({
                s for src in agg.candles for s in agg.candles[src]}),
            "crosscheck": agg.crosscheck(),
            "snapshot_vs_ticks": agg.snapshot_vs_ticks(),
            "tick_stats": agg.tick_stats,
            "gate_stats": agg.gate_stats,
            "checkpoint": agg.checkpoint(),
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
        if closed:
            status["diag"] = agg.build_diag()
        return status

    async def do_publish(closed=False):
        nonlocal publishes
        status = build_status(closed)
        # --- Dot 2: raw chunks + artifact theo nguon. Doc dirty
        # TRUOC build_intraday/clear. Raw drain o day: neu POST loi
        # thi mat toi da 1 chu ky raw (cung nguong kill-test). ---
        raw_recs = agg.drain_raw()
        raw_grouped = {}
        for rec in raw_recs:
            ts_ev = rec[2] if rec[2] is not None else rec[3]
            hour = datetime.fromtimestamp(ts_ev, ICT).strftime("%H")
            raw_grouped.setdefault(rec[0], {}).setdefault(
                hour, []).append(rec)
        raw_payload = {}
        for src_r, by_hour in raw_grouped.items():
            raw_payload[src_r] = {}
            for hour, recs in by_hour.items():
                lines = "\n".join(
                    json.dumps(r, separators=(",", ":"))
                    for r in recs)
                raw_payload[src_r][hour] = base64.b64encode(
                    gzip.compress(lines.encode())).decode()
        src_state = agg.export_src_state()
        artifacts = {}
        for name in CONFIG["sources"]:
            part = src_state.get(name) or {}
            bk = part.get("buckets") or {}
            bkpt = part.get("buckets_pt") or {}
            artifacts[name] = {
                "snapshot": {"by_symbol": part.get("snapshot") or {}},
                "health": build_health(name),
                "buckets_dirty": {s: bk[s] for s in agg.dirty
                                  if s in bk},
                "buckets_pt_dirty": {s: bkpt[s]
                                     for s in agg.dirty_pt
                                     if s in bkpt},
            }
        for name in CONFIG["sources"]:
            _prev_msgs[name] = adapters[name].msgs
        _prev_t[0] = time.time()
        intra = agg.build_intraday(sorted(agg.dirty))
        foreign = agg.build_foreign()
        flow = agg.build_foreign_flow()
        keys = list(intra.keys())
        chunks = [keys[i:i + 10]
                  for i in range(0, len(keys), 10)] or [[]]
        for ci, ckeys in enumerate(chunks):
            chunk = {k: intra[k] for k in ckeys}
            if ci == 0:
                p = {"tick_shadow": {"status": status,
                                     "foreign": foreign,
                                     "raw_chunks": raw_payload,
                                     "src_artifacts": artifacts}}
                if chunk:
                    p["tick_shadow"]["intraday"] = chunk
                await asyncio.to_thread(publish, p, "multi")
                if CONFIG["live"]:
                    live = {"tick_live": {"foreign": flow}}
                    if chunk:
                        live["tick_live"]["intraday"] = chunk
                    await asyncio.to_thread(publish, live, "multi")
            elif chunk:
                await asyncio.to_thread(
                    publish, {"tick_shadow": {"intraday": chunk}},
                    "multi")
                if CONFIG["live"]:
                    await asyncio.to_thread(
                        publish, {"tick_live": {"intraday": chunk}},
                        "multi")
        # Bang khop lenh chi tiet (tape): publish kem chu ky nay,
        # loi thi chu ky sau gui lai (build_tape chi danh dau dirty
        # lai khi co tick moi — neu POST loi o day, lan publish ke
        # tiep se khong co dirty cho ma khong phat sinh tick moi;
        # chap nhan duoc vi tape la du lieu hien thi, khong phai
        # artifact kiem toan).
        tape = agg.build_tape()
        if tape:
            await asyncio.to_thread(publish, {"tape": tape}, "multi")
        # Publish xong moi xoa dirty: POST loi giua chung -> dirty
        # con nguyen, chu ky sau gui lai (Worker merge idempotent,
        # intraday ghi de theo ma). Xoa truoc POST se lam mat
        # artifact cua ma ngung giao dich sau chu ky loi — vuot
        # nguong mat <= 1 chu ky cua kill-test.
        agg.dirty.clear()
        agg.dirty_pt.clear()
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
