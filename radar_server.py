#!/usr/bin/env python3
import json
import os
import re
import threading
import time
import urllib.request
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import websocket

PORT = int(os.environ.get("RADAR_PORT", "8765"))
STATIC_DIR = os.environ.get("RADAR_STATIC_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "radar"))

ADSBLOL_LAT = float(os.environ.get("ADSBLOL_LAT", "42.36"))
ADSBLOL_LON = float(os.environ.get("ADSBLOL_LON", "-71.02"))
ADSBLOL_RADIUS_NM = float(os.environ.get("ADSBLOL_RADIUS_NM", "35"))

_route_cache = {}
ROUTE_TTL = 3600
MY_LOCATION = {
    "lat": float(os.environ.get("MY_LOCATION_LAT", "42.33967")),
    "lon": float(os.environ.get("MY_LOCATION_LON", "-71.05591")),
    "label": os.environ.get("MY_LOCATION_LABEL", "55 W 5th St, South Boston"),
}

EARTHCAM_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
EARTHCAM_FEEDS = {
    "chicago": {
        "page_url": "https://www.earthcam.com/usa/illinois/chicago/skydeck/",
        "flv_name": "22820.flv",
    },
    "london": {
        "page_url": "https://www.earthcam.com/world/england/london/abbeyroad/",
        "flv_name": "AbbeyRoadHD1.flv",
    },
}
_earthcam_tokens = {name: {"t": "", "td": "", "fetched": 0} for name in EARTHCAM_FEEDS}
EARTHCAM_TOKEN_TTL = 240


def _earthcam_headers(feed):
    return {
        "Referer": EARTHCAM_FEEDS[feed]["page_url"],
        "Origin": "https://www.earthcam.com",
        "User-Agent": EARTHCAM_USER_AGENT,
    }


def _refresh_earthcam_token(feed):
    cfg = EARTHCAM_FEEDS[feed]
    token = _earthcam_tokens[feed]
    now = time.time()
    if token["t"] and now - token["fetched"] < EARTHCAM_TOKEN_TTL:
        return
    raw = fetch_json(cfg["page_url"], headers=_earthcam_headers(feed)).decode("utf-8", "ignore")
    flv_escaped = re.escape(cfg["flv_name"]).replace("/", r"\\/")
    pattern = r'"stream":"https?:\\/\\/videos-3\.earthcam\.com\\/fecnetwork\\/' + flv_escaped + r'\\/playlist\.m3u8\?t=([^"&\\]+)&td=([^"&\\]+)"'
    m = re.search(pattern, raw)
    if not m:
        raise RuntimeError(f"could not find {feed} stream token")
    token["t"] = m.group(1)
    token["td"] = m.group(2)
    token["fetched"] = now


_stopwatch_lock = threading.Lock()
_stopwatch = {"running": False, "started_at": None, "accumulated": 0.0}


def _stopwatch_state():
    with _stopwatch_lock:
        elapsed = _stopwatch["accumulated"]
        if _stopwatch["running"]:
            elapsed += time.time() - _stopwatch["started_at"]
        return {"running": _stopwatch["running"], "elapsed": elapsed}


AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY", "")
AIS_BBOX = [[42.25, -71.10], [42.42, -70.85]]  # Boston Harbor + approaches
VESSEL_STALE_SECONDS = 900

_vessels = {}
_vessels_lock = threading.Lock()


def _ais_worker():
    if not AISSTREAM_API_KEY:
        print("AISSTREAM_API_KEY not set; skipping vessel tracking")
        return
    while True:
        try:
            ws = websocket.create_connection("wss://stream.aisstream.io/v0/stream", timeout=30)
            sub = {
                "APIKey": AISSTREAM_API_KEY,
                "BoundingBoxes": [AIS_BBOX],
                "FilterMessageTypes": ["PositionReport"],
            }
            ws.send(json.dumps(sub))
            while True:
                raw = ws.recv()
                if not raw:
                    break
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if msg.get("MessageType") != "PositionReport":
                    continue
                meta = msg.get("MetaData", {})
                pr = msg.get("Message", {}).get("PositionReport", {})
                mmsi = meta.get("MMSI")
                if mmsi is None:
                    continue
                lat = pr.get("Latitude", meta.get("latitude"))
                lon = pr.get("Longitude", meta.get("longitude"))
                if lat is None or lon is None:
                    continue
                entry = {
                    "mmsi": mmsi,
                    "name": (meta.get("ShipName") or "").strip() or None,
                    "lat": lat,
                    "lon": lon,
                    "cog": pr.get("Cog"),
                    "heading": pr.get("TrueHeading"),
                    "sog": pr.get("Sog"),
                    "updated": time.time(),
                }
                with _vessels_lock:
                    _vessels[mmsi] = entry
        except Exception:
            time.sleep(5)
            continue


threading.Thread(target=_ais_worker, daemon=True).start()

def fetch_json(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "boston-radar/1.0"})
    with urllib.request.urlopen(req, timeout=8) as resp:
        return resp.read()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/states":
            try:
                url = f"https://api.adsb.lol/v2/point/{ADSBLOL_LAT}/{ADSBLOL_LON}/{ADSBLOL_RADIUS_NM}"
                raw = fetch_json(url)
                data = json.loads(raw)
                states = []
                for ac in data.get("ac", []):
                    lat = ac.get("lat")
                    lon = ac.get("lon")
                    if lat is None or lon is None:
                        continue
                    icao24 = ac.get("hex", "")
                    callsign = (ac.get("flight") or "").strip()
                    alt_baro = ac.get("alt_baro")
                    on_ground = alt_baro == "ground"
                    altitude = None if on_ground else alt_baro
                    heading = ac.get("track") or 0
                    velocity = ac.get("gs")
                    states.append([icao24, callsign, None, None, None, lon, lat,
                                    altitude, on_ground, velocity, heading, None, None, altitude])
                self._send(200, json.dumps({"states": states}).encode())
            except Exception as e:
                self._send(502, json.dumps({"error": str(e)}).encode())
            return

        if parsed.path == "/route":
            params = urllib.parse.parse_qs(parsed.query)
            callsign = (params.get("callsign", [""])[0] or "").strip()
            if not callsign:
                self._send(400, json.dumps({"error": "missing callsign"}).encode())
                return
            now = time.time()
            cached = _route_cache.get(callsign)
            if cached and now - cached[0] < ROUTE_TTL:
                self._send(200, cached[1])
                return
            try:
                data = fetch_json(f"https://api.adsbdb.com/v0/callsign/{urllib.parse.quote(callsign)}")
                _route_cache[callsign] = (now, data)
                self._send(200, data)
            except Exception as e:
                self._send(502, json.dumps({"error": str(e)}).encode())
            return

        if parsed.path == "/vessels":
            now = time.time()
            with _vessels_lock:
                fresh = [v for v in _vessels.values() if now - v["updated"] < VESSEL_STALE_SECONDS]
            self._send(200, json.dumps({"vessels": fresh}).encode())
            return

        if parsed.path == "/mylocation":
            self._send(200, json.dumps(MY_LOCATION).encode())
            return

        if parsed.path == "/stopwatch/state":
            self._send(200, json.dumps(_stopwatch_state()).encode())
            return

        earthcam_match = re.fullmatch(r"/(chicago|london)/hls/([A-Za-z0-9_.]+)", parsed.path)
        if earthcam_match:
            feed, filename = earthcam_match.group(1), earthcam_match.group(2)
            try:
                _refresh_earthcam_token(feed)
                token = _earthcam_tokens[feed]
                flv_name = EARTHCAM_FEEDS[feed]["flv_name"]
                name = "playlist.m3u8" if filename == "master.m3u8" else filename
                url = f"https://videos-3.earthcam.com/fecnetwork/{flv_name}/{name}?t={token['t']}&td={token['td']}"
                body = fetch_json(url, headers=_earthcam_headers(feed))
                ctype = "video/mp2t" if filename.endswith(".ts") else "application/vnd.apple.mpegurl"
                self._send(200, body, ctype)
            except Exception as e:
                self._send(502, str(e).encode(), "text/plain")
            return

        # static file serving
        path = parsed.path
        if path == "/":
            path = "/radar.html"
        fs_path = STATIC_DIR + path
        try:
            with open(fs_path, "rb") as f:
                body = f.read()
            ctype = "text/html"
            if fs_path.endswith(".js"):
                ctype = "application/javascript"
            elif fs_path.endswith(".css"):
                ctype = "text/css"
            self._send(200, body, ctype)
        except FileNotFoundError:
            self._send(404, b"not found", "text/plain")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        now = time.time()
        if parsed.path == "/stopwatch/start":
            with _stopwatch_lock:
                if not _stopwatch["running"]:
                    _stopwatch["running"] = True
                    _stopwatch["started_at"] = now
        elif parsed.path == "/stopwatch/stop":
            with _stopwatch_lock:
                if _stopwatch["running"]:
                    _stopwatch["accumulated"] += now - _stopwatch["started_at"]
                    _stopwatch["running"] = False
                    _stopwatch["started_at"] = None
        elif parsed.path == "/stopwatch/toggle":
            with _stopwatch_lock:
                if _stopwatch["running"]:
                    _stopwatch["accumulated"] += now - _stopwatch["started_at"]
                    _stopwatch["running"] = False
                    _stopwatch["started_at"] = None
                else:
                    _stopwatch["running"] = True
                    _stopwatch["started_at"] = now
        elif parsed.path == "/stopwatch/reset":
            with _stopwatch_lock:
                _stopwatch["running"] = False
                _stopwatch["started_at"] = None
                _stopwatch["accumulated"] = 0.0
        else:
            self._send(404, b"not found", "text/plain")
            return
        self._send(200, json.dumps(_stopwatch_state()).encode())

if __name__ == "__main__":
    host = os.environ.get("RADAR_HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, PORT), Handler)
    print(f"serving on {host}:{PORT}")
    server.serve_forever()
