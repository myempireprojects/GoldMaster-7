# ============================================================
# XAUUSD PA BOT — LOCAL WEBSOCKET CANDLE SERVER
# ============================================================
# Runs the internal candle engine and streams clean tick data
# to the HTML frontend over a local WebSocket on port 8765.
#
# Architecture:
#   Python (this file) ──ws://localhost:8765──> index.html
#
# Messages sent to the browser:
#   {"type":"tick",    "mid":X, "bid":X, "ask":X, "epoch":X}
#   {"type":"bar",     "tf":60|300|900, "bar":{epoch,O,H,L,C}}
#   {"type":"forming", "tf":60|300|900, "bar":{epoch,O,H,L,C}}
#
# INSTALL:
#   pip install twelvedata websockets
#
# RUN:
#   python server.py
#
# Then open index.html in your browser — it will auto-detect
# this server and use it instead of the 10-second TD polling.
# ============================================================

import asyncio
import json
import time

from twelvedata import TDClient
import websockets

# ── CONFIG ──────────────────────────────────────────────────
API_KEY       = "YOUR_API_KEY"   # ← paste your Twelve Data key here
SYMBOL        = "XAU/USD"
PORT          = 8765
POLL_INTERVAL = 1                # seconds between price polls
SPREAD        = 0.20             # simulated bid/ask spread in points
# ────────────────────────────────────────────────────────────

td      = TDClient(apikey=API_KEY)
clients = set()                  # connected WebSocket clients


# ── CANDLE CLASS ─────────────────────────────────────────────

class Candle:
    """Represents a single OHLC bar building in real-time."""

    def __init__(self, price: float, epoch: int):
        self.open  = price
        self.high  = price
        self.low   = price
        self.close = price
        self.epoch = epoch

    def update(self, price: float):
        self.close = price
        if price > self.high: self.high = price
        if price < self.low:  self.low  = price

    def to_dict(self) -> dict:
        return {
            "epoch": self.epoch,
            "open":  round(self.open,  2),
            "high":  round(self.high,  2),
            "low":   round(self.low,   2),
            "close": round(self.close, 2),
        }


# ── FORMING BARS (one per timeframe) ─────────────────────────

forming: dict[int, Candle | None] = {60: None, 300: None, 900: None}


def bucket_epoch(epoch: int, gran: int) -> int:
    """Round epoch down to the nearest candle boundary."""
    return (epoch // gran) * gran


# ── BROADCAST ────────────────────────────────────────────────

async def broadcast(msg: dict):
    """Send a JSON message to every connected client."""
    if not clients:
        return
    data = json.dumps(msg)
    await asyncio.gather(
        *[ws.send(data) for ws in clients],
        return_exceptions=True,
    )


# ── PRICE POLLING + CANDLE ENGINE ────────────────────────────

async def candle_engine():
    """
    Core loop: poll Twelve Data quote every POLL_INTERVAL seconds,
    update all timeframe forming bars, and broadcast to clients.
    """
    print(f"[engine] Starting — polling {SYMBOL} every {POLL_INTERVAL}s")
    last_error_log = 0

    while True:
        try:
            quote = td.quote(symbol=SYMBOL).as_json()

            # Build mid price with simulated spread
            price = float(quote["close"])
            bid   = price
            ask   = price + SPREAD
            mid   = (bid + ask) / 2
            epoch = int(time.time())

            # ── Broadcast tick to all connected browsers ──────
            await broadcast({
                "type":  "tick",
                "mid":   round(mid,  2),
                "bid":   round(bid,  2),
                "ask":   round(ask,  2),
                "epoch": epoch,
            })

            # ── Update forming bars for M1 / M5 / M15 ────────
            for gran in (60, 300, 900):
                bucket = bucket_epoch(epoch, gran)

                if forming[gran] is None or forming[gran].epoch != bucket:
                    # ── Close the previous bar ─────────────────
                    if forming[gran] is not None:
                        closed = forming[gran]
                        print(
                            f"[M{gran//60}] CLOSED  "
                            f"O:{closed.open:.2f} H:{closed.high:.2f} "
                            f"L:{closed.low:.2f}  C:{closed.close:.2f}"
                        )
                        await broadcast({
                            "type": "bar",
                            "tf":   gran,
                            "bar":  closed.to_dict(),
                        })

                    # ── Open a new forming bar ──────────────────
                    forming[gran] = Candle(mid, bucket)
                    print(f"[M{gran//60}] NEW BAR  @ {mid:.2f}  epoch={bucket}")

                else:
                    # ── Update current forming bar ──────────────
                    forming[gran].update(mid)

                # Always broadcast the current state of the forming bar
                await broadcast({
                    "type": "forming",
                    "tf":   gran,
                    "bar":  forming[gran].to_dict(),
                })

            # Console heartbeat
            m1 = forming[60]
            if m1:
                ts = time.strftime('%H:%M:%S', time.gmtime(epoch))
                print(
                    f"\r[{ts}]  MID:{mid:.2f}  "
                    f"O:{m1.open:.2f} H:{m1.high:.2f} "
                    f"L:{m1.low:.2f}  C:{m1.close:.2f}  "
                    f"clients:{len(clients)}",
                    end="", flush=True,
                )

        except Exception as exc:
            now = time.time()
            if now - last_error_log > 10:  # suppress repeat spam
                print(f"\n[engine] ERROR: {exc}")
                last_error_log = now

        await asyncio.sleep(POLL_INTERVAL)


# ── WEBSOCKET HANDLER ─────────────────────────────────────────

async def handler(ws):
    """Handle a single WebSocket client connection."""
    clients.add(ws)
    addr = ws.remote_address
    print(f"\n[ws] Client connected from {addr}  (total: {len(clients)})")

    # Send a hello message so the browser knows the server version
    await ws.send(json.dumps({
        "type":    "hello",
        "version": "1.0",
        "symbol":  SYMBOL,
        "message": "Connected to local XAUUSD candle server",
    }))

    try:
        await ws.wait_closed()
    finally:
        clients.discard(ws)
        print(f"\n[ws] Client disconnected from {addr}  (total: {len(clients)})")


# ── ENTRY POINT ───────────────────────────────────────────────

async def main():
    print("=" * 60)
    print("  XAUUSD PA BOT — LOCAL CANDLE SERVER")
    print(f"  Listening on  ws://localhost:{PORT}")
    print(f"  Symbol        {SYMBOL}")
    print(f"  Poll interval {POLL_INTERVAL}s")
    print("=" * 60)

    if API_KEY == "YOUR_API_KEY":
        print("\n⚠  WARNING: API_KEY is not set!")
        print("   Edit the API_KEY variable at the top of server.py\n")

    # Run WebSocket server and candle engine concurrently
    async with websockets.serve(handler, "localhost", PORT):
        await candle_engine()


if __name__ == "__main__":
    asyncio.run(main())
