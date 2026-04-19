"""
WebSocket server that bridges Redis trade events to browser clients.
Serves the dashboard and streams live trades via WebSocket.

Usage:
    uvicorn server:app --reload --port 8000
    DEMO=1 uvicorn server:app --reload --port 8000   # fake trades, no Redis needed
"""

import asyncio
import json
import os
import random
import string
import time
from asyncio import Queue
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CHANNEL = os.getenv("CHANNEL", "trades.raw")
DEMO = os.getenv("DEMO", "0") == "1"
REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", "5"))
REPLAY_BUFFER_SIZE = 60
BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = BASE_DIR / "index.html"

# websocket → its outbound queue
connected_clients: dict[WebSocket, Queue] = {}
replay_buffer: deque = deque(maxlen=REPLAY_BUFFER_SIZE)


# ---------------------------------------------------------------------------
# Trade formatting
# ---------------------------------------------------------------------------

def format_trade(trade: dict) -> dict:
    side = int(trade["side"])
    maker_amount = int(trade["maker_amount"])
    taker_amount = int(trade["taker_amount"])

    is_buy = side == 0
    if is_buy:
        usdc = maker_amount / 1_000_000
        tokens = taker_amount / 1_000_000
    else:
        usdc = taker_amount / 1_000_000
        tokens = maker_amount / 1_000_000

    price = usdc / tokens if tokens > 0 else 0.0
    return {
        "wallet": str(trade["wallet"]),
        "token_id": str(trade["token_id"]),
        "side": "BUY" if is_buy else "SELL",
        "tokens": tokens,
        "price": price,
        "total_usdc": usdc,
        "tx_hash": str(trade["transaction_hash"]),
        "block_number": int(trade["block_number"]),
        "timestamp": str(trade["timestamp"]),
        "server_ts": time.time(),
    }


# ---------------------------------------------------------------------------
# Broadcast: just enqueue — never sends directly, no race conditions
# ---------------------------------------------------------------------------

async def broadcast(msg: str) -> None:
    dead = []
    print(f"[broadcast] {len(connected_clients)} client(s) connected")
    for ws, q in connected_clients.items():
        try:
            await q.put(msg)
        except Exception as e:
            print(f"[broadcast] queue error: {e}")
            dead.append(ws)
    for ws in dead:
        connected_clients.pop(ws, None)


# ---------------------------------------------------------------------------
# Dedicated sender task — one per WebSocket, serialises all sends
# ---------------------------------------------------------------------------

async def sender(websocket: WebSocket, queue: Queue) -> None:
    while True:
        msg = await queue.get()
        try:
            await websocket.send_text(msg)
        except Exception as e:
            print(f"[sender] send failed, closing: {e}")
            break


# ---------------------------------------------------------------------------
# Redis listener
# ---------------------------------------------------------------------------

async def redis_listener() -> None:
    while True:
        client = redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        pubsub = client.pubsub()
        try:
            await pubsub.subscribe(CHANNEL)
            print(f"[redis] subscribed to '{CHANNEL}' @ {REDIS_URL}")

            async for message in pubsub.listen():
                print(f"[redis] raw type={message.get('type')} data={str(message.get('data',''))[:200]}")

                if message.get("type") != "message":
                    continue
                raw_data = message.get("data")
                if not isinstance(raw_data, str):
                    continue

                try:
                    payload = json.loads(raw_data)

                    # Handle both {"trade": {...}} and bare trade objects
                    trade_data = payload.get("trade") or payload
                    formatted = format_trade(trade_data)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
                    print(f"[redis] parse error: {e} | raw: {raw_data[:200]}")
                    continue

                msg = json.dumps(formatted)
                replay_buffer.append(msg)
                await broadcast(msg)

        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            print(f"[redis] connection lost ({exc}); retrying in {REDIS_RETRY_DELAY}s")
            await asyncio.sleep(REDIS_RETRY_DELAY)
        finally:
            try:
                await pubsub.unsubscribe(CHANNEL)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass
            try:
                await client.aclose()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Demo mode
# ---------------------------------------------------------------------------

_DEMO_TOKENS = [
    "21742633143463906290569050155826241533067272736897614950488156847949938836455",
    "52114319501245915516055106046884209969926127482827954674443846427813813222426",
    "71321045679252212594626385532706912750332728571942532289631379312455583992563",
    "88141047817977302126408973596697753695645015848954809082095777632820088264671",
    "10000000000000000000000000000000000000000000000000000000000000000000000000001",
    "10000000000000000000000000000000000000000000000000000000000000000000000000002",
    "10000000000000000000000000000000000000000000000000000000000000000000000000003",
]


def _rand_hex(n: int) -> str:
    return "0x" + "".join(random.choices(string.hexdigits[:16], k=n))


def _make_demo_trade() -> dict:
    is_buy = random.random() > 0.42
    usdc_raw = max(0.01, random.lognormvariate(3.5, 1.8))
    price = random.uniform(0.05, 0.97)
    tokens = usdc_raw / price
    usdc_micro = int(usdc_raw * 1_000_000)
    tokens_micro = int(tokens * 1_000_000)

    if is_buy:
        maker, taker = usdc_micro, tokens_micro
    else:
        maker, taker = tokens_micro, usdc_micro

    return {
        "wallet": _rand_hex(40),
        "token_id": random.choice(_DEMO_TOKENS),
        "side": "0" if is_buy else "1",
        "maker_amount": str(maker),
        "taker_amount": str(taker),
        "transaction_hash": _rand_hex(64),
        "block_number": str(random.randint(60_000_000, 70_000_000)),
        "timestamp": str(int(time.time())),
    }


async def demo_listener() -> None:
    print("[demo] generating fake trades — no Redis needed")
    while True:
        batch = random.randint(1, 8)
        for _ in range(batch):
            raw = _make_demo_trade()
            try:
                formatted = format_trade(raw)
            except Exception:
                continue
            msg = json.dumps(formatted)
            replay_buffer.append(msg)
            await broadcast(msg)
        await asyncio.sleep(random.uniform(0.2, 0.6))


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    listener = demo_listener if DEMO else redis_listener
    task = asyncio.create_task(listener())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()

    queue: Queue = Queue()
    connected_clients[websocket] = queue

    # Seed replay buffer into the queue first
    for msg in list(replay_buffer):
        await queue.put(msg)

    # One dedicated sender task per client — no concurrent send() calls
    send_task = asyncio.create_task(sender(websocket, queue))

    try:
        while True:
            await websocket.receive_text()  # keepalive
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] receive error: {e}")
    finally:
        connected_clients.pop(websocket, None)
        send_task.cancel()
        print(f"[ws] client disconnected, {len(connected_clients)} remaining")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(INDEX_HTML)
