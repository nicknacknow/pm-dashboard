# pm-dashboard

Real-time trade visualization dashboard that streams from your Redis pub/sub channel.

## Features

- **Markets board** — aggregated view per token with buy/sell pressure bars, sortable by volume, trade count, buy%, or recency
- **Live feed** — individual trades stream in with age, side badge, USDC amount, token count, price, and wallet
- **Filters** — BUY/SELL/ALL toggle, min USDC threshold, token ID search, wallet search
- **Token drill-down** — click any market or token ID in the feed to filter the feed to that token
- **Whale alerts** — floating toasts for trades ≥ $500 USDC
- **Pause/resume** — freeze the feed to read without losing trades
- **Replay buffer** — new connections instantly see the last 60 trades
- **Stats bar** — trades/min, total volume, buy pressure %, average trade size, active markets
- **Demo mode** — generate fake trades without a Redis instance

## Quick start

```bash
cd pm-dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
REDIS_URL=redis://localhost:6379/0 CHANNEL=trades.raw uvicorn server:app --host 0.0.0.0 --port 8000
```

Then open http://localhost:8000

If you come back later, reactivate the environment with:

```bash
source .venv/bin/activate
```

### Optional demo mode

If you want fake trades without Redis, run:

```bash
DEMO=1 uvicorn server:app --host 0.0.0.0 --reload --port 8000
```

## Environment variables

| Variable    | Default                    | Description                        |
|-------------|----------------------------|------------------------------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL               |
| `CHANNEL`   | `trades.raw`               | Pub/sub channel name               |
| `DEMO`      | `0`                        | Set to `1` for fake trade mode     |

## Architecture

```
Redis pub/sub
    │
    ▼
server.py  ←──── formats trades (same logic as pminspect)
    │
    ▼ WebSocket /ws
index.html
    ├── Markets board  (aggregated by token_id, left panel)
    ├── Live feed      (individual trades, right panel, max 250 DOM rows)
    └── Whale alerts   (floating toasts, ≥ $500 USDC)
```

## Performance notes

- DOM is capped at 250 trade rows; oldest are evicted automatically
- Trade rendering is batched via `requestAnimationFrame` to handle bursts
- Token board updates are throttled to every 400ms
- Up to 2,000 trades are kept in memory for filter rebuilds
- Age labels are updated once per second via a shared interval (not per-row timers)
