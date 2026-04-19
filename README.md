# pm-dashboard

Real-time dashboard for `pminspect` trade events over Redis pub/sub.

## Prerequisites

- Python 3.12+
- `pminspect` running and publishing to Redis at `redis://localhost:6379/0`

## Setup

```bash
cd pm-dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
REDIS_URL=redis://localhost:6379/0 CHANNEL=trades.raw uvicorn server:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 in your browser.

## Optional demo mode

```bash
DEMO=1 uvicorn server:app --host 0.0.0.0 --port 8000
```

## Environment variables

- `REDIS_URL` — Redis connection URL
- `CHANNEL` — Redis pub/sub channel
- `DEMO` — set to `1` to use fake trades

