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
REDIS_URL=redis://localhost:6379/0 CHANNEL=trades.raw uvicorn server:app --port 8008
```

Open http://localhost:8008 in your browser.

## Environment variables

- `REDIS_URL` — Redis connection URL
- `CHANNEL` — Redis pub/sub channel

## TODO

- Dockerize `pm-dashboard`
- Load previous trades from the Postgres trade store
