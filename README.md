# Polymarket Trading Bot

A production-grade algorithmic trading bot for **Polymarket’s 15-minute BTC up/down markets**. It combines multiple real-time signal sources, risk limits, monitoring, and optional learning hooks on a seven-phase pipeline.


---

> ### ⭐ Want more profitable trading bots?
>
> This bot is built and maintained by **[Gamma Trade Lab](https://github.com/gamma-trade-lab)** — a lab dedicated to building high-performance automated trading systems for polymarket.
>
> | | |
> |---|---|
> | 📩 **Gmail** | [gammatradeorg@gmail.com](mailto:gammatradeorg@gmail.com) |
> | 📞 **Telegram** | [t.me/RetroValix](https://t.me/RetroValix) |
>
> *Star the repo · Follow for new bots · Reach out for custom builds*

---

## Table of contents

- [Core idea](#core-idea)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Running the bot](#running-the-bot)
- [Monitoring](#monitoring)
- [Trading modes](#trading-modes)
- [Testing individual phases](#testing-individual-phases)
- [How much money do I need to start?](#how-much-money-do-i-need-to-start)
- [Is this profitable?](#is-this-profitable)
- [Best for](#best-for)
- [Contributing and ideas](#contributing-and-ideas)
- [License](#license)
- [Disclaimer](#disclaimer)

---

## Core idea

Prediction markets for short-horizon BTC moves are noisy and fast. This project treats them like a **systematic trading problem**: pull in market and context data, normalize it through a single ingestion path, fuse multiple detectors into a decision, then execute through a broker adapter with **hard risk limits** (small size per trade, stop loss / take profit parameters). The goal is not “one magic signal” but a **testable stack** you can run in simulation, observe in Grafana, and only then point at live capital.

---

## Features

- **Seven-phase pipeline** — External feeds → ingestion → Nautilus core → signal processors and fusion → execution and risk → monitoring → feedback / learning hooks.
- **Multi-signal stack** — Spike detection, sentiment-style inputs, divergence logic, order-book and momentum-style processors, plus fusion to combine votes.
- **Risk-first defaults** — Configurable caps (for example ~\$1 per trade in the reference setup), stop loss, take profit, and exposure-minded execution.
- **Simulation and live** — Run paper / test modes without touching production keys; switch toward live only when you intend to.
- **Operational tooling** — Redis-based mode hints, Grafana-friendly metrics, paper trade inspection, auto-restart wrapper for long runs.
- **Self-learning hook** — Weights can be adjusted from performance feedback (see `feedback/` and strategy configuration).
- **Resilience** — WebSocket handling, rate limiting, validation, and patches around Polymarket + Nautilus edge cases (Gamma loading, market-order sizing).
- **Phase tests** — Each major layer has a script you can run on its own (see below).

---

## Prerequisites

- **Python 3.14+**
- **Redis** — Used for mode switching and related control-plane behavior
- **Polymarket account** with API credentials for live trading
- **Git**

---

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/polymarket-btc-15m-bot.git
cd polymarket-btc-15m-bot
```

### 2. Create a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials and parameters, for example:

```env
# Polymarket API
POLYMARKET_PK=your_private_key_here
POLYMARKET_API_KEY=your_api_key_here
POLYMARKET_API_SECRET=your_api_secret_here
POLYMARKET_PASSPHRASE=your_passphrase_here

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=2

# Trading parameters (adjust to your risk tolerance)
MAX_POSITION_SIZE=1.0
STOP_LOSS_PCT=0.30
TAKE_PROFIT_PCT=0.20
SPIKE_THRESHOLD=0.15
DIVERGENCE_THRESHOLD=0.05
```

### 5. Start Redis

```bash
redis-server
```

On macOS with Homebrew: `brew install redis` then `redis-server`. On Debian/Ubuntu: `sudo apt install redis-server` then `redis-server`.

### 6. Run the bot

```bash
# Fast test loop (simulated trades about every minute)
python main.py --test-mode

# Normal simulation (15-min clock)
python main.py --simulation

# Live trading (real money — requires working credentials)
python supervisor.py --live
```

---

## Configuration

| Argument | Description | Default |
|----------|-------------|---------|
| `--test-mode` | Faster cadence for testing | `False` |
| `--live` | Enable live trading | `False` |
| `--no-grafana` | Disable Grafana-oriented metrics export | `False` |

See `main.py` and `supervisor.py` for the full set of flags.

---

## Running the bot

- **Unified entrypoint**: `main.py` supports `--test-mode`, `--simulation`, and `--live` (see its module docstring).
- **Auto-restart wrapper**: `supervisor.py` runs `main.py` in a loop for unattended operation.
- **Paper trades**: After simulation runs, inspect history with:

```bash
python scripts/view_trades.py
```

---

## Monitoring

- Metrics exporters and helpers live under `monitoring/`.
- Grafana dashboard assets live under `grafana/` (import with `grafana/import_dashboard.py` if you use Grafana).

Wire these to your own Prometheus/Grafana stack as needed.

---

## Trading modes

Mode switching via Redis is supported for toggling simulation vs live without always editing code; see `scripts/redis_control.py`. Treat Redis-driven mode changes as **experimental** until you validate them in your environment.

---

## Testing individual phases

Run the numbered checks **in order** after each previous phase succeeds. Each script forwards arguments to the underlying Typer CLI (use `--help` on any script for options).

| Phase | Focus | Command |
|-------|--------|---------|
| 1 | Data sources (exchanges, news, etc.) | `python scripts/test_data_sources.py test` |
| 2 | Ingestion (adapter, websockets, validation, limits) | `python scripts/test_ingestion.py test` |
| 3 | Nautilus core (instruments, engine, events) | `python scripts/test_nautilus.py test` |
| 4 | Strategy brain (processors, fusion, strategy) | `python scripts/test_strategy.py test` |
| 5 | Execution (risk, client, engine) | `python scripts/test_execution.py test` |

Optional: query the Gamma HTTP API directly for debugging:

```bash
python scripts/debug_gamma_api.py
```

---

## How much money do I need to start?

The reference configuration uses **very small notional per trade** (on the order of **\$1** per fill in typical setups). You still need enough balance on Polymarket to place orders, absorb fees/spread, and handle a string of losses. Many operators keep **on the order of \$10–\$50** for early experiments; scale only after simulation matches your expectations. **This is not financial advice.**

---

## Is this profitable?

There is **no guarantee** of profit. Short-horizon markets have fees, spread, adverse selection, and outages. Simulation or backtest results **do not** reliably predict live performance. Use paper and small size first; treat every run as an experiment.

---

## Best for

- **Traders who want speed and automation** for 15-minute crypto prediction markets rather than manual clicking.
- **Developers** who are comfortable editing `.env`, reading logs, and running phase tests when something breaks.
- **People who treat risk as primary** and want explicit caps and observability before scaling size.

---

## Contributing and ideas

Contributions are welcome through the usual GitHub flow (fork, branch, pull request).

**Ideas for contributions**

- Add derivatives context (funding, open interest) as additional processors.
- New signal processors or fusion rules.
- Telegram or Discord alerts for fills and errors.
- A small web UI for config and status.
- Extend beyond BTC to other Polymarket short-horizon products (ETH, SOL, etc.).
- Stronger ML or calibration layers with honest evaluation and paper trading gates.

---

## License

This project is licensed under the MIT License. See the repository’s `LICENSE` file if present.

---

## Disclaimer

Trading cryptocurrencies and prediction-market instruments involves **substantial risk of loss**. This software is provided for **education and research**. Past performance does not guarantee future results. The authors are **not** responsible for any financial losses. Start in simulation, use small size, and only trade with capital you can afford to lose entirely.

---

## Acknowledgments

- [NautilusTrader](https://nautilustrader.io/) — Trading framework
- [Polymarket](https://polymarket.com) — Prediction market venue

Thanks to everyone who reports issues and improves the stack.
