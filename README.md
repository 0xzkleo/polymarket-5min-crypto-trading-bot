# Polymarket Trading Bot

**What is your native language?**
[🇨🇳 中文](README.zh.md) · [🇷🇺 Русский](README.ru.md)

---
<img width="1983" height="793" alt="thumbnail" src="https://github.com/user-attachments/assets/c73980d1-51e9-4d77-8200-cac77b8f9c7e" />
---

<p align="center">
  <strong>⭐ Want more profitable trading bots?</strong><br><br>
  Built by <a href="https://github.com/gamma-trade-lab"><strong>Gamma Trade Lab</strong></a> — high-performance automated trading systems for Polymarket.<br><br>
  <a href="https://github.com/gamma-trade-lab"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-gamma--trade--lab-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="mailto:gammatradeorg@gmail.com"><img alt="Email" src="https://img.shields.io/badge/Email-gammatradeorg@gmail.com-EA4335?logo=gmail&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>
</p>

---
## How do this bot work on polymarket?

https://github.com/user-attachments/assets/cd194cab-0566-4f3a-8b88-f1c1bb152cb4

---

---

## Proof of work

<img width="1919" height="1035" alt="1" src="https://github.com/user-attachments/assets/27f00a58-db8d-4992-a1ed-1b5ee741bede" />

<img width="1919" height="1032" alt="2" src="https://github.com/user-attachments/assets/447c9671-3f47-4bde-a4be-744af27bdbb1" />

<img width="1916" height="1008" alt="4" src="https://github.com/user-attachments/assets/8b88610b-c54b-4e3d-b7a6-2ccef7b72ca4" />

<img width="1823" height="942" alt="3" src="https://github.com/user-attachments/assets/f7052333-8107-40d8-9703-d1bbd2b77bc7" />

---

## Core Idea

Prediction markets for short-horizon BTC moves are noisy and fast. This project treats them like a **systematic trading problem**: pull in market and context data, normalize it through a single ingestion path, fuse multiple detectors into a decision, then execute through a broker adapter with **hard risk limits** (small size per trade, take profit parameters). The goal is not "one magic signal" but a **testable stack** you can run in simulation, observe in Grafana, and only then point at live capital.

---

## Features

- **Seven-phase pipeline** — External feeds → ingestion → Nautilus core → signal processors and fusion → execution and risk → monitoring → feedback / learning hooks.
- **Multi-signal stack** — Spike detection, sentiment-style inputs, divergence logic, order-book and momentum-style processors, plus fusion to combine votes.
- **Risk-first defaults** — Configurable caps (e.g. ~$1 per trade), take profit, entry-price band, spread filter, direction lock, and anti-chase guard.
- **Stop-loss toggle** — `ENABLE_STOP_LOSS=false` lets positions ride to take-profit or settlement; flip to `true` to re-enable the early-exit SL.
- **ML edge gate** — Only bets when the XGBoost model's probability is at least `MIN_ML_EDGE` (default 10 pp) away from Polymarket's price.
- **One bet per market** — `MAX_TRADES_PER_MARKET=1` fires a single entry per 15-min slot and moves on.
- **Simulation and live** — Run paper / test modes without touching production keys; switch to live only when ready.
- **Operational tooling** — Redis-based mode hints, Grafana-friendly metrics, paper trade inspection, auto-restart wrapper for long runs.
- **Self-learning hook** — Weights can be adjusted from performance feedback (see `feedback/` and strategy configuration).
- **Resilience** — WebSocket handling, rate limiting, validation, and patches around Polymarket + Nautilus edge cases (Gamma loading, market-order sizing, Windows `prometheus_client` guard).

---

## Prerequisites

- **Python 3.14+**
- **Redis** — used for mode switching and related control-plane behavior
- **Polymarket account** with API credentials for live trading
- **Git**

---

## Quick Start

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

Edit `.env` with your credentials and parameters:

```env
POLYMARKET_PK=your_private_key_here
POLYMARKET_API_KEY=your_api_key_here
POLYMARKET_API_SECRET=your_api_secret_here
POLYMARKET_PASSPHRASE=your_passphrase_here

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=2

ENABLE_STOP_LOSS=false
TAKE_PROFIT_PCT=0.40
MIN_ENTRY_PRICE=0.25
MAX_ENTRY_PRICE=0.75
MAX_TRADES_PER_MARKET=1
MIN_ML_EDGE=0.10
```

### 5. Start Redis

```bash
redis-server
```

On macOS with Homebrew: `brew install redis && redis-server`.
On Debian/Ubuntu: `sudo apt install redis-server && redis-server`.

### 6. Run the bot

```bash
# Fast test loop (simulated trades ~every minute)
python main.py --test-mode

# Normal simulation (15-min clock)
python main.py --simulation

# Live trading (real money — requires valid credentials)
python supervisor.py --live
```

---

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ENABLE_STOP_LOSS` | Enable early stop-loss exit | `false` |
| `STOP_LOSS_PCT` | Capital fraction lost at SL (only when SL enabled) | `0.50` |
| `TAKE_PROFIT_PCT` | Fraction of remaining upside to take | `0.40` |
| `MIN_ENTRY_PRICE` | Minimum token price to enter | `0.25` |
| `MAX_ENTRY_PRICE` | Maximum token price to enter | `0.75` |
| `MAX_SPREAD_PCT` | Max bid-ask spread relative to mid | `0.05` |
| `ENTRY_COOLDOWN_SEC` | Seconds between entry attempts | `90` |
| `MAX_TRADES_PER_MARKET` | Max entries per 15-min market | `1` |
| `LOCK_MARKET_DIRECTION` | Lock direction after first trade on a market | `true` |
| `MAX_CHASE_DELTA` | Max price delta allowed for re-entry | `0.12` |
| `MIN_ML_EDGE` | Min ML probability gap required to bet | `0.10` |
| `LATE_ENTRY_CUTOFF_SEC` | Refuse entries this close to settlement | `120` |
| `MARKET_BUY_USD` | USD per order | `1.00` |

See `.env.example` for the full list with inline comments.

---

## Running the Bot

- **Unified entrypoint**: `main.py` supports `--test-mode`, `--simulation`, and `--live`.
- **Auto-restart wrapper**: `supervisor.py` runs `main.py` in a loop for unattended operation.
- **Paper trades**: After simulation runs, inspect history with:

```bash
python scripts/view_trades.py
```

---

## Monitoring

- Metrics exporters and helpers live under `monitoring/`.
- Grafana dashboard assets live under `grafana/` (import with `grafana/import_dashboard.py`).

Wire these to your own Prometheus/Grafana stack as needed.

---

## Trading Modes

Mode switching via Redis is supported for toggling simulation vs live without restarting; see `scripts/redis_control.py`.

---

## Testing Individual Phases

Run the numbered checks **in order** after each previous phase succeeds.

| Phase | Focus | Command |
|-------|-------|---------|
| 1 | Data sources (exchanges, news) | `python scripts/test_data_sources.py test` |
| 2 | Ingestion (adapter, websockets, validation) | `python scripts/test_ingestion.py test` |
| 3 | Nautilus core (instruments, engine, events) | `python scripts/test_nautilus.py test` |
| 4 | Strategy brain (processors, fusion) | `python scripts/test_strategy.py test` |
| 5 | Execution (risk, client, engine) | `python scripts/test_execution.py test` |

Debug the Gamma API directly:

```bash
python scripts/debug_gamma_api.py
```

---

## How Much Money Do I Need to Start?

The reference configuration uses **~$1 per fill**. You still need enough balance to cover fees, spread, and a string of losses. Many operators keep **$10–$50** for early experiments; scale only after simulation matches expectations. **This is not financial advice.**

---

## Is This Profitable?

There is **no guarantee** of profit. Short-horizon markets have fees, spread, adverse selection, and outages. Simulation results **do not** reliably predict live performance. Use paper mode and small size first; treat every run as an experiment.

---

## Best For

- **Traders who want speed and automation** for 15-minute crypto prediction markets.
- **Developers** comfortable editing `.env`, reading logs, and running phase tests.
- **People who treat risk as primary** and want explicit caps and observability before scaling.

---

## Contributing and Ideas

Contributions are welcome via the usual GitHub flow (fork, branch, pull request).

**Ideas for contributions:**
- Add derivatives context (funding, open interest) as additional processors.
- New signal processors or fusion rules.
- Telegram or Discord alerts for fills and errors.
- A small web UI for config and status.
- Extend beyond BTC to ETH, SOL, and other Polymarket short-horizon products.
- Stronger ML / calibration layers with honest evaluation and paper-trading gates.

---

## License

MIT License. See the repository's `LICENSE` file.

---

## Disclaimer

Trading cryptocurrencies and prediction-market instruments involves **substantial risk of loss**. This software is provided for **education and research**. Past performance does not guarantee future results. The authors are **not** responsible for any financial losses. Start in simulation, use small size, and only trade with capital you can afford to lose entirely.

---

## Acknowledgments

- [NautilusTrader](https://nautilustrader.io/) — Trading framework
- [Polymarket](https://polymarket.com) — Prediction market venue
