# Polymarket 5-Min Trading Bot

[🇨🇳 中文](README.zh.md) · [🇷🇺 Русский](README.ru.md)

## Overview

This is an automated trading bot for Polymarket Bitcoin Up or Down 5-minute markets. It runs on NautilusTrader, fuses multiple market microstructure signals, and gates entries with an XGBoost edge model vs Polymarket price.

---

https://github.com/user-attachments/assets/797ddc35-c440-440b-ae63-4362b0e6c428

<p align="center">
  Built by <a href="https://x.com/RetroValix"><strong>Retro Valix</strong></a><br><br>
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>&nbsp;
  <a href="https://x.com/RetroValix"><img alt="X" src="https://img.shields.io/badge/X-@RetroValix-000000?logo=x&logoColor=white"></a>&nbsp;
  <a href="https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154"><img alt="Medium" src="https://img.shields.io/badge/Medium-Guide-000000?logo=medium&logoColor=white"></a>
</p>

---

## Public vs Premium

This repo is the **public version** — open source so you can test the stack, risk logic, and PnL tracking yourself.  
Premium is for traders who want production performance after validating the public bot.

| | Public (this repo) | Premium |
|---|---|---|
| Purpose | Test · learn · verify | Live capital · best results |
| Risk & PnL | Included partially | Included fully + tuned |
| Training data | General / warming model | **200,000+** trades |
| Win rate | Not claimed (educational) | **98.8%+** |
| Proof | You run sim / live yourself | Shared in private meeting |
| Access | Only BTC | BTC, ETH, SOL, DOGE, XRP, BNB |

**Flow:** run public → validate results → [talk collaboration](https://t.me/RetroValix)

Polymarket account proof is shared only on a call — not posted publicly.

→ **Collaboration / proof:** [Telegram @RetroValix](https://t.me/RetroValix)

---

## What it trades

Polymarket **Bitcoin Up or Down** 5-minute markets:

- Slug pattern: `btc-updown-5m-{unix_start}`
- Window length: **300 seconds** (UTC floor: `(ts // 300) * 300`)
- Default entry window: seconds **180–270** of each market (late-window style)

---

## References

| Source | Link |
|--------|------|
| Medium — build guide | [Polymarket BTC AI Trading Bot with NautilusTrader](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| X (Twitter) | [@RetroValix](https://x.com/RetroValix) |
| Telegram | [@RetroValix](https://t.me/RetroValix) |

---

## Features

- Multi-signal fusion + ML edge gate tuned for **5-min** BTC Up/Down markets
- Risk controls (size caps, TP/SL, spread filter, anti-chase, one bet per window)
- Simulation / live modes + terminal UI dashboard
- Paper trade logs & Grafana metrics

---

## Quick Start

**Requires:** Python 3.14+ · Redis · Polymarket API keys (for live)

```bash
git clone https://github.com/0xRetroVaIix/polymarket-5min-crypto-trading-bot.git
cd polymarket-5min-crypto-trading-bot

python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # add your keys
```

```bash
python main.py --test-mode      # fast paper loop
python main.py --simulation     # 5-min paper
python supervisor.py --live     # real money
```

Inspect paper trades:

```bash
python scripts/view_trades.py
```

---

## Config (essentials)

| Parameter | Default | Notes |
|-----------|---------|--------|
| `MARKET_BUY_USD` | `1.00` | USD per order |
| `ENABLE_STOP_LOSS` | `false` | Early SL exit |
| `TAKE_PROFIT_PCT` | `0.40` | Take profit fraction |
| `MIN_ENTRY_PRICE` / `MAX_ENTRY_PRICE` | `0.25` / `0.75` | Entry band |
| `TRADE_WINDOW_SEC_START` / `END` | `180` / `270` | Entry window inside each 5-min market |
| `ENTRY_COOLDOWN_SEC` | `30` | Min seconds between entries |
| `MAX_TRADES_PER_MARKET` | `1` | One bet per 5-min window |
| `MIN_ML_EDGE` | `0.10` | Min ML vs market gap |

Full list: [`.env.example`](.env.example)

---

## Links

| | |
|---|---|
| Start here | [Quick Start](#quick-start) |
| Public vs Premium | [Table](#public-vs-premium) |
| Medium guide | [Article](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| Contact | [Telegram](https://t.me/RetroValix) · [X](https://x.com/RetroValix) |
| Phase tests | `python scripts/test_data_sources.py test` → … → `test_execution.py` |

---

## Disclaimer

Trading involves **substantial risk of loss**. This software is for **education and research**. No profit is guaranteed. Past performance ≠ future results. Start in simulation; only use capital you can afford to lose.

---

## License

MIT — see [`LICENSE`](LICENSE)

---

<div align="center">
  <a href="https://t.me/RetroValix">
    <img width="85" height="85" alt="Retro Valix" src="https://github.com/user-attachments/assets/66c994bf-c618-40e7-a0f4-d295e09d1e91" /><br>
    <span>Retro Valix</span>
  </a>
</div>
