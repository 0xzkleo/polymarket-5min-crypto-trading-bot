# Polymarket BTC 15-Min Trading Bot

[🇨🇳 中文](README.zh.md) · [🇷🇺 Русский](README.ru.md)

---

<img width="1981" height="793" alt="thumbnail" src="https://github.com/user-attachments/assets/31efdf63-1172-46b2-8713-e1173dc06722" />

<p align="center">
  Built by <a href="https://github.com/RetroVaIix"><strong>Retro Valix</strong></a><br><br>
  <a href="https://github.com/RetroVaIix"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-RetroVaIix-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>&nbsp;
  <a href="https://x.com/RetroValix"><img alt="X" src="https://img.shields.io/badge/X-@RetroValix-000000?logo=x&logoColor=white"></a>&nbsp;
  <a href="https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154"><img alt="Medium" src="https://img.shields.io/badge/Medium-Guide-000000?logo=medium&logoColor=white"></a>
</p>

---

## Demo

https://github.com/user-attachments/assets/8f9a2b66-e291-44e6-8e6f-edecf65a7f4d

---

## Public vs Premium

This repo is the **public version** — open source so you can test the stack, risk logic, and PnL tracking yourself.  
Premium is for traders who want production performance after validating the public bot.

| | Public (this repo) | Premium |
|---|---|---|
| Purpose | Test · learn · verify | Live capital · best results |
| Risk & PnL | Included | Included + tuned |
| Training data | General / warming model | **20,000+** trades |
| Win rate | Not claimed (educational) | **97%+** |
| Proof | You run sim / live yourself | Shared in private meeting |
| Access | Full source here | Private build + support |

**Flow:** run public → validate results → [talk premium](https://t.me/RetroValix)

Polymarket account proof is shared only on a call — not posted publicly.

→ **Premium / proof:** [Telegram @RetroValix](https://t.me/RetroValix)

---

## References

| Source | Link |
|--------|------|
| Medium — build guide | [Polymarket BTC 15-Minute AI Trading Bot with NautilusTrader](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| X (Twitter) | [@RetroValix](https://x.com/RetroValix) |
| Telegram | [@RetroValix](https://t.me/RetroValix) |
| GitHub | [RetroVaIix](https://github.com/RetroVaIix) |

---

## Features

- Multi-signal fusion + ML edge gate
- Risk controls (size caps, TP/SL, spread filter, anti-chase)
- Simulation / live modes + terminal UI dashboard
- Paper trade logs & Grafana metrics

---

## Quick Start

**Requires:** Python 3.14+ · Redis · Polymarket API keys (for live)

```bash
git clone https://github.com/yourusername/polymarket-btc-15m-bot.git
cd polymarket-btc-15m-bot

python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # add your keys
redis-server
```

```bash
python main.py --test-mode      # fast paper loop
python main.py --simulation     # 15-min paper
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
| `MAX_TRADES_PER_MARKET` | `1` | One bet per window |
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
