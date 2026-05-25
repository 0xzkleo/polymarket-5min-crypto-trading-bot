# Polymarket Trading Bot

**Language / 语言 / Язык:**
[English](#english) · [中文](#中文) · [Русский](#русский)

---

<a name="english"></a>
# 🇬🇧 English

A production-grade algorithmic trading bot for **Polymarket's 15-minute BTC up/down markets**. It combines multiple real-time signal sources, risk limits, monitoring, and optional learning hooks on a seven-phase pipeline.

---

<p align="center">
  <strong>⭐ Want more profitable trading bots?</strong><br><br>
  Built by <a href="https://github.com/gamma-trade-lab"><strong>Gamma Trade Lab</strong></a> — high-performance automated trading systems for Polymarket.<br><br>
  <a href="https://github.com/gamma-trade-lab"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-gamma--trade--lab-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="mailto:gammatradeorg@gmail.com"><img alt="Email" src="https://img.shields.io/badge/Email-gammatradeorg@gmail.com-EA4335?logo=gmail&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>
</p>

---

## Table of Contents

- [Core idea](#core-idea)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Running the bot](#running-the-bot)
- [Monitoring](#monitoring)
- [Trading modes](#trading-modes)
- [Testing individual phases](#testing-individual-phases)
- [How much money do I need?](#how-much-money-do-i-need-to-start)
- [Is this profitable?](#is-this-profitable)
- [Best for](#best-for)
- [Contributing](#contributing-and-ideas)
- [License](#license)
- [Disclaimer](#disclaimer)

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

---
---

<a name="中文"></a>
# 🇨🇳 中文

**Polymarket 15 分钟 BTC 涨跌自动交易机器人**。它将多个实时信号源、风控限制、监控以及可选的机器学习钩子整合到一个七阶段流水线中。

---

<p align="center">
  <strong>⭐ 想要更多盈利的交易机器人？</strong><br><br>
  由 <a href="https://github.com/gamma-trade-lab"><strong>Gamma Trade Lab</strong></a> 打造 — 面向 Polymarket 的高性能自动化交易系统。<br><br>
  <a href="https://github.com/gamma-trade-lab"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-gamma--trade--lab-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="mailto:gammatradeorg@gmail.com"><img alt="Email" src="https://img.shields.io/badge/Email-gammatradeorg@gmail.com-EA4335?logo=gmail&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>
</p>

---

## 目录

- [核心理念](#核心理念)
- [功能特性](#功能特性)
- [前置条件](#前置条件)
- [快速开始](#快速开始)
- [参数配置](#参数配置)
- [运行机器人](#运行机器人)
- [监控](#监控)
- [交易模式](#交易模式)
- [分阶段测试](#分阶段测试)
- [需要多少本金？](#需要多少本金)
- [是否盈利？](#是否盈利)
- [适合人群](#适合人群)
- [贡献与想法](#贡献与想法)
- [许可证](#许可证)
- [免责声明](#免责声明)

---

## 核心理念

BTC 短周期预测市场噪音大、节奏快。本项目将其视为**系统化交易问题**：拉取市场与上下文数据，通过统一的摄入路径进行规范化，将多个检测器融合成决策，再通过经纪适配器以**硬性风控限制**（每笔交易小仓位、止盈参数）执行。目标不是"一个神奇信号"，而是一个**可测试的技术栈**：先在模拟环境运行、在 Grafana 中观察，再将真实资金指向它。

---

## 功能特性

- **七阶段流水线** — 外部数据源 → 摄入 → Nautilus 核心 → 信号处理器与融合 → 执行与风控 → 监控 → 反馈/学习钩子。
- **多信号栈** — 尖峰检测、情绪类输入、背离逻辑、订单簿与动量处理器，加上融合引擎合并投票。
- **风险优先默认值** — 可配置的仓位上限（例如每笔 ~$1）、止盈、入场价格区间、价差过滤、方向锁定、反追涨保护。
- **止损开关** — `ENABLE_STOP_LOSS=false` 让仓位持有到止盈或市场结算；改为 `true` 可重新启用提前止损。
- **ML 边际门控** — 仅当 XGBoost 模型预测概率与 Polymarket 价格相差至少 `MIN_ML_EDGE`（默认 10 个百分点）时才下注。
- **每市场一次下注** — `MAX_TRADES_PER_MARKET=1` 在每个 15 分钟槽内只开一次仓。
- **模拟与实盘** — 无需触碰生产密钥即可运行纸交易/测试模式；准备好后再切换到实盘。
- **运维工具** — 基于 Redis 的模式切换、Grafana 友好的指标、纸交易记录查看、长时运行自动重启。
- **自学习钩子** — 权重可根据绩效反馈调整（见 `feedback/` 目录）。
- **鲁棒性** — WebSocket 处理、限速、校验，以及针对 Polymarket + Nautilus 边界情况的补丁（Gamma 加载、市价单大小、Windows `prometheus_client` 保护）。

---

## 前置条件

- **Python 3.14+**
- **Redis** — 用于模式切换和控制平面行为
- **Polymarket 账户** — 实盘交易需要 API 凭证
- **Git**

---

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/yourusername/polymarket-btc-15m-bot.git
cd polymarket-btc-15m-bot
```

### 2. 创建虚拟环境

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 配置环境变量

```bash
cp .env.example .env
```

编辑 `.env`，填入你的凭证和参数：

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

### 5. 启动 Redis

```bash
redis-server
```

macOS（Homebrew）：`brew install redis && redis-server`。
Debian/Ubuntu：`sudo apt install redis-server && redis-server`。

### 6. 运行机器人

```bash
# 快速测试循环（模拟交易，约每分钟一次）
python main.py --test-mode

# 普通模拟（15 分钟时钟）
python main.py --simulation

# 实盘交易（真实资金 — 需要有效凭证）
python supervisor.py --live
```

---

## 参数配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `ENABLE_STOP_LOSS` | 启用提前止损 | `false` |
| `STOP_LOSS_PCT` | 止损时资金损失比例（仅 SL 启用时有效） | `0.50` |
| `TAKE_PROFIT_PCT` | 剩余上涨空间止盈比例 | `0.40` |
| `MIN_ENTRY_PRICE` | 最低入场价格 | `0.25` |
| `MAX_ENTRY_PRICE` | 最高入场价格 | `0.75` |
| `MAX_SPREAD_PCT` | 买卖价差相对中间价的最大比例 | `0.05` |
| `ENTRY_COOLDOWN_SEC` | 两次入场尝试间隔秒数 | `90` |
| `MAX_TRADES_PER_MARKET` | 每个 15 分钟市场最大入场次数 | `1` |
| `LOCK_MARKET_DIRECTION` | 首次交易后锁定方向 | `true` |
| `MAX_CHASE_DELTA` | 允许再次入场的最大价格变动 | `0.12` |
| `MIN_ML_EDGE` | 下注所需的最小 ML 概率差 | `0.10` |
| `LATE_ENTRY_CUTOFF_SEC` | 结算前拒绝新入场的秒数 | `120` |
| `MARKET_BUY_USD` | 每笔订单美元金额 | `1.00` |

完整列表见 `.env.example`。

---

## 运行机器人

- **统一入口**：`main.py` 支持 `--test-mode`、`--simulation`、`--live`。
- **自动重启**：`supervisor.py` 循环运行 `main.py`，用于无人值守运行。
- **查看纸交易记录**：

```bash
python scripts/view_trades.py
```

---

## 监控

- 指标导出器和辅助工具位于 `monitoring/` 目录。
- Grafana 仪表盘资源位于 `grafana/` 目录（使用 `grafana/import_dashboard.py` 导入）。

根据需要接入你自己的 Prometheus/Grafana 栈。

---

## 交易模式

支持通过 Redis 进行模式切换（模拟 vs 实盘），无需重启；见 `scripts/redis_control.py`。

---

## 分阶段测试

**按顺序**运行各阶段检查，前一阶段成功后再运行下一阶段。

| 阶段 | 测试重点 | 命令 |
|------|----------|------|
| 1 | 数据源（交易所、新闻等） | `python scripts/test_data_sources.py test` |
| 2 | 摄入（适配器、WebSocket、校验） | `python scripts/test_ingestion.py test` |
| 3 | Nautilus 核心（合约、引擎、事件） | `python scripts/test_nautilus.py test` |
| 4 | 策略大脑（信号处理器、融合） | `python scripts/test_strategy.py test` |
| 5 | 执行（风控、客户端、引擎） | `python scripts/test_execution.py test` |

直接调试 Gamma API：

```bash
python scripts/debug_gamma_api.py
```

---

## 需要多少本金？

参考配置每笔交易使用 **~$1**。你仍然需要足够的余额来支付手续费、价差，以及应对连续亏损。许多使用者在早期实验阶段保留 **$10–$50**；在模拟结果符合预期之前不要加仓。**这不是财务建议。**

---

## 是否盈利？

**不保证盈利**。短周期市场有手续费、价差、逆向选择和服务中断等风险。模拟结果**不能**可靠预测实盘表现。先用纸交易和小仓位；将每次运行都视为实验。

---

## 适合人群

- **需要速度和自动化**的 15 分钟加密预测市场交易者。
- **开发者**：习惯编辑 `.env`、阅读日志、运行阶段测试排查问题。
- **将风险放在首位**的用户：希望在扩大仓位前拥有明确的上限和可观测性。

---

## 贡献与想法

欢迎通过常规 GitHub 流程（fork、分支、Pull Request）贡献代码。

**贡献方向：**
- 添加衍生品上下文（资金费率、持仓量）作为额外信号处理器。
- 新的信号处理器或融合规则。
- 成交和错误的 Telegram / Discord 告警。
- 配置与状态的轻量 Web UI。
- 扩展至 ETH、SOL 及其他 Polymarket 短周期产品。
- 更强的 ML / 校准层，配合诚实的评估和纸交易门控。

---

## 许可证

MIT 许可证。见仓库中的 `LICENSE` 文件。

---

## 免责声明

加密货币和预测市场工具的交易涉及**巨大的亏损风险**。本软件仅用于**教育和研究目的**。过往表现不代表未来结果。作者对任何财务损失**不承担责任**。请先使用模拟模式，保持小仓位，且只使用你能够承受全部损失的资金进行交易。

---

## 致谢

- [NautilusTrader](https://nautilustrader.io/) — 交易框架
- [Polymarket](https://polymarket.com) — 预测市场平台

---
---

<a name="русский"></a>
# 🇷🇺 Русский

**Алгоритмический торговый бот для 15-минутных рынков BTC вверх/вниз на Polymarket.** Объединяет множество источников сигналов в реальном времени, ограничения рисков, мониторинг и опциональные хуки машинного обучения в семиэтапный конвейер.

---

<p align="center">
  <strong>⭐ Хотите более прибыльных торговых ботов?</strong><br><br>
  Создан <a href="https://github.com/gamma-trade-lab"><strong>Gamma Trade Lab</strong></a> — высокопроизводительные автоматизированные торговые системы для Polymarket.<br><br>
  <a href="https://github.com/gamma-trade-lab"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-gamma--trade--lab-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="mailto:gammatradeorg@gmail.com"><img alt="Email" src="https://img.shields.io/badge/Email-gammatradeorg@gmail.com-EA4335?logo=gmail&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>
</p>

---

## Содержание

- [Основная идея](#основная-идея)
- [Возможности](#возможности)
- [Предварительные требования](#предварительные-требования)
- [Быстрый старт](#быстрый-старт)
- [Конфигурация](#конфигурация)
- [Запуск бота](#запуск-бота)
- [Мониторинг](#мониторинг)
- [Режимы торговли](#режимы-торговли)
- [Тестирование отдельных этапов](#тестирование-отдельных-этапов)
- [Сколько нужно денег?](#сколько-нужно-денег)
- [Это прибыльно?](#это-прибыльно)
- [Для кого подходит](#для-кого-подходит)
- [Вклад и идеи](#вклад-и-идеи)
- [Лицензия](#лицензия)
- [Отказ от ответственности](#отказ-от-ответственности)

---

## Основная Идея

Рынки прогнозов для краткосрочных движений BTC шумные и быстрые. Этот проект рассматривает их как **систематическую торговую задачу**: получить рыночные и контекстные данные, нормализовать их через единый путь приёма, объединить несколько детекторов в решение и исполнить его через адаптер брокера с **жёсткими ограничениями риска** (малый размер позиции, параметры тейк-профита). Цель — не «один магический сигнал», а **тестируемый стек**, который можно запустить в симуляции, наблюдать в Grafana и только потом направить на реальный капитал.

---

## Возможности

- **Семиэтапный конвейер** — Внешние данные → приём → ядро Nautilus → обработчики сигналов и их слияние → исполнение и риск → мониторинг → обратная связь / хуки обучения.
- **Многосигнальный стек** — Обнаружение всплесков, сентиментальные входные данные, логика дивергенции, обработчики стакана и импульса, плюс движок слияния для объединения голосов.
- **Риск как приоритет** — Настраиваемые лимиты (~$1 за сделку), тейк-профит, ценовой диапазон входа, фильтр спреда, блокировка направления, защита от гонки за ценой.
- **Переключатель стоп-лосса** — `ENABLE_STOP_LOSS=false` позволяет позиции удерживаться до тейк-профита или расчёта рынка; переключите на `true`, чтобы снова включить ранний стоп-лосс.
- **Фильтр преимущества ML** — Ставка делается только когда вероятность модели XGBoost отклоняется от цены Polymarket минимум на `MIN_ML_EDGE` (по умолчанию 10 процентных пунктов).
- **Одна ставка на рынок** — `MAX_TRADES_PER_MARKET=1` открывает один вход за 15-минутный слот.
- **Симуляция и реальная торговля** — Запускайте бумажные / тестовые режимы без производственных ключей; переходите к реальной торговле только когда готовы.
- **Операционный инструментарий** — Управление режимом через Redis, метрики для Grafana, просмотр бумажных сделок, автоматический перезапуск для длительной работы.
- **Хук самообучения** — Веса могут корректироваться на основе обратной связи о производительности (см. `feedback/`).
- **Отказоустойчивость** — Обработка WebSocket, ограничение частоты запросов, валидация и патчи для граничных случаев Polymarket + Nautilus (загрузка Gamma, размер рыночного ордера, защита `prometheus_client` на Windows).

---

## Предварительные Требования

- **Python 3.14+**
- **Redis** — используется для переключения режимов и управляющей плоскости
- **Аккаунт Polymarket** с API-ключами для реальной торговли
- **Git**

---

## Быстрый Старт

### 1. Клонировать репозиторий

```bash
git clone https://github.com/yourusername/polymarket-btc-15m-bot.git
cd polymarket-btc-15m-bot
```

### 2. Создать виртуальное окружение

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. Установить зависимости

```bash
pip install -r requirements.txt
```

### 4. Настроить переменные окружения

```bash
cp .env.example .env
```

Отредактируйте `.env`, вставив ваши учётные данные и параметры:

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

### 5. Запустить Redis

```bash
redis-server
```

macOS (Homebrew): `brew install redis && redis-server`.
Debian/Ubuntu: `sudo apt install redis-server && redis-server`.

### 6. Запустить бота

```bash
# Быстрый тестовый цикл (симулированные сделки ~каждую минуту)
python main.py --test-mode

# Обычная симуляция (15-минутный таймер)
python main.py --simulation

# Реальная торговля (настоящие деньги — нужны рабочие ключи)
python supervisor.py --live
```

---

## Конфигурация

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `ENABLE_STOP_LOSS` | Включить ранний стоп-лосс | `false` |
| `STOP_LOSS_PCT` | Доля капитала при стоп-лоссе (только при включённом SL) | `0.50` |
| `TAKE_PROFIT_PCT` | Доля оставшегося роста для тейк-профита | `0.40` |
| `MIN_ENTRY_PRICE` | Минимальная цена токена для входа | `0.25` |
| `MAX_ENTRY_PRICE` | Максимальная цена токена для входа | `0.75` |
| `MAX_SPREAD_PCT` | Максимальный спред относительно средней цены | `0.05` |
| `ENTRY_COOLDOWN_SEC` | Секунд между попытками входа | `90` |
| `MAX_TRADES_PER_MARKET` | Максимум входов на 15-минутный рынок | `1` |
| `LOCK_MARKET_DIRECTION` | Блокировка направления после первой сделки | `true` |
| `MAX_CHASE_DELTA` | Максимальное ценовое отклонение для повторного входа | `0.12` |
| `MIN_ML_EDGE` | Минимальное отклонение вероятности ML для ставки | `0.10` |
| `LATE_ENTRY_CUTOFF_SEC` | Секунд до расчёта, после которых вход запрещён | `120` |
| `MARKET_BUY_USD` | Сумма ордера в долларах | `1.00` |

Полный список с комментариями — в `.env.example`.

---

## Запуск Бота

- **Единая точка входа**: `main.py` поддерживает `--test-mode`, `--simulation`, `--live`.
- **Автоматический перезапуск**: `supervisor.py` запускает `main.py` в цикле для работы без присмотра.
- **Просмотр бумажных сделок**:

```bash
python scripts/view_trades.py
```

---

## Мониторинг

- Экспортёры метрик и вспомогательные инструменты находятся в `monitoring/`.
- Дашборды Grafana — в `grafana/` (импорт через `grafana/import_dashboard.py`).

Подключайте к вашему стеку Prometheus/Grafana по мере необходимости.

---

## Режимы Торговли

Переключение режимов через Redis (симуляция vs реальная торговля) поддерживается без перезапуска бота; см. `scripts/redis_control.py`.

---

## Тестирование Отдельных Этапов

Запускайте проверки **по порядку** — следующий этап только после успешного завершения предыдущего.

| Этап | Что тестируется | Команда |
|------|-----------------|---------|
| 1 | Источники данных (биржи, новости) | `python scripts/test_data_sources.py test` |
| 2 | Приём (адаптер, WebSocket, валидация) | `python scripts/test_ingestion.py test` |
| 3 | Ядро Nautilus (инструменты, движок, события) | `python scripts/test_nautilus.py test` |
| 4 | Мозг стратегии (обработчики, слияние) | `python scripts/test_strategy.py test` |
| 5 | Исполнение (риск, клиент, движок) | `python scripts/test_execution.py test` |

Отладка Gamma API напрямую:

```bash
python scripts/debug_gamma_api.py
```

---

## Сколько Нужно Денег?

Базовая конфигурация использует **~$1 за сделку**. Вам всё равно нужен достаточный баланс для покрытия комиссий, спреда и серии убыточных сделок. Многие операторы держат **$10–$50** для ранних экспериментов; масштабируйтесь только после того, как симуляция оправдает ожидания. **Это не финансовый совет.**

---

## Это Прибыльно?

**Прибыль не гарантирована.** Краткосрочные рынки имеют комиссии, спред, неблагоприятный отбор и перебои в работе. Результаты симуляции **не позволяют** надёжно прогнозировать реальную торговлю. Сначала используйте бумажный режим и малый размер позиции; относитесь к каждому запуску как к эксперименту.

---

## Для Кого Подходит

- **Трейдеры**, которым нужна скорость и автоматизация на 15-минутных криптовалютных рынках прогнозов.
- **Разработчики**, умеющие редактировать `.env`, читать логи и запускать поэтапные тесты при неполадках.
- **Люди, ставящие риск на первое место**: хотят чёткие лимиты и наблюдаемость перед масштабированием.

---

## Вклад и Идеи

Вклад приветствуется через стандартный GitHub-процесс (форк, ветка, Pull Request).

**Идеи для вклада:**
- Добавить деривативный контекст (ставки финансирования, открытый интерес) как дополнительные обработчики.
- Новые обработчики сигналов или правила слияния.
- Оповещения Telegram / Discord о сделках и ошибках.
- Небольшой веб-интерфейс для конфигурации и мониторинга статуса.
- Расширить поддержку на ETH, SOL и другие краткосрочные продукты Polymarket.
- Более сильные слои ML / калибровки с честной оценкой и шлюзами бумажной торговли.

---

## Лицензия

Лицензия MIT. См. файл `LICENSE` в репозитории.

---

## Отказ от Ответственности

Торговля криптовалютами и инструментами рынков прогнозов связана со **значительным риском потерь**. Данное программное обеспечение предоставляется исключительно в **образовательных и исследовательских целях**. Прошлые результаты не гарантируют будущих. Авторы **не несут ответственности** за какие-либо финансовые потери. Начинайте с симуляции, используйте малый размер позиции и торгуйте только теми средствами, потерю которых вы можете себе позволить.

---

## Благодарности

- [NautilusTrader](https://nautilustrader.io/) — Торговый фреймворк
- [Polymarket](https://polymarket.com) — Платформа рынков прогнозов
