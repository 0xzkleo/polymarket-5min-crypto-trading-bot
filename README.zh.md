# Polymarket 交易机器人

**阅读其他语言版本：**
[🇬🇧 English](README.md) · [🇷🇺 Русский](README.ru.md)

---

<p align="center">
  <strong>⭐ 想要更多盈利的交易机器人？</strong><br><br>
  由 <a href="https://github.com/gamma-trade-lab"><strong>Gamma Trade Lab</strong></a> 打造 — 面向 Polymarket 的高性能自动化交易系统。<br><br>
  <a href="https://github.com/gamma-trade-lab"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-gamma--trade--lab-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="mailto:gammatradeorg@gmail.com"><img alt="Email" src="https://img.shields.io/badge/Email-gammatradeorg@gmail.com-EA4335?logo=gmail&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>
</p>

---

**Polymarket 15 分钟 BTC 涨跌自动交易机器人。** 将多个实时信号源、风控限制、监控以及可选的机器学习钩子整合到一个七阶段流水线中。

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
