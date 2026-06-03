# Polymarket 交易机器人

**阅读其他语言版本：** [🇬🇧 English](README.md) · [🇷🇺 Русский](README.ru.md)

---
<img width="1981" height="793" alt="thumbnail" src="https://github.com/user-attachments/assets/31efdf63-1172-46b2-8713-e1173dc06722" />

<p align="center">
  <strong>⭐ 想要更多盈利的交易机器人？</strong><br><br>
  由 <a href="https://github.com/RetroValixx"><strong>Retro Valix</strong></a> 打造 — 面向 Polymarket 的高性能自动化交易系统。<br><br>
  <a href="https://github.com/RetroValixx"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-RetroValixx-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>&nbsp;
  <a href="https://x.com/RetroValix"><img alt="X" src="https://img.shields.io/badge/X-@RetroValix-000000?logo=x&logoColor=white"></a>
</p>

---

## 工作展示

<video width="100%" controls src="https://github.com/user-attachments/assets/d89a6bc1-0cf6-4a1f-a29e-5e0549945e6f">
  <a href="https://github.com/user-attachments/assets/d89a6bc1-0cf6-4a1f-a29e-5e0549945e6f">观看演示视频</a>
</video>

<img width="100%" alt="2" src="https://github.com/user-attachments/assets/447c9671-3f47-4bde-a4be-744af27bdbb1" />

<img width="100%" alt="4" src="https://github.com/user-attachments/assets/8b88610b-c54b-4e3d-b7a6-2ccef7b72ca4" />

<img width="100%" alt="3" src="https://github.com/user-attachments/assets/f7052333-8107-40d8-9703-d1bbd2b77bc7" />

---

## 核心理念

短期 BTC 涨跌预测市场噪声大、节奏快。本项目将其视为**系统化交易问题**：拉取市场与上下文数据，经统一接入路径规范化，融合多个检测器形成决策，再通过经纪适配器以**硬性风控**（小仓位、止盈参数等）执行。目标不是“一个神奇信号”，而是一个可在模拟环境运行、在 Grafana 中观察、再考虑接入真实资金的**可测试技术栈**。

---

## 功能特性

- **七阶段流水线** — 外部数据源 → 接入 → Nautilus 核心 → 信号处理器与融合 → 执行与风控 → 监控 → 反馈/学习钩子。
- **多信号栈** — 尖峰检测、情绪类输入、背离逻辑、订单簿与动量类处理器，以及融合投票。
- **风控优先默认值** — 可配置上限（如每笔约 $1）、止盈、入场价格区间、价差过滤、方向锁定、反追涨保护。
- **止损开关** — `ENABLE_STOP_LOSS=false` 时持仓可持有至止盈或结算；设为 `true` 可重新启用提前止损。
- **ML 边缘门槛** — 仅当 XGBoost 概率与 Polymarket 价格相差至少 `MIN_ML_EDGE`（默认 10 个百分点）时才下单。
- **每市场限制** — `MAX_TRADES_PER_MARKET=1` 在每个 15 分钟槽内只开一次仓。
- **模拟与实盘** — 纸面/测试模式无需生产密钥；准备好后再切换实盘。
- **运维工具** — Redis 模式切换、Grafana 指标、纸面交易查看、长时间运行自动重启。
- **自学习钩子** — 可根据绩效反馈调整权重（见 `feedback/` 与策略配置）。
- **韧性** — WebSocket、限流、校验，以及针对 Polymarket + Nautilus 边界情况的补丁（Gamma 加载、市价单大小、Windows `prometheus_client` 保护）。

---

## 前置条件

- **Python 3.14+**
- **Redis** — 用于模式切换与控制面行为
- **Polymarket 账户** — 实盘交易需 API 凭证
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

编辑 `.env`，填入凭证与参数：

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

macOS（Homebrew）：`brew install redis && redis-server`  
Debian/Ubuntu：`sudo apt install redis-server && redis-server`

### 6. 运行机器人

```bash
# 快速测试（模拟交易，约每分钟一次）
python main.py --test-mode

# 常规模拟（15 分钟时钟）
python main.py --simulation

# 实盘（真实资金 — 需有效凭证）
python supervisor.py --live
```

---

## 参数配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `ENABLE_STOP_LOSS` | 启用提前止损 | `false` |
| `STOP_LOSS_PCT` | 止损时损失的本金比例（仅 SL 启用时） | `0.50` |
| `TAKE_PROFIT_PCT` | 剩余上涨空间止盈比例 | `0.40` |
| `MIN_ENTRY_PRICE` | 最低入场价格 | `0.25` |
| `MAX_ENTRY_PRICE` | 最高入场价格 | `0.75` |
| `MAX_SPREAD_PCT` | 买卖价差相对中间价上限 | `0.05` |
| `ENTRY_COOLDOWN_SEC` | 两次入场尝试间隔（秒） | `90` |
| `MAX_TRADES_PER_MARKET` | 每个 15 分钟市场最大入场次数 | `1` |
| `LOCK_MARKET_DIRECTION` | 首笔交易后锁定方向 | `true` |
| `MAX_CHASE_DELTA` | 再入场允许的最大价格变动 | `0.12` |
| `MIN_ML_EDGE` | 下单所需最小 ML 概率差 | `0.10` |
| `LATE_ENTRY_CUTOFF_SEC` | 结算前拒绝新入场的秒数 | `120` |
| `MARKET_BUY_USD` | 每笔订单美元金额 | `1.00` |

完整列表见 `.env.example` 内联注释。

---

## 运行机器人

- **统一入口**：`main.py` 支持 `--test-mode`、`--simulation`、`--live`。
- **自动重启**：`supervisor.py` 循环运行 `main.py`，适合无人值守。
- **查看纸面交易**：

```bash
python scripts/view_trades.py
```

---

## 监控

- 指标导出与辅助工具位于 `monitoring/`。
- Grafana 仪表板资源位于 `infra/grafana/`（使用 `infra/grafana/import_dashboard.py` 导入）。

按需接入你自己的 Prometheus/Grafana 栈。

---

## 交易模式

支持通过 Redis 在模拟与实盘之间切换，无需重启；见 `scripts/redis_control.py`。

---

## 分阶段测试

**按顺序**运行各阶段检查，前一阶段成功后再进行下一阶段。

| 阶段 | 测试重点 | 命令 |
|------|----------|------|
| 1 | 数据源（交易所、新闻等） | `python scripts/test_data_sources.py test` |
| 2 | 接入（适配器、WebSocket、校验） | `python scripts/test_ingestion.py test` |
| 3 | Nautilus 核心（合约、引擎、事件） | `python scripts/test_nautilus.py test` |
| 4 | 策略（处理器、融合） | `python scripts/test_strategy.py test` |
| 5 | 执行（风控、客户端、引擎） | `python scripts/test_execution.py test` |

直接调试 Gamma API：

```bash
python scripts/debug_gamma_api.py
```

---

## 需要多少本金？

参考配置每笔约 **$1**。仍需足够余额支付手续费、价差并承受连续亏损。许多人在早期实验阶段保留 **$10–$50**；仅在模拟结果符合预期后再放大。**不构成财务建议。**

---

## 是否盈利？

**不保证盈利。** 短期市场有手续费、价差、逆向选择和中断风险。模拟结果**不能**可靠预测实盘表现。请先纸面交易、小仓位；将每次运行视为实验。

---

## 适合人群

- 希望在 15 分钟加密预测市场上**自动化交易**的交易者。
- 习惯编辑 `.env`、阅读日志、运行分阶段测试的**开发者**。
- 将**风险放在首位**、希望在放大仓位前具备明确上限与可观测性的用户。

---

## 贡献与想法

欢迎通过常规 GitHub 流程（fork、分支、Pull Request）贡献。

**贡献方向示例：**
- 将衍生品上下文（资金费率、持仓量）加入为额外处理器。
- 新的信号处理器或融合规则。
- 成交与错误的 Telegram / Discord 告警。
- 配置与状态的轻量 Web UI。
- 扩展至 ETH、SOL 及其他 Polymarket 短期产品。
- 更强的 ML/校准层，配合诚实的评估与纸面交易门槛。

---

## 许可证

MIT 许可证。见仓库中的 `LICENSE` 文件。

---

## 免责声明

加密货币与预测市场工具交易涉及**重大亏损风险**。本软件仅供**教育与研究**使用。过往表现不代表未来结果。作者对任何财务损失**不承担责任**。请先模拟、小仓位，且仅使用你能完全承受损失的资金交易。

---

## 致谢

- [NautilusTrader](https://nautilustrader.io/) — 交易框架
- [Polymarket](https://polymarket.com) — 预测市场平台

<div align="center">
  <h2>Made with ❤️ by</h2>
  <a href="https://t.me/RetroValix">
    <img width="85" height="85" alt="XTLLbabR_400x400" src="https://github.com/user-attachments/assets/66c994bf-c618-40e7-a0f4-d295e09d1e91" />    <br>
    <span>Retro Valix</span>
  </a>
</div>
