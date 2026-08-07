# Polymarket BTC 5 分钟交易机器人

[🇬🇧 English](README.md) · [🇷🇺 Русский](README.ru.md)

---

<img width="1981" height="793" alt="thumbnail" src="https://github.com/user-attachments/assets/31efdf63-1172-46b2-8713-e1173dc06722" />

<p align="center">
  由 <a href="https://x.com/RetroValix"><strong>Retro Valix</strong></a> 打造<br><br>
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>&nbsp;
  <a href="https://x.com/RetroValix"><img alt="X" src="https://img.shields.io/badge/X-@RetroValix-000000?logo=x&logoColor=white"></a>&nbsp;
  <a href="https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154"><img alt="Medium" src="https://img.shields.io/badge/Medium-Guide-000000?logo=medium&logoColor=white"></a>
</p>

---

## 演示

https://github.com/user-attachments/assets/8f9a2b66-e291-44e6-8e6f-edecf65a7f4d

---

## 公开版 vs 高级版

本仓库为**公开版** — 开源，便于你自行测试技术栈、风控逻辑与盈亏跟踪。  
高级能力面向已验证公开版、希望获得生产级表现的交易者。

| | 公开版（本仓库） | 高级版 |
|---|---|---|
| 用途 | 测试 · 学习 · 验证 | 实盘资金 · 最佳表现 |
| 风控与盈亏 | 部分包含 | 完整包含 + 调优 |
| 训练数据 | 通用 / 预热模型 | **200,000+** 笔交易 |
| 胜率 | 不作宣称（教育用途） | **98.8%+** |
| 证明 | 自行运行模拟 / 实盘 | 私下会议分享 |
| 标的范围 | 仅 BTC | BTC、ETH、SOL、DOGE、XRP、BNB |

**流程：** 运行公开版 → 验证结果 → [洽谈协作](https://t.me/RetroValix)

Polymarket 账户证明仅在通话中分享 — 不公开张贴。

→ **协作 / 证明：** [Telegram @RetroValix](https://t.me/RetroValix)

> ### ⚠️ 我提供什么
>
> **我不单独出售高级版。** 仅提供协作模式，共同运行机器人。
>
> - 按你投入的资金比例，被动获得收益分成。
> - 决定退出时，已投入资金可按约定**申请撤回**。
>
> → 通过 [Telegram @RetroValix](https://t.me/RetroValix) 联系洽谈。

---

## 交易标的

Polymarket **Bitcoin Up or Down** 5 分钟市场：

- Slug 格式：`btc-updown-5m-{unix_start}`
- 窗口时长：**300 秒**（UTC 向下取整：`(ts // 300) * 300`）
- 默认入场窗口：每个市场第 **180–270** 秒（偏后期入场）

---

## 参考链接

| 来源 | 链接 |
|--------|------|
| Medium — 构建指南 | [Polymarket BTC AI Trading Bot with NautilusTrader](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| X (Twitter) | [@RetroValix](https://x.com/RetroValix) |
| Telegram | [@RetroValix](https://t.me/RetroValix) |

---

## 功能特性

- 面向 **5 分钟** BTC Up/Down 市场的多信号融合 + ML 边缘门槛
- 风控（仓位上限、止盈/止损、价差过滤、反追涨、每窗口一笔）
- 模拟 / 实盘模式 + 终端 UI 仪表盘
- 纸面交易日志与 Grafana 指标

---

## 快速开始

**要求：** Python 3.14+ · Redis · Polymarket API 密钥（实盘）

```bash
git clone https://github.com/retrovaliks/polymarket-btc-5m-trading-bot.git
cd polymarket-btc-5m-trading-bot

python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # 填入你的密钥
```

```bash
python main.py --test-mode      # 快速纸面循环
python main.py --simulation     # 5 分钟纸面
python supervisor.py --live     # 真实资金
```

查看纸面交易：

```bash
python scripts/view_trades.py
```

---

## 配置（要点）

| 参数 | 默认值 | 说明 |
|-----------|---------|--------|
| `MARKET_BUY_USD` | `1.00` | 每笔订单美元金额 |
| `ENABLE_STOP_LOSS` | `false` | 提前止损退出 |
| `TAKE_PROFIT_PCT` | `0.40` | 止盈比例 |
| `MIN_ENTRY_PRICE` / `MAX_ENTRY_PRICE` | `0.25` / `0.75` | 入场价格区间 |
| `TRADE_WINDOW_SEC_START` / `END` | `180` / `270` | 每个 5 分钟市场内的入场窗口 |
| `ENTRY_COOLDOWN_SEC` | `30` | 两次入场最短间隔（秒） |
| `MAX_TRADES_PER_MARKET` | `1` | 每个 5 分钟窗口一笔 |
| `MIN_ML_EDGE` | `0.10` | ML 相对市场的最小差距 |

完整列表：[`.env.example`](.env.example)

---

## 链接

| | |
|---|---|
| 从这里开始 | [快速开始](#快速开始) |
| 公开版 vs 高级版 | [对照表](#公开版-vs-高级版) |
| Medium 指南 | [文章](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| 联系 | [Telegram](https://t.me/RetroValix) · [X](https://x.com/RetroValix) |
| 分阶段测试 | `python scripts/test_data_sources.py test` → … → `test_execution.py` |

---

## 免责声明

交易涉及**重大亏损风险**。本软件仅供**教育与研究**。不保证盈利。过往表现 ≠ 未来结果。请先模拟；仅使用你能承受全部损失的资金。

---

## 许可证

MIT — 见 [`LICENSE`](LICENSE)

---

<div align="center">
  <a href="https://t.me/RetroValix">
    <img width="85" height="85" alt="Retro Valix" src="https://github.com/user-attachments/assets/66c994bf-c618-40e7-a0f4-d295e09d1e91" /><br>
    <span>Retro Valix</span>
  </a>
</div>
