# Polymarket BTC 5-мин Торговый Бот

[🇬🇧 English](README.md) · [🇨🇳 中文](README.zh.md)

---

<img width="1981" height="793" alt="thumbnail" src="https://github.com/user-attachments/assets/31efdf63-1172-46b2-8713-e1173dc06722" />

<p align="center">
  Создан <a href="https://x.com/RetroValix"><strong>Retro Valix</strong></a><br><br>
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>&nbsp;
  <a href="https://x.com/RetroValix"><img alt="X" src="https://img.shields.io/badge/X-@RetroValix-000000?logo=x&logoColor=white"></a>&nbsp;
  <a href="https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154"><img alt="Medium" src="https://img.shields.io/badge/Medium-Guide-000000?logo=medium&logoColor=white"></a>
</p>

---

## Демо

https://github.com/user-attachments/assets/8f9a2b66-e291-44e6-8e6f-edecf65a7f4d

---

## Публичная vs Premium

Этот репозиторий — **публичная версия**: открытый исходный код, чтобы вы сами могли проверить стек, риск-логику и учёт PnL.  
Premium — для трейдеров, которым нужна production-производительность после проверки публичного бота.

| | Публичная (этот репо) | Premium |
|---|---|---|
| Назначение | Тест · обучение · проверка | Живой капитал · лучшие результаты |
| Риск и PnL | Частично включено | Полностью + тюнинг |
| Обучающие данные | Общая / прогревающая модель | **200,000+** сделок |
| Win rate | Не заявляется (образовательно) | **98.8%+** |
| Доказательства | Симуляция / live у вас | На приватной встрече |
| Доступ | Только BTC | BTC, ETH, SOL, DOGE, XRP, BNB |

**Путь:** запустить публичную версию → проверить результаты → [обсудить сотрудничество](https://t.me/RetroValix)

Доказательства по аккаунту Polymarket — только на звонке, не публикуются.

→ **Сотрудничество / proof:** [Telegram @RetroValix](https://t.me/RetroValix)

> ### ⚠️ Что я предлагаю
>
> **Я не продаю premium-версию отдельно.** Доступен только формат сотрудничества — запуск бота вместе.
>
> - Пассивная доля прибыли пропорционально внесённому капиталу.
> - Зафиксированный капитал можно **вывести по запросу**, когда вы решите выйти из соглашения.
>
> → Свяжитесь через [Telegram @RetroValix](https://t.me/RetroValix).

---

## Чем торгует бот

Рынки Polymarket **Bitcoin Up or Down** на 5 минут:

- Формат slug: `btc-updown-5m-{unix_start}`
- Длина окна: **300 секунд** (UTC floor: `(ts // 300) * 300`)
- Окно входа по умолчанию: секунды **180–270** каждого рынка (поздний вход)

---

## Ссылки и материалы

| Источник | Ссылка |
|--------|------|
| Medium — гайд | [Polymarket BTC AI Trading Bot with NautilusTrader](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| X (Twitter) | [@RetroValix](https://x.com/RetroValix) |
| Telegram | [@RetroValix](https://t.me/RetroValix) |

---

## Возможности

- Мультисигнальный fusion + ML edge gate для **5-мин** рынков BTC Up/Down
- Риск-контроль (лимит размера, TP/SL, фильтр спреда, anti-chase, одна ставка на окно)
- Режимы simulation / live + терминальный UI
- Логи paper-сделок и метрики Grafana

---

## Быстрый старт

**Требуется:** Python 3.14+ · Redis · API-ключи Polymarket (для live)

```bash
git clone https://github.com/retrovaliks/polymarket-btc-5m-trading-bot.git
cd polymarket-btc-5m-trading-bot

python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # добавьте ключи
```

```bash
python main.py --test-mode      # быстрый paper-цикл
python main.py --simulation     # 5-мин paper
python supervisor.py --live     # реальные деньги
```

Просмотр paper-сделок:

```bash
python scripts/view_trades.py
```

---

## Конфиг (основное)

| Параметр | По умолчанию | Примечание |
|-----------|---------|--------|
| `MARKET_BUY_USD` | `1.00` | USD на ордер |
| `ENABLE_STOP_LOSS` | `false` | Ранний выход по SL |
| `TAKE_PROFIT_PCT` | `0.40` | Доля take-profit |
| `MIN_ENTRY_PRICE` / `MAX_ENTRY_PRICE` | `0.25` / `0.75` | Диапазон входа |
| `TRADE_WINDOW_SEC_START` / `END` | `180` / `270` | Окно входа внутри 5-мин рынка |
| `ENTRY_COOLDOWN_SEC` | `30` | Мин. пауза между входами |
| `MAX_TRADES_PER_MARKET` | `1` | Одна ставка на 5-мин окно |
| `MIN_ML_EDGE` | `0.10` | Мин. разрыв ML vs рынок |

Полный список: [`.env.example`](.env.example)

---

## Ссылки

| | |
|---|---|
| Начать здесь | [Быстрый старт](#быстрый-старт) |
| Публичная vs Premium | [Таблица](#публичная-vs-premium) |
| Medium-гайд | [Статья](https://medium.com/@RetroValix/polymarket-btc-15-minute-ai-trading-bot-with-nautilustrader-c897bf225154) |
| Контакт | [Telegram](https://t.me/RetroValix) · [X](https://x.com/RetroValix) |
| Фазовые тесты | `python scripts/test_data_sources.py test` → … → `test_execution.py` |

---

## Отказ от ответственности

Торговля сопряжена с **существенным риском потерь**. ПО предназначено для **обучения и исследований**. Прибыль не гарантируется. Прошлые результаты ≠ будущие. Начинайте с симуляции; используйте только капитал, полную потерю которого можете принять.

---

## Лицензия

MIT — см. [`LICENSE`](LICENSE)

---

<div align="center">
  <a href="https://t.me/RetroValix">
    <img width="85" height="85" alt="Retro Valix" src="https://github.com/user-attachments/assets/66c994bf-c618-40e7-a0f4-d295e09d1e91" /><br>
    <span>Retro Valix</span>
  </a>
</div>
