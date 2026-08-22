# Polymarket 5-мин Торговый Бот

[🇬🇧 English](README.md) · [🇨🇳 中文](README.zh.md)

## Обзор

Это автоматический торговый бот для 5-минутных рынков Polymarket Bitcoin Up or Down. Он работает на NautilusTrader, объединяет несколько сигналов рыночной микроструктуры и фильтрует входы с помощью XGBoost-модели edge относительно цены Polymarket.

---

<img width="1774" height="710" alt="HPiqjFvWcAAWuKw" src="https://github.com/user-attachments/assets/b9ca6c33-243e-4657-b5b4-1731dec8eb12" />

<p align="center">
  Создан <a href="https://x.com/0xzkleo"><strong>Leo</strong></a><br><br>
  <a href="https://t.me/iamjustleo"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@iamjustleo-26A5E4?logo=telegram&logoColor=white"></a>&nbsp;
  <a href="https://x.com/0xzkleo"><img alt="X" src="https://img.shields.io/badge/X-@0xzkleo-000000?logo=x&logoColor=white"></a>&nbsp;
</p>

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

→ **Сотрудничество / proof:** [Telegram @iamjustleo](https://t.me/iamjustleo)

> ### ⚠️ Что я предлагаю
>
> **Я не продаю premium-версию отдельно.** Доступен только формат сотрудничества — запуск бота вместе.
>
> - Прибыль выплачивается пассивно, исходя из вложенного капитала (15% от инвестиций в неделю).
> - Зафиксированный капитал можно **вывести по запросу**, когда вы решите выйти из соглашения.
>
> → Свяжитесь через [Telegram @RetroValix](https://t.me/iamjustleo).

---

## Чем торгует бот

Рынки Polymarket **Bitcoin Up or Down** на 5 минут:

- Формат slug: `btc-updown-5m-{unix_start}`
- Длина окна: **300 секунд** (UTC floor: `(ts // 300) * 300`)
- Окно входа по умолчанию: секунды **180–270** каждого рынка (поздний вход)

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
git clone https://github.com/0xzkleo/polymarket-5min-crypto-trading-bot.git
cd polymarket-5min-crypto-trading-bot

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
| Twitter-гайд | [Статья](https://x.com/0xzkleo/status/2031880258425627103) |
| Контакт | [Telegram](https://t.me/iamjustleo) · [X](https://x.com/0xzkleo) |
| Фазовые тесты | `python scripts/test_data_sources.py test` → … → `test_execution.py` |

---

## Отказ от ответственности

Торговля сопряжена с **существенным риском потерь**. ПО предназначено для **обучения и исследований**. Прибыль не гарантируется. Прошлые результаты ≠ будущие. Начинайте с симуляции; используйте только капитал, полную потерю которого можете принять.

---

## Лицензия

MIT — см. [`LICENSE`](LICENSE)
