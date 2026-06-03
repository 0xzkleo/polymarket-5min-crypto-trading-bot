# Polymarket Торговый Бот

**Читать на другом языке:** [🇬🇧 English](README.md) · [🇨🇳 中文](README.zh.md)

---
<img width="1981" height="793" alt="thumbnail" src="https://github.com/user-attachments/assets/31efdf63-1172-46b2-8713-e1173dc06722" />

<p align="center">
  <strong>⭐ Хотите более прибыльных торговых ботов?</strong><br><br>
  Создан <a href="https://github.com/RetroValixx"><strong>Retro Valix</strong></a> — высокопроизводительные автоматизированные торговые системы для Polymarket.<br><br>
  <a href="https://github.com/RetroValixx"><img alt="GitHub" src="https://img.shields.io/badge/GitHub-gamma--trade--lab-181717?logo=github&logoColor=white"></a>&nbsp;
  <a href="https://t.me/RetroValix"><img alt="Telegram" src="https://img.shields.io/badge/Telegram-@RetroValix-26A5E4?logo=telegram&logoColor=white"></a>
</p>

---

## Демонстрация работы

<video width="100%" controls src="https://github.com/user-attachments/assets/d89a6bc1-0cf6-4a1f-a29e-5e0549945e6f">
  <a href="https://github.com/user-attachments/assets/d89a6bc1-0cf6-4a1f-a29e-5e0549945e6f">Смотреть демо-видео</a>
</video>

<img width="100%" alt="2" src="https://github.com/user-attachments/assets/447c9671-3f47-4bde-a4be-744af27bdbb1" />

<img width="100%" alt="4" src="https://github.com/user-attachments/assets/8b88610b-c54b-4e3d-b7a6-2ccef7b72ca4" />

<img width="100%" alt="3" src="https://github.com/user-attachments/assets/f7052333-8107-40d8-9703-d1bbd2b77bc7" />

---

## Основная идея

Рынки краткосрочных движений BTC шумные и быстрые. Проект рассматривает их как **систематическую торговую задачу**: сбор рыночных и контекстных данных, нормализация через единый путь ingestion, слияние нескольких детекторов в решение и исполнение через брокерский адаптер с **жёсткими лимитами риска** (малый размер сделки, параметры take-profit). Цель — не «один магический сигнал», а **тестируемый стек**: сначала симуляция, наблюдение в Grafana, затем (при готовности) реальный капитал.

---

## Возможности

- **Семифазный конвейер** — внешние фиды → ingestion → ядро Nautilus → процессоры сигналов и fusion → исполнение и риск → мониторинг → feedback / обучение.
- **Мультисигнальный стек** — детекция всплесков, sentiment-подобные входы, логика дивергенции, order book и momentum-процессоры, fusion для объединения голосов.
- **Риск в приоритете** — настраиваемые лимиты (~$1 на сделку), take-profit, диапазон цены входа, фильтр спреда, блокировка направления, защита от погони за ценой.
- **Переключатель stop-loss** — `ENABLE_STOP_LOSS=false`: позиция до TP или settlement; `true` — ранний SL снова включён.
- **Порог ML edge** — ставка только если вероятность XGBoost отличается от цены Polymarket минимум на `MIN_ML_EDGE` (по умолчанию 10 п.п.).
- **Одна сделка на рынок** — `MAX_TRADES_PER_MARKET=1`: один вход на 15-минутный слот.
- **Симуляция и live** — paper/test без продакшен-ключей; live — когда готовы.
- **Операционные инструменты** — переключение режима через Redis, метрики для Grafana, просмотр paper-сделок, автоперезапуск.
- **Самообучение** — корректировка весов по результатам (см. `feedback/` и конфиг стратегии).
- **Устойчивость** — WebSocket, rate limit, валидация, патчи для Polymarket + Nautilus (Gamma, размер market-order, защита `prometheus_client` на Windows).

---

## Требования

- **Python 3.14+**
- **Redis** — переключение режимов и control-plane
- **Аккаунт Polymarket** с API для live
- **Git**

---

## Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone https://github.com/yourusername/polymarket-btc-15m-bot.git
cd polymarket-btc-15m-bot
```

### 2. Виртуальное окружение

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. Зависимости

```bash
pip install -r requirements.txt
```

### 4. Переменные окружения

```bash
cp .env.example .env
```

Отредактируйте `.env`:

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

### 5. Запуск Redis

```bash
redis-server
```

macOS (Homebrew): `brew install redis && redis-server`  
Debian/Ubuntu: `sudo apt install redis-server && redis-server`

### 6. Запуск бота

```bash
# Быстрый тест (симуляция ~раз в минуту)
python main.py --test-mode

# Обычная симуляция (15-минутные рынки)
python main.py --simulation

# Live (реальные деньги — нужны валидные ключи)
python supervisor.py --live
```

---

## Конфигурация

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `ENABLE_STOP_LOSS` | Ранний stop-loss | `false` |
| `STOP_LOSS_PCT` | Доля капитала при SL (если SL включён) | `0.50` |
| `TAKE_PROFIT_PCT` | Доля оставшегося upside для TP | `0.40` |
| `MIN_ENTRY_PRICE` | Мин. цена входа | `0.25` |
| `MAX_ENTRY_PRICE` | Макс. цена входа | `0.75` |
| `MAX_SPREAD_PCT` | Макс. спред к mid | `0.05` |
| `ENTRY_COOLDOWN_SEC` | Пауза между попытками входа (с) | `90` |
| `MAX_TRADES_PER_MARKET` | Макс. входов на 15-мин рынок | `1` |
| `LOCK_MARKET_DIRECTION` | Блокировка направления после первой сделки | `true` |
| `MAX_CHASE_DELTA` | Макс. ухудшение цены для повторного входа | `0.12` |
| `MIN_ML_EDGE` | Мин. разрыв ML vs Polymarket | `0.10` |
| `LATE_ENTRY_CUTOFF_SEC` | Запрет входа перед settlement (с) | `120` |
| `MARKET_BUY_USD` | USD на ордер | `1.00` |

Полный список — в `.env.example`.

---

## Запуск бота

- **Точка входа**: `main.py` — `--test-mode`, `--simulation`, `--live`.
- **Автоперезапуск**: `supervisor.py` в цикле для длительной работы.
- **Paper-сделки**:

```bash
python scripts/view_trades.py
```

---

## Мониторинг

- Экспорт метрик: `monitoring/`.
- Дашборды Grafana: `infra/grafana/` (импорт: `infra/grafana/import_dashboard.py`).

Подключите свой Prometheus/Grafana.

---

## Режимы торговли

Переключение simulation/live через Redis без перезапуска — `scripts/redis_control.py`.

---

## Поэтапное тестирование

Запускайте **по порядку**; следующая фаза — после успеха предыдущей.

| Фаза | Фокус | Команда |
|------|-------|---------|
| 1 | Источники данных | `python scripts/test_data_sources.py test` |
| 2 | Ingestion | `python scripts/test_ingestion.py test` |
| 3 | Ядро Nautilus | `python scripts/test_nautilus.py test` |
| 4 | Стратегия (процессоры, fusion) | `python scripts/test_strategy.py test` |
| 5 | Исполнение (риск, клиент) | `python scripts/test_execution.py test` |

Отладка Gamma API:

```bash
python scripts/debug_gamma_api.py
```

---

## Сколько денег нужно для старта?

В референсной конфигурации **~$1 на сделку**. Нужен запас на комиссии, спред и серию убытков. Многие начинают с **$10–$50**; масштабируйте только после совпадения симуляции с ожиданиями. **Не финансовый совет.**

---

## Это прибыльно?

**Прибыль не гарантируется.** Короткие рынки: комиссии, спред, adverse selection, простои. Симуляция **не** предсказывает live. Сначала paper и малый размер; каждый запуск — эксперимент.

---

## Кому подходит

- Трейдерам **15-минутных** крипто-рынков предсказаний, нужна автоматизация.
- **Разработчикам**, готовым править `.env`, читать логи и гонять фазовые тесты.
- Тем, кто ставит **риск на первое место** и хочет лимиты и observability до масштабирования.

---

## Вклад и идеи

Приветствуются fork → branch → pull request.

**Идеи:**
- Процессоры на деривативы (funding, OI).
- Новые сигналы или правила fusion.
- Алерты в Telegram/Discord.
- Лёгкий Web UI для конфига и статуса.
- ETH, SOL и другие короткие рынки Polymarket.
- Усиление ML/калибровки с честной оценкой и paper-gates.

---

## Лицензия

MIT. См. файл `LICENSE` в репозитории.

---

## Отказ от ответственности

Торговля криптовалютами и инструментами prediction markets сопряжена с **существенным риском потерь**. ПО предназначено для **обучения и исследований**. Прошлые результаты не гарантируют будущих. Авторы **не** несут ответственности за убытки. Начинайте с симуляции, малых размеров и только с капиталом, полную потерю которого вы можете принять.

---

## Благодарности

- [NautilusTrader](https://nautilustrader.io/) — торговый фреймворк
- [Polymarket](https://polymarket.com) — площадка prediction markets

<div align="center">
  <h2>Made with ❤️ by</h2>
  <a href="https://t.me/RetroValix">
    <img width="85" height="85" alt="XTLLbabR_400x400" src="https://github.com/user-attachments/assets/66c994bf-c618-40e7-a0f4-d295e09d1e91" />    <br>
    <span>Retro Valix</span>
  </a>
</div>
