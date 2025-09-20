# 🚨 Система детекции аномалий поведения пользователей

## Обзор

Система детекции аномалий позволяет выявлять необычные паттерны в поведении пользователей на основе недельного анализа их активности. Система использует статистические методы (z-score, ratio analysis) для обнаружения всплесков и провалов в покупательской активности.

## 🏗️ Архитектура

### Backend (API)
- **FastAPI** - REST API для управления аномалиями
- **PostgreSQL** - хранение данных и выполнение аналитических запросов
- **SQLAlchemy** - ORM для работы с базой данных
- **Pydantic** - валидация данных

### Frontend (Dashboard)
- **React + TypeScript** - пользовательский интерфейс
- **Recharts** - визуализация данных
- **Axios** - HTTP клиент для API
- **React Router** - навигация

### ML Engine
- **PostgreSQL Functions** - алгоритмы детекции аномалий
- **Window Functions** - скользящие средние и статистики
- **SQL Analytics** - агрегация и анализ данных

## 📊 Компоненты системы

### 1. Недельная витрина поведения (`ml_user_behavior_weekly`)

Агрегирует поведение пользователей по неделям:

```sql
CREATE TABLE ml_user_behavior_weekly (
  user_id            BIGINT NOT NULL,
  week_start_date    DATE   NOT NULL,
  orders_count       INT    NOT NULL DEFAULT 0,
  monetary_sum       NUMERIC(12,2) NOT NULL DEFAULT 0,
  categories_count   INT    NOT NULL DEFAULT 0,
  aov_weekly         NUMERIC(12,2),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, week_start_date)
);
```

**Метрики:**
- `orders_count` - количество заказов за неделю
- `monetary_sum` - сумма покупок за неделю
- `categories_count` - количество уникальных категорий
- `aov_weekly` - средний чек за неделю

### 2. Таблица аномалий (`ml_user_anomalies_weekly`)

Хранит результаты детекции аномалий:

```sql
CREATE TABLE ml_user_anomalies_weekly (
  user_id        BIGINT NOT NULL,
  week_start     DATE   NOT NULL,
  anomaly_score  NUMERIC NOT NULL,
  is_anomaly     BOOLEAN NOT NULL,
  triggers       TEXT[]  NOT NULL,
  insufficient_history BOOLEAN NOT NULL DEFAULT FALSE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, week_start)
);
```

**Поля:**
- `anomaly_score` - максимальный z-score или ln(ratio)
- `is_anomaly` - флаг аномалии
- `triggers` - массив сработавших правил
- `insufficient_history` - недостаточно истории (< 4 недель)

## 🧮 Алгоритм детекции

### Метрики динамики
- **Δorders** = orders_count - lag(orders_count, 1)
- **Δmonetary** = monetary_sum - lag(monetary_sum, 1)
- **ratio_orders** = orders_count / NULLIF(lag(orders_count, 1), 0)
- **ratio_monetary** = monetary_sum / NULLIF(lag(monetary_sum, 1), 0)

### Скользящие средние (4 недели)
- **mean_orders_4w** - среднее количество заказов
- **std_orders_4w** - стандартное отклонение заказов
- **mean_monetary_4w** - средняя сумма покупок
- **std_monetary_4w** - стандартное отклонение суммы

### Z-score расчет
- **z_orders** = (orders_count - mean_orders_4w) / std_orders_4w
- **z_monetary** = (monetary_sum - mean_monetary_4w) / std_monetary_4w

### Правила аномалий
1. **Всплеск заказов**: z_orders ≥ 3 ИЛИ ratio_orders ≥ 3.0
2. **Провал заказов**: z_orders ≤ -3 ИЛИ (lag_orders > 0 И orders = 0)
3. **Всплеск трат**: z_monetary ≥ 3 ИЛИ ratio_monetary ≥ 3.0
4. **Провал трат**: z_monetary ≤ -3

## 🚀 API Endpoints

### Получение аномалий за неделю
```http
GET /api/v1/anomalies/weekly?date=2025-09-15&min_score=3&limit=50
```

**Ответ:**
```json
{
  "anomalies": [
    {
      "user_id": 253,
      "week_start": "2025-09-15",
      "anomaly_score": 87.0868,
      "is_anomaly": true,
      "triggers": ["z_monetary>=3", "ratio_monetary>=3"],
      "insufficient_history": false
    }
  ],
  "total_count": 33,
  "week_date": "2025-09-15",
  "summary_stats": {
    "total_anomalies": 33,
    "avg_score": 14.37,
    "max_score": 99.73,
    "unique_users": 33
  }
}
```

### История аномалий пользователя
```http
GET /api/v1/anomalies/user/253?weeks=12&min_score=0
```

### Статистика аномалий
```http
GET /api/v1/anomalies/stats
```

### Запуск детекции
```http
POST /api/v1/anomalies/detect
```

## 🎨 Frontend Dashboard

### Страница аномалий (`/anomalies`)

**Компоненты:**
- **AnomaliesTable** - таблица с аномалиями и пагинацией
- **UserAnomalyChart** - модальное окно с графиком пользователя
- **AlgorithmInfo** - описание алгоритма детекции

**Функциональность:**
- Сортировка по anomaly_score (DESC)
- Пагинация по 50 строк
- Клик по пользователю → детализация
- Интерактивные графики с Recharts
- Подсветка аномальных недель
- Tooltip с детальной информацией

### Навигация
- Добавлена карточка "Аномалии" на главную страницу
- Интеграция с существующим дизайном
- Модальные окна для детализации

## 🛠️ Установка и запуск

### 1. Создание таблиц
```bash
# Подключиться к PostgreSQL
psql -d customer_data

# Выполнить SQL скрипты
\i customer-data-analytics/ml-engine/sql/create_behavior_weekly_table.sql
\i customer-data-analytics/ml-engine/sql/create_anomalies_weekly_table.sql

# Заполнить данные
SELECT populate_behavior_weekly();
SELECT detect_anomalies_weekly();
```

### 2. Запуск API сервера
```bash
cd customer-data-analytics/api
source venv/bin/activate
python main.py
```

### 3. Запуск Frontend
```bash
cd customer-data-analytics/frontend
npm install
npm run dev
```

## 📈 Мониторинг и аналитика

### Ключевые метрики
- **Процент аномалий** - доля аномальных недель от общего числа
- **Топ пользователи** - пользователи с наибольшим количеством аномалий
- **Топ триггеры** - наиболее часто срабатывающие правила
- **Распределение по неделям** - динамика аномалий во времени

### Логирование
- Все API запросы логируются
- Ошибки детекции записываются в лог
- Статистика выполнения функций

## 🔧 Конфигурация

### Переменные окружения
```env
DATABASE_URL=postgresql://user@localhost:5432/customer_data
```

### Настройки алгоритма
- **Минимальная история**: 4 недели
- **Окно скользящего среднего**: 4 недели
- **Пороги z-score**: ±3
- **Пороги ratio**: 3.0
- **Период анализа**: 6 месяцев

## 🚀 Развертывание

### Production
1. Настроить PostgreSQL с оптимизированными индексами
2. Запустить API сервер с Gunicorn
3. Собрать и развернуть React приложение
4. Настроить мониторинг и алерты

### Docker (планируется)
- Контейнеризация всех компонентов
- Docker Compose для локальной разработки
- Kubernetes для production

## 📚 Документация

- [API Documentation](customer-data-analytics/docs/API.md)
- [Architecture Overview](customer-data-analytics/docs/ARCHITECTURE.md)
- [Frontend Integration Guide](FRONTEND_INTEGRATION_GUIDE.md)

## 🤝 Вклад в проект

1. Fork репозитория
2. Создать feature ветку
3. Внести изменения
4. Создать Pull Request

## 📄 Лицензия

Проект распространяется под лицензией MIT.

---

**Система детекции аномалий** - мощный инструмент для анализа поведения пользователей и выявления необычных паттернов в их покупательской активности. 🚀
