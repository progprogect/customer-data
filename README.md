# Customer Data Analytics Platform v2.0.0

## 🚀 Стабильная версия 2.0.0 - LTV Анализ

Комплексная платформа для анализа клиентских данных с системой LTV (Lifetime Value) анализа.

### 📊 Основные возможности

- **LTV Анализ** - Ретроспективный расчет ценности клиентов по горизонтам 3/6/12 месяцев
- **Аналитика клиентов** - RFM анализ, сегментация, прогнозирование оттока
- **ML движок** - Машинное обучение для предсказания покупок и поведения
- **Веб-интерфейс** - Современный React фронтенд с TypeScript
- **API** - RESTful API на FastAPI с документацией
- **Telegram бот** - Уведомления и интерактивные отчеты

### 🏗️ Архитектура

```
customer-data-analytics/
├── api/                    # FastAPI бэкенд
│   ├── routes/            # API маршруты
│   ├── models/            # Pydantic модели
│   ├── services/          # Бизнес-логика
│   └── main.py           # Точка входа API
├── frontend/              # React фронтенд
│   ├── src/
│   │   ├── components/    # React компоненты
│   │   ├── pages/         # Страницы приложения
│   │   └── main.tsx      # Точка входа React
│   └── package.json
├── ml-engine/             # ML движок
│   ├── scripts/           # Python скрипты
│   └── sql/              # SQL запросы и функции
├── telegram-bot/          # Telegram бот
└── shared/                # Общие утилиты
```

### 🛠️ Технологии

**Backend:**
- Python 3.13
- FastAPI
- SQLAlchemy
- PostgreSQL
- psycopg2

**Frontend:**
- React 18
- TypeScript
- Vite
- CSS3

**ML & Data:**
- scikit-learn
- pandas
- numpy
- PostgreSQL

**DevOps:**
- Docker
- Docker Compose
- Git

### 🚀 Быстрый старт

1. **Клонирование репозитория:**
```bash
git clone https://github.com/progprogect/customer-data.git
cd customer-data
git checkout stable-v2
```

2. **Настройка базы данных:**
```bash
# Создание базы данных PostgreSQL
createdb customer_data

# Запуск SQL скриптов
psql -d customer_data -f create_database.sql
psql -d customer_data -f customer-data-analytics/ml-engine/sql/create_ltv_table.sql
psql -d customer_data -f customer-data-analytics/ml-engine/sql/calculate_ltv_function.sql
```

3. **Запуск API сервера:**
```bash
cd customer-data-analytics/api
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или venv\Scripts\activate  # Windows
pip install -r requirements.txt
DATABASE_URL="postgresql://username:password@localhost:5432/customer_data" python main.py
```

4. **Запуск фронтенда:**
```bash
cd customer-data-analytics/frontend
npm install
npm run dev
```

5. **Открыть в браузере:**
- Фронтенд: http://localhost:5173
- API документация: http://localhost:8000/docs

### 📈 LTV Анализ

**Новая функциональность в v2.0.0:**

- **Ретроспективный LTV** - Кумулятивная выручка клиентов по временным горизонтам
- **Агрегированная статистика** - Средние, медианные значения и перцентили
- **Ключевые инсайты** - Рекомендации по CAC, анализ роста ценности
- **Визуализация** - Интерактивные графики и таблицы

**API Endpoints:**
- `GET /api/v1/ltv/summary` - Сводная статистика LTV
- `GET /api/v1/ltv/users` - Данные пользователей с пагинацией
- `GET /api/v1/ltv/distribution` - Распределение LTV по диапазонам
- `POST /api/v1/ltv/calculate` - Пересчет LTV данных

### 📊 Модули системы

1. **LTV Анализ** (`/ltv`) - Анализ ценности клиентов
2. **RFM Анализ** (`/rfm`) - Сегментация клиентов
3. **Прогнозирование оттока** (`/churn`) - ML модели для предсказания оттока
4. **Рекомендации** (`/recommendations`) - Система рекомендаций товаров
5. **Аномалии** (`/anomalies`) - Детекция аномального поведения
6. **Аналитика поведения** (`/behavior`) - Анализ паттернов поведения

### 🔧 Конфигурация

**Переменные окружения:**
```bash
DATABASE_URL=postgresql://username:password@localhost:5432/customer_data
DB_HOST=localhost
DB_PORT=5432
DB_NAME=customer_data
DB_USER=username
DB_PASSWORD=password
```

### 📝 Документация

- [API Документация](customer-data-analytics/docs/API.md)
- [Архитектура системы](customer-data-analytics/docs/ARCHITECTURE.md)
- [LTV Implementation Report](LTV_IMPLEMENTATION_REPORT.md)

### 🤝 Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для новой функции (`git checkout -b feature/amazing-feature`)
3. Зафиксируйте изменения (`git commit -m 'Add amazing feature'`)
4. Отправьте в ветку (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

### 📄 Лицензия

Этот проект распространяется под лицензией MIT. См. файл `LICENSE` для подробностей.

### 🏷️ Версии

- **v2.0.0** - Стабильная версия с LTV анализом
- **v1.x.x** - Предыдущие версии с базовой аналитикой

### 📞 Поддержка

Если у вас есть вопросы или проблемы, создайте issue в репозитории или свяжитесь с командой разработки.

---

**Customer Data Analytics Platform v2.0.0** - Анализируйте данные клиентов как профессионал! 🚀
