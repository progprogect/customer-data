# Customer Data Analytics

Система аналитики и персонализированных рекомендаций с многоканальным сбором данных, машинным обучением и автоматизированными рекомендациями через Telegram-бота.

## 🏗️ Архитектура проекта

```
customer-data-analytics/
├── ml-engine/              # ML модели и обучение
│   ├── models/             # ML модели
│   ├── data/               # Данные для обучения
│   ├── training/           # Скрипты обучения
│   └── utils/              # Утилиты ML
├── api/                    # REST API (FastAPI)
│   ├── routes/             # API маршруты
│   ├── models/             # Pydantic модели
│   ├── services/           # Бизнес-логика
│   └── middleware/         # Промежуточное ПО
├── frontend/               # React дашборд
│   ├── src/
│   │   ├── components/     # React компоненты
│   │   ├── services/       # API сервисы
│   │   └── utils/          # Утилиты фронтенда
│   └── public/             # Статические файлы
├── telegram-bot/           # Telegram бот
│   ├── handlers/           # Обработчики команд
│   ├── utils/              # Утилиты бота
│   └── keyboards/          # Клавиатуры
├── shared/                 # Общие компоненты
│   ├── database/           # Подключение к БД
│   ├── utils/              # Общие утилиты
│   └── types/              # Общие типы
├── config/                 # Конфигурация
└── docs/                   # Документация
```

## 🚀 Быстрый старт

### Предварительные требования

- Python 3.8+
- Node.js 16+
- PostgreSQL 15+
- Redis (опционально)

### Установка

1. **Клонирование репозитория**
```bash
git clone <repository-url>
cd customer-data-analytics
```

2. **Настройка базы данных**
```bash
# Создание базы данных
createdb customer_data

# Выполнение SQL скрипта
psql -d customer_data -f create_database.sql
```

3. **Настройка переменных окружения**
```bash
# Копирование примера конфигурации
cp config/env.example .env

# Редактирование конфигурации
nano .env
```

4. **Установка зависимостей**

**ML Engine:**
```bash
cd ml-engine
pip install -r requirements.txt
```

**API:**
```bash
cd api
pip install -r requirements.txt
```

**Frontend:**
```bash
cd frontend
npm install
```

**Telegram Bot:**
```bash
cd telegram-bot
pip install -r requirements.txt
```

### Запуск

1. **API сервер**
```bash
cd api
python main.py
```

2. **Frontend**
```bash
cd frontend
npm start
```

3. **Telegram Bot**
```bash
cd telegram-bot
python main.py
```

## 📊 Функциональность

### ML Engine
- **Сегментация пользователей** - K-means, DBSCAN
- **Предиктивная модель** - Random Forest, XGBoost
- **Рекомендательная система** - Collaborative + Content-based
- **Churn prediction** - предсказание оттока клиентов
- **Price elasticity** - анализ чувствительности к цене

### API (FastAPI)
- **REST API** для всех компонентов системы
- **Автодокументация** - Swagger UI
- **Валидация данных** - Pydantic
- **Асинхронность** - async/await

### Frontend (React)
- **Интерактивный дашборд** с графиками
- **Адаптивный дизайн** для мобильных устройств
- **Real-time обновления** данных
- **Компонентная архитектура**

### Telegram Bot
- **Богатый интерфейс** с inline кнопками
- **Callback обработчики** для навигации
- **Интеграция с API** для получения данных
- **Удобная навигация** по разделам

## 🔧 Конфигурация

### Переменные окружения

```env
# Environment
ENVIRONMENT=development
DEBUG=true

# Database
DATABASE_URL=postgresql://username:password@localhost:5432/customer_data

# API
API_HOST=0.0.0.0
API_PORT=8000
API_URL=http://localhost:8000

# Frontend
FRONTEND_URL=http://localhost:3000

# Telegram Bot
BOT_TOKEN=your_bot_token_here

# Logging
LOG_LEVEL=INFO
LOG_DIR=./logs

# ML Engine
ML_MODELS_DIR=./ml-engine/models
ML_DATA_DIR=./ml-engine/data

# Redis
REDIS_URL=redis://localhost:6379/0
```

## 📚 API Документация

После запуска API сервера документация доступна по адресам:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Основные эндпоинты

- `GET /api/v1/analytics/dashboard` - данные дашборда
- `POST /api/v1/analytics/segmentation` - сегментация пользователей
- `POST /api/v1/analytics/churn-prediction` - предсказание оттока
- `GET /api/v1/recommendations/user/{user_id}` - рекомендации
- `GET /api/v1/users/` - список пользователей
- `GET /api/v1/products/` - список товаров

## 🤖 Telegram Bot

### Команды

- `/start` - главное меню
- `/analytics` - раздел аналитики
- `/recommendations` - рекомендации
- `/users` - управление пользователями
- `/products` - каталог товаров

### Навигация

Бот использует inline клавиатуры для удобной навигации:
- 📊 **Аналитика** - дашборд, сегментация, предсказание оттока
- 🎯 **Рекомендации** - персонализированные рекомендации
- 👥 **Пользователи** - управление пользователями
- 📦 **Товары** - каталог и поиск товаров

## 🗄️ База данных

### Основные таблицы

- `users` - пользователи
- `products` - товары
- `orders` - заказы
- `order_items` - позиции заказов
- `user_events` - события поведения
- `product_prices` - история цен

### Индексы

- B-tree индексы для основных полей
- GIN индексы для JSONB и массивов
- Составные индексы для оптимизации запросов

## 🧪 Тестирование

### ML модели
```bash
cd ml-engine
python -m pytest tests/
```

### API
```bash
cd api
python -m pytest tests/
```

### Frontend
```bash
cd frontend
npm test
```

## 📈 Мониторинг

### Логирование
- Консольный вывод
- Файловые логи с ротацией
- Разные уровни логирования

### Метрики
- Производительность API
- Использование ML моделей
- Активность пользователей

## 🚀 Развертывание

### Production
1. Настройка переменных окружения
2. Настройка базы данных
3. Запуск сервисов
4. Настройка мониторинга

### Docker (опционально)
```bash
docker-compose up -d
```

## 🤝 Разработка

### Структура кода
- **Модульная архитектура** - четкое разделение ответственности
- **Типизация** - TypeScript для фронтенда, Pydantic для API
- **Документация** - docstrings и комментарии
- **Тестирование** - unit и integration тесты

### Git workflow
1. Создание feature ветки
2. Разработка функциональности
3. Тестирование
4. Code review
5. Merge в main

## 📝 Лицензия

MIT License

## 👥 Команда

- **System Architect** - проектирование архитектуры
- **ML Engineer** - машинное обучение
- **Backend Developer** - API и база данных
- **Frontend Developer** - пользовательский интерфейс
- **DevOps Engineer** - развертывание и мониторинг

## 📞 Поддержка

Для вопросов и поддержки:
- Создайте issue в репозитории
- Обратитесь к команде разработки
- Проверьте документацию API

---

**Версия**: 1.0.0  
**Последнее обновление**: 2024-01-01

