# 🚀 Customer Data Analytics - Stable Version 3.0

## 📋 Обзор проекта

Полнофункциональная система аналитики клиентских данных с Telegram-ботом для маркетинга, включающая:

- **Frontend**: React-приложение с дашбордом
- **Backend**: FastAPI с PostgreSQL
- **ML Engine**: Модели предсказания покупок и оттока
- **Telegram Bot**: Карманный дашборд для маркетинга

## 🎯 Ключевые возможности Stable Version 3.0

### ✅ Telegram Marketing Assistant
- **Поиск пользователей** по ID с полной аналитикой
- **Реальные данные** из PostgreSQL (без моков)
- **ML предсказания**: вероятность покупки и риск оттока
- **Сегментация**: Новые/Неактивные, Обычные, VIP
- **Рекомендации товаров** на основе ML
- **Обнаружение аномалий** в поведении

### ✅ Техническая архитектура
- **Прямые запросы к БД** для гарантии актуальности данных
- **Rate limiting** для предотвращения спама
- **Обработка ошибок** с информативными сообщениями
- **HTML-форматирование** для красивого отображения
- **Логирование** всех операций

## 🛠 Установка и запуск

### Предварительные требования
- Python 3.13+
- PostgreSQL 14+
- Node.js 18+

### 1. Backend API
```bash
cd customer-data-analytics/api
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или venv\Scripts\activate  # Windows
pip install -r requirements.txt
export DATABASE_URL="postgresql://user:pass@localhost:5432/customer_data"
python main.py
```

### 2. Telegram Bot
```bash
cd customer-data-analytics/telegram-bot
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# Настройте BOT_TOKEN в .env файле
python simple_bot.py
```

### 3. Frontend
```bash
cd customer-data-analytics/frontend
npm install
npm run dev
```

## 📊 Структура данных

### Пользователи
- **LTV (12 месяцев)**: Общая выручка от пользователя
- **Сегменты**: Автоматическая кластеризация по поведению
- **ML предсказания**: Вероятность покупки (30 дней), риск оттока (60 дней)

### API Endpoints
- `GET /api/v1/telegram-db/user-summary/{user_id}` - Полная сводка пользователя
- `GET /api/v1/segments/distribution` - Распределение по сегментам
- `GET /api/v1/direct-users` - Пользователи с ML предсказаниями

## 🔧 Конфигурация

### Environment Variables
```bash
# Telegram Bot
BOT_TOKEN=your_telegram_bot_token
API_KEY=dev-token-12345
API_URL=http://localhost:8000

# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/customer_data
```

## 📈 Возможности Telegram Bot

### Главное меню
- 👥 **Сегментация** - анализ распределения пользователей
- 🛒 **Вероятность покупки** - пользователи с высокой вероятностью
- 🔍 **Поиск пользователя** - детальная аналитика по ID
- ⚠️ **Риск оттока** - пользователи с высоким риском
- 🎯 **Рекомендации** - товары для пользователей
- 🚨 **Аномалии** - необычное поведение
- 💰 **Эластичность цен** - анализ ценовой чувствительности

### Пример использования
```
👤 Пользователь #123
🎯 Сегмент: VIP
💰 LTV(12м): $1,944.10

📅 Последняя покупка: 4 дня назад
🛒 Вероятность покупки (30д): 10.0%
⚠️ Риск оттока (60д): 80.0%
🎯 Рекомендации: BrandI Accessorie #2, BrandC Smartphone #3
🚨 Аномалии (90д): нет
```

## 🔄 Обновления в версии 3.0

### ✅ Исправления
- Убраны строки "нет данных" для ML предсказаний
- Исправлены названия сегментов
- Удалены лишние кнопки из меню
- Улучшено форматирование таблиц
- Добавлен прямой доступ к базе данных

### ✅ Новые возможности
- Полная интеграция с ML моделями
- Реальные данные вместо моков
- Оптимизированная производительность
- Улучшенная обработка ошибок

## 📝 Лицензия

MIT License

## 🤝 Поддержка

Для вопросов и предложений создавайте Issues в репозитории.

---

**Stable Version 3.0** - Готова к продакшену! 🎉
