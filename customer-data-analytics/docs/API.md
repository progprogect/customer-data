# API Документация

## Обзор

REST API для системы Customer Data Analytics построен на FastAPI и предоставляет эндпоинты для всех компонентов системы.

## Базовый URL

```
http://localhost:8000
```

## Аутентификация

В текущей версии аутентификация не реализована (тестовая среда).

## Общие заголовки

```http
Content-Type: application/json
Accept: application/json
```

## Коды ответов

- `200` - Успешный запрос
- `201` - Ресурс создан
- `400` - Неверный запрос
- `404` - Ресурс не найден
- `500` - Внутренняя ошибка сервера

## Формат ответов

### Успешный ответ
```json
{
  "success": true,
  "message": "Success",
  "data": { ... }
}
```

### Ошибка
```json
{
  "success": false,
  "message": "Error message",
  "error": "Detailed error information"
}
```

## Эндпоинты

### Аналитика

#### GET /api/v1/analytics/dashboard
Получение данных для дашборда.

**Параметры запроса:**
- `start_date` (optional) - начальная дата (ISO format)
- `end_date` (optional) - конечная дата (ISO format)

**Ответ:**
```json
{
  "total_users": 1000,
  "total_orders": 5000,
  "total_revenue": 150000.0,
  "active_users": 750,
  "conversion_rate": 0.15,
  "avg_order_value": 30.0,
  "top_products": [...],
  "user_growth": [...],
  "revenue_trend": [...]
}
```

#### POST /api/v1/analytics/segmentation
Сегментация пользователей.

**Тело запроса:**
```json
{
  "algorithm": "kmeans",
  "n_clusters": 5
}
```

**Ответ:**
```json
{
  "segments": [...],
  "metrics": {
    "silhouette_score": 0.5
  },
  "algorithm": "kmeans",
  "n_clusters": 5
}
```

#### POST /api/v1/analytics/churn-prediction
Предсказание оттока клиентов.

**Тело запроса:**
```json
{
  "user_ids": [1, 2, 3],
  "features": {}
}
```

**Ответ:**
```json
{
  "predictions": [...],
  "model_metrics": {
    "accuracy": 0.85
  },
  "feature_importance": [...]
}
```

#### GET /api/v1/analytics/metrics
Получение метрик.

**Параметры запроса:**
- `metric_type` - тип метрики (revenue, users, orders)
- `period` - период (7d, 30d, 90d, 1y)

#### GET /api/v1/analytics/anomaly-detection
Детекция аномалий.

**Параметры запроса:**
- `threshold` - порог для детекции (default: 0.1)

### Рекомендации

#### POST /api/v1/recommendations/
Получение рекомендаций для пользователя.

**Тело запроса:**
```json
{
  "user_id": 1,
  "n_recommendations": 5,
  "method": "hybrid"
}
```

**Ответ:**
```json
{
  "user_id": 1,
  "recommendations": [
    {
      "product_id": 1,
      "title": "Product 1",
      "score": 0.9
    }
  ],
  "method": "hybrid",
  "confidence": 0.85
}
```

#### GET /api/v1/recommendations/user/{user_id}
Получение рекомендаций для конкретного пользователя.

**Параметры запроса:**
- `n_recommendations` - количество рекомендаций (default: 5)
- `method` - метод рекомендаций (default: hybrid)

#### GET /api/v1/recommendations/similar-products/{product_id}
Получение похожих товаров.

**Параметры запроса:**
- `n_products` - количество товаров (default: 5)

#### POST /api/v1/recommendations/train
Обучение модели рекомендаций.

**Тело запроса:**
```json
{
  "method": "hybrid",
  "force_retrain": false
}
```

#### GET /api/v1/recommendations/model-status
Получение статуса модели рекомендаций.

### Пользователи

#### GET /api/v1/users/
Получение списка пользователей.

**Параметры запроса:**
- `skip` - количество пропускаемых записей (default: 0)
- `limit` - максимальное количество записей (default: 100)
- `country` - фильтр по стране
- `city` - фильтр по городу

#### GET /api/v1/users/{user_id}
Получение информации о пользователе.

#### GET /api/v1/users/{user_id}/profile
Получение профиля пользователя с аналитикой.

#### GET /api/v1/users/{user_id}/orders
Получение заказов пользователя.

**Параметры запроса:**
- `skip` - количество пропускаемых записей (default: 0)
- `limit` - максимальное количество записей (default: 50)

#### GET /api/v1/users/{user_id}/events
Получение событий пользователя.

**Параметры запроса:**
- `event_type` - тип события
- `start_date` - начальная дата
- `end_date` - конечная дата
- `skip` - количество пропускаемых записей (default: 0)
- `limit` - максимальное количество записей (default: 100)

#### GET /api/v1/users/{user_id}/ltv
Расчет пожизненной ценности клиента.

#### GET /api/v1/users/segments/
Получение сегментов пользователей.

### Товары

#### GET /api/v1/products/
Получение списка товаров.

**Параметры запроса:**
- `skip` - количество пропускаемых записей (default: 0)
- `limit` - максимальное количество записей (default: 100)
- `category` - фильтр по категории
- `brand` - фильтр по бренду
- `min_price` - минимальная цена
- `max_price` - максимальная цена

#### GET /api/v1/products/{product_id}
Получение информации о товаре.

#### GET /api/v1/products/{product_id}/profile
Получение профиля товара.

#### GET /api/v1/products/{product_id}/price-history
Получение истории цен товара.

**Параметры запроса:**
- `days` - количество дней истории (default: 30)

#### GET /api/v1/products/categories/
Получение списка категорий.

#### GET /api/v1/products/brands/
Получение списка брендов.

#### GET /api/v1/products/popular/
Получение популярных товаров.

**Параметры запроса:**
- `period` - период популярности (default: 30d)
- `limit` - количество товаров (default: 10)

#### GET /api/v1/products/search/
Поиск товаров.

**Параметры запроса:**
- `query` - поисковый запрос
- `limit` - максимальное количество результатов (default: 20)

## Примеры использования

### Получение данных дашборда
```bash
curl -X GET "http://localhost:8000/api/v1/analytics/dashboard?start_date=2024-01-01&end_date=2024-01-31"
```

### Сегментация пользователей
```bash
curl -X POST "http://localhost:8000/api/v1/analytics/segmentation" \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "kmeans", "n_clusters": 5}'
```

### Получение рекомендаций
```bash
curl -X POST "http://localhost:8000/api/v1/recommendations/" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "n_recommendations": 5, "method": "hybrid"}'
```

### Поиск товаров
```bash
curl -X GET "http://localhost:8000/api/v1/products/search/?query=iphone&limit=10"
```

## Ограничения

### Rate Limiting
- 1000 запросов в минуту на IP
- 100 запросов в минуту на пользователя

### Размеры запросов
- Максимальный размер тела запроса: 10MB
- Максимальное количество элементов в массиве: 1000

### Таймауты
- Таймаут запроса: 30 секунд
- Таймаут подключения к БД: 10 секунд

## Обработка ошибок

### Валидация данных
```json
{
  "success": false,
  "message": "Validation error",
  "error": {
    "field": "user_id",
    "message": "Field is required"
  }
}
```

### Ошибки сервера
```json
{
  "success": false,
  "message": "Internal server error",
  "error": "Database connection failed"
}
```

## Swagger UI

Интерактивная документация API доступна по адресу:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
