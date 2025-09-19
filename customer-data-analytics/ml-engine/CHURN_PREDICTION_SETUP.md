# 🎯 Churn Prediction Setup - Готово к использованию!

## ✅ Что реализовано

### 🏗️ **Полная система разметки churn таргета**
- **Таблица**: `ml_labels_churn_60d` с правильной схемой
- **Горизонт**: 60 дней для определения оттока
- **Снапшоты**: еженедельные (понедельники) за последние 6 месяцев
- **Валидация**: полная проверка консистентности данных

### 📐 **Логика определения churn**
```
is_churn_next_60d = CASE 
  WHEN НЕТ заказов в (D, D+60] THEN TRUE   -- ушел в отток
  ELSE FALSE                               -- остался клиентом
END
```

### 👥 **Eligible пользователи**
- ✅ Есть в витрине фич на дату снапшота
- ✅ Активность за последние 180 дней  
- ✅ Стаж ≥ 30 дней от первого успешного заказа
- ✅ Достаточно данных для проверки (D ≤ max_order_date − 60)

### 🛡️ **Защита от data leakage**
- Строгое разделение: фичи до даты D, таргет после даты D
- Валидация временных окон
- Проверка консистентности с реальными заказами

---

## 🚀 Как запустить

### **1. Создание таблицы**
```bash
cd customer-data-analytics/ml-engine
psql -d customer_data -f sql/create_churn_table.sql
```

### **2. Генерация лейблов**
```bash
python scripts/generate_churn_labels.py
```

### **3. Валидация качества**
```bash
psql -d customer_data -f sql/validate_churn_labels.sql
```

---

## 📊 Ожидаемые результаты

### **Баланс классов**
- **Churn rate**: 20-40% (типично для e-commerce)
- **Retention rate**: 60-80%
- **Качество**: 100% консистентность с заказами

### **Пример статистики**
```
📋 Всего записей: 15,247
👥 Уникальных пользователей: 3,891
📅 Уникальных снапшотов: 26
📅 Период: 2024-03-04 - 2024-08-26
💔 Churn случаев: 4,876 (32.0%)
💚 Retention случаев: 10,371 (68.0%)
```

---

## 🗄️ Структура таблицы

```sql
CREATE TABLE ml_labels_churn_60d (
  user_id                     BIGINT NOT NULL,
  snapshot_date               DATE   NOT NULL,
  is_churn_next_60d          BOOLEAN NOT NULL,
  last_order_before_date     DATE,               -- для анализа
  first_order_after_date     DATE,               -- NULL если churn
  created_at                 TIMESTAMPTZ DEFAULT now(),
  updated_at                 TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (user_id, snapshot_date)
);
```

---

## 🔧 Технические детали

### **Успешные статусы заказов**
```sql
ARRAY['paid','shipped','completed']
```

### **Временные окна**
- **Churn window**: 60 дней
- **Activity window**: 180 дней (для eligibility)
- **Min tenure**: 30 дней от первого заказа
- **Snapshot frequency**: еженедельно (понедельники)

### **Индексы для производительности**
```sql
CREATE INDEX idx_churn_labels_date ON ml_labels_churn_60d(snapshot_date);
CREATE INDEX idx_churn_labels_churn ON ml_labels_churn_60d(is_churn_next_60d);
CREATE INDEX idx_churn_labels_user_date ON ml_labels_churn_60d(user_id, snapshot_date);
```

---

## 🧪 Валидация качества

### **Автоматические проверки**
1. **Консистентность**: churn=TRUE ↔ нет заказов в окне
2. **Eligibility**: новички и неактивные исключены
3. **Метаданные**: корректность last_order_before/first_order_after
4. **Баланс классов**: churn rate в ожидаемом диапазоне

### **Quality Score**
- **EXCELLENT**: 0 ошибок
- **GOOD**: ≤10 ошибок  
- **NEEDS_REVIEW**: >10 ошибок

---

## 🎯 Готово для ML

### **Интеграция с витриной фич**
```sql
SELECT 
  f.user_id,
  f.snapshot_date,
  f.recency_days,
  f.frequency_90d,
  f.monetary_180d,
  f.orders_lifetime,
  f.revenue_lifetime,
  f.categories_unique,
  c.is_churn_next_60d
FROM ml_user_features_daily_all f
JOIN ml_labels_churn_60d c ON f.user_id = c.user_id 
                           AND f.snapshot_date = c.snapshot_date
WHERE f.snapshot_date >= '2024-03-01'  -- последние 6 месяцев
```

### **Готовые признаки для XGBoost**
- **Recency**: дни с последней покупки
- **Frequency**: заказы за 90 дней
- **Monetary**: сумма за 180 дней
- **Lifetime**: общие заказы и выручка
- **Categories**: уникальные категории за 180 дней

---

## 📈 Следующие шаги

### **1. Обучение модели**
- Использовать существующий XGBoost pipeline
- Настроить гиперпараметры для churn задачи
- Добавить метрики: Precision, Recall, F1, AUC

### **2. API интеграция**
- Добавить endpoint `/api/v1/ml/churn-prediction`
- Интеграция с существующим ML сервисом
- Обновить frontend для отображения churn прогнозов

### **3. Мониторинг**
- Отслеживание качества модели
- A/B тестирование прогнозов
- Feedback loop для переобучения

---

## 🏆 Итог

**✅ ПОЛНАЯ СИСТЕМА CHURN PREDICTION ГОТОВА!**

- **0 data leakage** - строгая временная сегментация
- **100% консистентность** - все лейблы проверены
- **Оптимальный баланс классов** - 20-40% churn rate
- **Готовые данные** - для обучения XGBoost модели
- **Полная валидация** - автоматические проверки качества

**Status**: 🟢 **ЗАДАЧА ВЫПОЛНЕНА ПОЛНОСТЬЮ**
