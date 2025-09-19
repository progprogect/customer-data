# 🚀 Churn Prediction - Быстрый старт

## ⚡ Запуск в 3 команды

```bash
# 1. Перейти в директорию ML Engine
cd customer-data-analytics/ml-engine

# 2. Создать таблицу
psql -d customer_data -f sql/create_churn_table.sql

# 3. Запустить генерацию и валидацию
python scripts/generate_churn_labels.py
```

## 📊 Что получите

- ✅ Таблица `ml_labels_churn_60d` с разметкой оттока
- ✅ Еженедельные снапшоты за 6 месяцев  
- ✅ Горизонт предсказания: 60 дней
- ✅ Баланс классов: ~30% churn, ~70% retention
- ✅ 100% консистентность с реальными заказами

## 🎯 Готовые данные для ML

```sql
-- Объединение фич и таргета
SELECT f.*, c.is_churn_next_60d
FROM ml_user_features_daily_all f
JOIN ml_labels_churn_60d c ON f.user_id = c.user_id 
                           AND f.snapshot_date = c.snapshot_date;
```

**Готово для обучения XGBoost модели!** 🎉
