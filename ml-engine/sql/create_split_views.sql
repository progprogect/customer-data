-- =========================
-- Create Split Views (train/valid/test)
-- =========================
-- Создание материализованных представлений для каждого сплита

-- Train set view
CREATE OR REPLACE VIEW ml_train_set AS
SELECT 
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  aov_180d,
  orders_lifetime,
  revenue_lifetime,
  categories_unique,
  purchase_next_30d,
  split
FROM ml_training_dataset 
WHERE split = 'train';

-- Valid set view
CREATE OR REPLACE VIEW ml_valid_set AS
SELECT 
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  aov_180d,
  orders_lifetime,
  revenue_lifetime,
  categories_unique,
  purchase_next_30d,
  split
FROM ml_training_dataset 
WHERE split = 'valid';

-- Test set view
CREATE OR REPLACE VIEW ml_test_set AS
SELECT 
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  aov_180d,
  orders_lifetime,
  revenue_lifetime,
  categories_unique,
  purchase_next_30d,
  split
FROM ml_training_dataset 
WHERE split = 'test';

-- Комментарии к представлениям
COMMENT ON VIEW ml_train_set IS 'Тренировочный набор (73.9% данных, 2025-03-17 - 2025-07-07)';
COMMENT ON VIEW ml_valid_set IS 'Валидационный набор (13.0% данных, 2025-07-14 - 2025-07-28)';
COMMENT ON VIEW ml_test_set IS 'Тестовый набор (13.0% данных, 2025-08-04 - 2025-08-18)';

-- Проверяем создание представлений
SELECT 
  'ml_train_set' as view_name,
  COUNT(*) as row_count,
  COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END) as positive_count
FROM ml_train_set

UNION ALL

SELECT 
  'ml_valid_set' as view_name,
  COUNT(*) as row_count,
  COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END) as positive_count
FROM ml_valid_set

UNION ALL

SELECT 
  'ml_test_set' as view_name,
  COUNT(*) as row_count,
  COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END) as positive_count
FROM ml_test_set;
