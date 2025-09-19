-- =========================
-- Create Churn Training Dataset
-- =========================
-- Создание тренировочного датасета для churn prediction
-- Объединение фич и таргета с time-based split

-- ========= ПАРАМЕТРЫ =========
WITH params AS (
  SELECT
    0.7 ::numeric AS train_split_ratio,    -- 70% для train
    0.3 ::numeric AS valid_test_ratio      -- 30% для valid/test
),

-- ========= ОБЪЕДИНЕНИЕ ФИЧ И ТАРГЕТА =========
features_with_target AS (
  SELECT 
    -- Идентификаторы
    f.user_id,
    f.snapshot_date,
    
    -- RFM признаки
    f.recency_days,                    -- дни с последней покупки
    f.frequency_90d,                   -- заказы за 90 дней
    f.monetary_180d,                   -- сумма за 180 дней
    f.aov_180d,                        -- средний чек за 180 дней
    f.orders_lifetime,                 -- общие заказы
    f.revenue_lifetime,                -- общая выручка
    f.categories_unique,               -- уникальные категории за 180 дней
    
    -- Таргет
    c.is_churn_next_60d AS target,
    
    -- Метаданные для анализа
    c.last_order_before_date,
    c.first_order_after_date,
    
    -- Системные поля
    f.snapshot_date::timestamp AS created_at
    
  FROM ml_user_features_daily_all f
  INNER JOIN ml_labels_churn_60d c ON f.user_id = c.user_id 
                                   AND f.snapshot_date = c.snapshot_date
  
  -- Фильтруем только валидные записи
  WHERE f.snapshot_date IS NOT NULL
    AND c.is_churn_next_60d IS NOT NULL
),

-- ========= ПРИМЕНЕНИЕ TIME-BASED SPLIT =========
dataset_with_splits AS (
  SELECT 
    f.*,
    -- Простой time-based split: первые 70% снапшотов = train, остальные = valid_test
    CASE 
      WHEN f.snapshot_date <= '2025-06-02' THEN 'train'  -- Примерно 70% от общего периода
      ELSE 'valid_test'
    END AS split_type,
    
    -- Дополнительные метрики для анализа
    '2025-03-17'::date AS min_date,
    '2025-07-14'::date AS max_date,
    '2025-06-02'::date AS train_valid_boundary,
    18 AS total_snapshots
    
  FROM features_with_target f
),

-- ========= ФИНАЛЬНАЯ АГРЕГАЦИЯ С СТАТИСТИКОЙ =========
final_dataset AS (
  SELECT 
    d.*,
    -- Статистика по split'ам
    COUNT(*) OVER (PARTITION BY d.split_type) AS split_count,
    COUNT(*) OVER () AS total_records,
    
    -- Проверка на NULL значения
    CASE 
      WHEN d.recency_days IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.frequency_90d IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.monetary_180d IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.aov_180d IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.orders_lifetime IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.revenue_lifetime IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.categories_unique IS NULL THEN 1 ELSE 0 
    END + 
    CASE 
      WHEN d.target IS NULL THEN 1 ELSE 0 
    END AS null_count
  FROM dataset_with_splits d
)

-- ========= ВСТАВКА В ТАБЛИЦУ ТРЕНИРОВОЧНОГО ДАТАСЕТА =========
INSERT INTO ml_training_dataset_churn (
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  aov_180d,
  orders_lifetime,
  revenue_lifetime,
  categories_unique,
  target,
  split_type,
  last_order_before_date,
  first_order_after_date,
  created_at
)
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
  target,
  split_type,
  last_order_before_date,
  first_order_after_date,
  created_at
FROM final_dataset
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  recency_days = EXCLUDED.recency_days,
  frequency_90d = EXCLUDED.frequency_90d,
  monetary_180d = EXCLUDED.monetary_180d,
  aov_180d = EXCLUDED.aov_180d,
  orders_lifetime = EXCLUDED.orders_lifetime,
  revenue_lifetime = EXCLUDED.revenue_lifetime,
  categories_unique = EXCLUDED.categories_unique,
  target = EXCLUDED.target,
  split_type = EXCLUDED.split_type,
  last_order_before_date = EXCLUDED.last_order_before_date,
  first_order_after_date = EXCLUDED.first_order_after_date,
  created_at = EXCLUDED.created_at;

-- ========= ЛОГИРОВАНИЕ СТАТИСТИКИ =========
DO $$
DECLARE
    total_records INTEGER;
    train_records INTEGER;
    valid_test_records INTEGER;
    churn_count INTEGER;
    retention_count INTEGER;
    churn_rate NUMERIC;
    null_records INTEGER;
    split_boundary_date DATE;
BEGIN
    -- Получаем статистику
    SELECT 
      COUNT(*),
      COUNT(*) FILTER (WHERE split_type = 'train'),
      COUNT(*) FILTER (WHERE split_type = 'valid_test'),
      COUNT(*) FILTER (WHERE target = TRUE),
      COUNT(*) FILTER (WHERE target = FALSE),
      COUNT(*) FILTER (WHERE 
        recency_days IS NULL OR frequency_90d IS NULL OR 
        monetary_180d IS NULL OR aov_180d IS NULL OR 
        orders_lifetime IS NULL OR revenue_lifetime IS NULL OR 
        categories_unique IS NULL),
      '2025-06-02'::date
    INTO total_records, train_records, valid_test_records, churn_count, retention_count, null_records, split_boundary_date
    FROM ml_training_dataset_churn;
    
    -- Вычисляем churn rate
    churn_rate := (churn_count::NUMERIC / total_records::NUMERIC) * 100;
    
    -- Логируем результаты
    RAISE NOTICE '=========================';
    RAISE NOTICE 'CHURN TRAINING DATASET CREATED';
    RAISE NOTICE '=========================';
    RAISE NOTICE 'Total records: %', total_records;
    RAISE NOTICE 'Train records: % (%.1f%%)', train_records, (train_records::NUMERIC / total_records::NUMERIC) * 100;
    RAISE NOTICE 'Valid/Test records: % (%.1f%%)', valid_test_records, (valid_test_records::NUMERIC / total_records::NUMERIC) * 100;
    RAISE NOTICE 'Split boundary: %', split_boundary_date;
    RAISE NOTICE 'Churn cases: % (%.1f%%)', churn_count, churn_rate;
    RAISE NOTICE 'Retention cases: % (%.1f%%)', retention_count, 100 - churn_rate;
    RAISE NOTICE 'Records with NULLs: %', null_records;
    RAISE NOTICE '=========================';
    
    -- Проверяем качество данных
    IF null_records > total_records * 0.05 THEN
        RAISE WARNING 'WARNING: %.1f%% records have NULL values (threshold: 5%%)', (null_records::NUMERIC / total_records::NUMERIC) * 100;
    ELSE
        RAISE NOTICE 'INFO: NULL rate (%.1f%%) is acceptable', (null_records::NUMERIC / total_records::NUMERIC) * 100;
    END IF;
    
    IF churn_rate < 15 OR churn_rate > 50 THEN
        RAISE WARNING 'WARNING: Churn rate (%.1f%%) outside expected range (15-50%%)', churn_rate;
    ELSE
        RAISE NOTICE 'INFO: Churn rate (%.1f%%) is within expected range', churn_rate;
    END IF;
    
    RAISE NOTICE '=========================';
END $$;
