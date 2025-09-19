-- =========================
-- Analyze Churn Training Dataset
-- =========================
-- Анализ тренировочного датасета для churn prediction

-- ========= ОСНОВНАЯ СТАТИСТИКА =========
WITH basic_stats AS (
  SELECT 
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE split_type = 'train') AS train_records,
    COUNT(*) FILTER (WHERE split_type = 'valid_test') AS valid_test_records,
    COUNT(*) FILTER (WHERE target = TRUE) AS churn_count,
    COUNT(*) FILTER (WHERE target = FALSE) AS retention_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT snapshot_date) AS unique_snapshots,
    MIN(snapshot_date) AS earliest_snapshot,
    MAX(snapshot_date) AS latest_snapshot
  FROM ml_training_dataset_churn
),

-- ========= СТАТИСТИКА ПО SPLIT'АМ =========
split_stats AS (
  SELECT 
    split_type,
    COUNT(*) AS records_count,
    COUNT(*) FILTER (WHERE target = TRUE) AS churn_count,
    COUNT(*) FILTER (WHERE target = FALSE) AS retention_count,
    (COUNT(*) FILTER (WHERE target = TRUE)::NUMERIC / COUNT(*)::NUMERIC) * 100 AS churn_rate_percent,
    MIN(snapshot_date) AS earliest_date,
    MAX(snapshot_date) AS latest_date
  FROM ml_training_dataset_churn
  GROUP BY split_type
),

-- ========= СТАТИСТИКА ПРИЗНАКОВ =========
feature_stats AS (
  SELECT 
    -- Recency
    COUNT(*) FILTER (WHERE recency_days IS NULL) AS recency_nulls,
    AVG(recency_days) AS recency_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY recency_days) AS recency_median,
    MIN(recency_days) AS recency_min,
    MAX(recency_days) AS recency_max,
    
    -- Frequency 90d
    COUNT(*) FILTER (WHERE frequency_90d IS NULL) AS frequency_nulls,
    AVG(frequency_90d) AS frequency_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY frequency_90d) AS frequency_median,
    MIN(frequency_90d) AS frequency_min,
    MAX(frequency_90d) AS frequency_max,
    
    -- Monetary 180d
    COUNT(*) FILTER (WHERE monetary_180d IS NULL) AS monetary_nulls,
    AVG(monetary_180d) AS monetary_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monetary_180d) AS monetary_median,
    MIN(monetary_180d) AS monetary_min,
    MAX(monetary_180d) AS monetary_max,
    
    -- AOV 180d
    COUNT(*) FILTER (WHERE aov_180d IS NULL) AS aov_nulls,
    AVG(aov_180d) AS aov_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY aov_180d) AS aov_median,
    MIN(aov_180d) AS aov_min,
    MAX(aov_180d) AS aov_max,
    
    -- Orders Lifetime
    COUNT(*) FILTER (WHERE orders_lifetime IS NULL) AS orders_lifetime_nulls,
    AVG(orders_lifetime) AS orders_lifetime_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders_lifetime) AS orders_lifetime_median,
    MIN(orders_lifetime) AS orders_lifetime_min,
    MAX(orders_lifetime) AS orders_lifetime_max,
    
    -- Revenue Lifetime
    COUNT(*) FILTER (WHERE revenue_lifetime IS NULL) AS revenue_lifetime_nulls,
    AVG(revenue_lifetime) AS revenue_lifetime_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_lifetime) AS revenue_lifetime_median,
    MIN(revenue_lifetime) AS revenue_lifetime_min,
    MAX(revenue_lifetime) AS revenue_lifetime_max,
    
    -- Categories Unique
    COUNT(*) FILTER (WHERE categories_unique IS NULL) AS categories_unique_nulls,
    AVG(categories_unique) AS categories_unique_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY categories_unique) AS categories_unique_median,
    MIN(categories_unique) AS categories_unique_min,
    MAX(categories_unique) AS categories_unique_max
    
  FROM ml_training_dataset_churn
),

-- ========= КОРРЕЛЯЦИЯ ПРИЗНАКОВ С ТАРГЕТОМ =========
feature_target_correlation AS (
  SELECT 
    -- Корреляция с churn (TRUE = 1, FALSE = 0)
    CORR(COALESCE(recency_days, 999), target::int) AS recency_churn_corr,
    CORR(frequency_90d, target::int) AS frequency_churn_corr,
    CORR(monetary_180d, target::int) AS monetary_churn_corr,
    CORR(COALESCE(aov_180d, 0), target::int) AS aov_churn_corr,
    CORR(orders_lifetime, target::int) AS orders_lifetime_churn_corr,
    CORR(revenue_lifetime, target::int) AS revenue_lifetime_churn_corr,
    CORR(categories_unique, target::int) AS categories_unique_churn_corr
  FROM ml_training_dataset_churn
)

-- ========= ФИНАЛЬНЫЙ ОТЧЕТ =========
SELECT 
  '=========================' AS separator1,
  'CHURN TRAINING DATASET ANALYSIS' AS title,
  '=========================' AS separator2,
  
  -- Основная статистика
  '📊 ОСНОВНАЯ СТАТИСТИКА:' AS section1,
  bs.total_records AS total_records,
  bs.unique_users AS unique_users,
  bs.unique_snapshots AS unique_snapshots,
  bs.earliest_snapshot AS earliest_snapshot,
  bs.latest_snapshot AS latest_snapshot,
  bs.churn_count AS churn_cases,
  bs.retention_count AS retention_cases,
  ROUND((bs.churn_count::NUMERIC / bs.total_records::NUMERIC) * 100, 2) AS churn_rate_percent,
  
  -- Split статистика
  '📈 SPLIT СТАТИСТИКА:' AS section2,
  ss_train.records_count AS train_records,
  ss_train.churn_rate_percent AS train_churn_rate,
  ss_valid.records_count AS valid_test_records,
  ss_valid.churn_rate_percent AS valid_test_churn_rate,
  
  -- NULL значения
  '❓ NULL ЗНАЧЕНИЯ:' AS section3,
  fs.recency_nulls AS recency_nulls,
  fs.frequency_nulls AS frequency_nulls,
  fs.monetary_nulls AS monetary_nulls,
  fs.aov_nulls AS aov_nulls,
  fs.orders_lifetime_nulls AS orders_lifetime_nulls,
  fs.revenue_lifetime_nulls AS revenue_lifetime_nulls,
  fs.categories_unique_nulls AS categories_unique_nulls,
  
  -- Корреляции с таргетом
  '🔗 КОРРЕЛЯЦИИ С CHURN:' AS section4,
  ROUND(ftc.recency_churn_corr::numeric, 4) AS recency_churn_corr,
  ROUND(ftc.frequency_churn_corr::numeric, 4) AS frequency_churn_corr,
  ROUND(ftc.monetary_churn_corr::numeric, 4) AS monetary_churn_corr,
  ROUND(ftc.aov_churn_corr::numeric, 4) AS aov_churn_corr,
  ROUND(ftc.orders_lifetime_churn_corr::numeric, 4) AS orders_lifetime_churn_corr,
  ROUND(ftc.revenue_lifetime_churn_corr::numeric, 4) AS revenue_lifetime_churn_corr,
  ROUND(ftc.categories_unique_churn_corr::numeric, 4) AS categories_unique_churn_corr,
  
  '=========================' AS separator3

FROM basic_stats bs
CROSS JOIN feature_stats fs
CROSS JOIN feature_target_correlation ftc
CROSS JOIN split_stats ss_train
CROSS JOIN split_stats ss_valid
WHERE ss_train.split_type = 'train' AND ss_valid.split_type = 'valid_test';

-- ========= ДЕТАЛЬНАЯ СТАТИСТИКА ПРИЗНАКОВ =========
SELECT 
  '📋 ДЕТАЛЬНАЯ СТАТИСТИКА ПРИЗНАКОВ:' AS section,
  'recency_days' AS feature_name,
  ROUND(fs.recency_mean::numeric, 2) AS mean_value,
  ROUND(fs.recency_median::numeric, 2) AS median_value,
  fs.recency_min AS min_value,
  fs.recency_max AS max_value,
  fs.recency_nulls AS null_count
FROM feature_stats fs

UNION ALL

SELECT 
  'frequency_90d' AS feature_name,
  ROUND(fs.frequency_mean, 2) AS mean_value,
  ROUND(fs.frequency_median, 2) AS median_value,
  fs.frequency_min AS min_value,
  fs.frequency_max AS max_value,
  fs.frequency_nulls AS null_count
FROM feature_stats fs

UNION ALL

SELECT 
  'monetary_180d' AS feature_name,
  ROUND(fs.monetary_mean, 2) AS mean_value,
  ROUND(fs.monetary_median, 2) AS median_value,
  fs.monetary_min AS min_value,
  fs.monetary_max AS max_value,
  fs.monetary_nulls AS null_count
FROM feature_stats fs

UNION ALL

SELECT 
  'aov_180d' AS feature_name,
  ROUND(fs.aov_mean, 2) AS mean_value,
  ROUND(fs.aov_median, 2) AS median_value,
  fs.aov_min AS min_value,
  fs.aov_max AS max_value,
  fs.aov_nulls AS null_count
FROM feature_stats fs

UNION ALL

SELECT 
  'orders_lifetime' AS feature_name,
  ROUND(fs.orders_lifetime_mean, 2) AS mean_value,
  ROUND(fs.orders_lifetime_median, 2) AS median_value,
  fs.orders_lifetime_min AS min_value,
  fs.orders_lifetime_max AS max_value,
  fs.orders_lifetime_nulls AS null_count
FROM feature_stats fs

UNION ALL

SELECT 
  'revenue_lifetime' AS feature_name,
  ROUND(fs.revenue_lifetime_mean, 2) AS mean_value,
  ROUND(fs.revenue_lifetime_median, 2) AS median_value,
  fs.revenue_lifetime_min AS min_value,
  fs.revenue_lifetime_max AS max_value,
  fs.revenue_lifetime_nulls AS null_count
FROM feature_stats fs

UNION ALL

SELECT 
  'categories_unique' AS feature_name,
  ROUND(fs.categories_unique_mean, 2) AS mean_value,
  ROUND(fs.categories_unique_median, 2) AS median_value,
  fs.categories_unique_min AS min_value,
  fs.categories_unique_max AS max_value,
  fs.categories_unique_nulls AS null_count
FROM feature_stats fs

ORDER BY feature_name;

-- ========= ПРОВЕРКА КАЧЕСТВА ДАННЫХ =========
DO $$
DECLARE
    total_records INTEGER;
    null_records INTEGER;
    churn_rate NUMERIC;
    train_ratio NUMERIC;
    valid_test_ratio NUMERIC;
BEGIN
    -- Получаем статистику
    SELECT 
      COUNT(*),
      COUNT(*) FILTER (WHERE recency_days IS NULL OR frequency_90d IS NULL OR 
                       monetary_180d IS NULL OR aov_180d IS NULL OR 
                       orders_lifetime IS NULL OR revenue_lifetime IS NULL OR 
                       categories_unique IS NULL),
      (COUNT(*) FILTER (WHERE target = TRUE)::NUMERIC / COUNT(*)::NUMERIC) * 100,
      (COUNT(*) FILTER (WHERE split_type = 'train')::NUMERIC / COUNT(*)::NUMERIC) * 100,
      (COUNT(*) FILTER (WHERE split_type = 'valid_test')::NUMERIC / COUNT(*)::NUMERIC) * 100
    INTO total_records, null_records, churn_rate, train_ratio, valid_test_ratio
    FROM ml_training_dataset_churn;
    
    -- Выводим итоговую оценку
    RAISE NOTICE '=========================';
    RAISE NOTICE 'ИТОГОВАЯ ОЦЕНКА КАЧЕСТВА';
    RAISE NOTICE '=========================';
    RAISE NOTICE 'Total Records: %', total_records;
    RAISE NOTICE 'Churn Rate: %.1f%%', churn_rate;
    RAISE NOTICE 'Train Ratio: %.1f%%', train_ratio;
    RAISE NOTICE 'Valid/Test Ratio: %.1f%%', valid_test_ratio;
    RAISE NOTICE 'Records with NULLs: % (%.1f%%)', null_records, (null_records::NUMERIC / total_records::NUMERIC) * 100;
    
    -- Рекомендации
    IF churn_rate < 15 OR churn_rate > 50 THEN
        RAISE WARNING '⚠️  Churn rate (%.1f%%) вне ожидаемого диапазона (15-50%%)', churn_rate;
    ELSE
        RAISE NOTICE '✅ Churn rate (%.1f%%) в ожидаемом диапазоне', churn_rate;
    END IF;
    
    IF train_ratio < 65 OR train_ratio > 75 THEN
        RAISE WARNING '⚠️  Train ratio (%.1f%%) вне ожидаемого диапазона (65-75%%)', train_ratio;
    ELSE
        RAISE NOTICE '✅ Train ratio (%.1f%%) в ожидаемом диапазоне', train_ratio;
    END IF;
    
    IF null_records > total_records * 0.05 THEN
        RAISE WARNING '⚠️  Слишком много NULL значений (%.1f%%)', (null_records::NUMERIC / total_records::NUMERIC) * 100;
    ELSE
        RAISE NOTICE '✅ NULL rate (%.1f%%) приемлемый', (null_records::NUMERIC / total_records::NUMERIC) * 100;
    END IF;
    
    RAISE NOTICE '=========================';
END $$;
