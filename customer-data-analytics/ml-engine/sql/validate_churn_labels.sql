-- =========================
-- Validate Churn Labels Quality
-- =========================
-- Валидация качества разметки churn лейблов с горизонтом 60 дней

-- ========= ПАРАМЕТРЫ ВАЛИДАЦИИ =========
WITH validation_params AS (
  SELECT
    60 ::int AS churn_window_days,
    ARRAY['paid','shipped','completed']::text[] AS ok_status,
    30 ::int AS min_tenure_days
),

-- ========= ОСНОВНАЯ СТАТИСТИКА =========
basic_stats AS (
  SELECT 
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE) AS churn_count,
    COUNT(*) FILTER (WHERE is_churn_next_60d = FALSE) AS retention_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT snapshot_date) AS unique_snapshots,
    MIN(snapshot_date) AS earliest_snapshot,
    MAX(snapshot_date) AS latest_snapshot
  FROM ml_labels_churn_60d
),

-- ========= РАСЧЕТ CHURN RATE =========
churn_rate AS (
  SELECT 
    bs.*,
    (bs.churn_count::NUMERIC / bs.total_records::NUMERIC) * 100 AS churn_rate_percent,
    (bs.retention_count::NUMERIC / bs.total_records::NUMERIC) * 100 AS retention_rate_percent
  FROM basic_stats bs
),

-- ========= ПРОВЕРКА КОНСИСТЕНТНОСТИ С ЗАКАЗАМИ =========
consistency_check AS (
  SELECT 
    -- Проверяем churn=TRUE: НЕ должно быть заказов в окне
    COUNT(*) FILTER (
      WHERE is_churn_next_60d = TRUE 
        AND EXISTS (
          SELECT 1 FROM orders o
          WHERE o.user_id = ml_labels_churn_60d.user_id
            AND o.status = ANY((SELECT ok_status FROM validation_params))
            AND o.created_at::date > ml_labels_churn_60d.snapshot_date
            AND o.created_at::date <= (ml_labels_churn_60d.snapshot_date + (SELECT churn_window_days || ' days' FROM validation_params)::interval)
        )
    ) AS churn_with_orders_error_count,
    
    -- Проверяем churn=FALSE: ДОЛЖЕН быть хотя бы один заказ в окне
    COUNT(*) FILTER (
      WHERE is_churn_next_60d = FALSE 
        AND NOT EXISTS (
          SELECT 1 FROM orders o
          WHERE o.user_id = ml_labels_churn_60d.user_id
            AND o.status = ANY((SELECT ok_status FROM validation_params))
            AND o.created_at::date > ml_labels_churn_60d.snapshot_date
            AND o.created_at::date <= (ml_labels_churn_60d.snapshot_date + (SELECT churn_window_days || ' days' FROM validation_params)::interval)
        )
    ) AS retention_without_orders_error_count
  FROM ml_labels_churn_60d
),

-- ========= ПРОВЕРКА ELIGIBILITY КРИТЕРИЕВ =========
eligibility_check AS (
  SELECT 
    -- Новые пользователи (стаж < 30 дней)
    COUNT(*) FILTER (
      WHERE EXISTS (
        SELECT 1 FROM orders o
        WHERE o.user_id = ml_labels_churn_60d.user_id
          AND o.status = ANY((SELECT ok_status FROM validation_params))
        HAVING (ml_labels_churn_60d.snapshot_date - MIN(o.created_at::date)) < (SELECT min_tenure_days FROM validation_params)
      )
    ) AS new_users_included_error_count,
    
    -- Пользователи без активности за 180 дней
    COUNT(*) FILTER (
      WHERE NOT EXISTS (
        SELECT 1 FROM orders o
        WHERE o.user_id = ml_labels_churn_60d.user_id
          AND o.status = ANY((SELECT ok_status FROM validation_params))
          AND o.created_at::date >= (ml_labels_churn_60d.snapshot_date - '180 days'::interval)
          AND o.created_at::date <= ml_labels_churn_60d.snapshot_date
      )
    ) AS inactive_users_included_error_count
  FROM ml_labels_churn_60d
),

-- ========= РАСПРЕДЕЛЕНИЕ ПО ДАТАМ =========
date_distribution AS (
  SELECT 
    snapshot_date,
    COUNT(*) AS records_count,
    COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE) AS churn_count,
    (COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE)::NUMERIC / COUNT(*)::NUMERIC) * 100 AS churn_rate_percent
  FROM ml_labels_churn_60d
  GROUP BY snapshot_date
  ORDER BY snapshot_date
),

-- ========= ПРОВЕРКА МЕТАДАННЫХ =========
metadata_check AS (
  SELECT 
    COUNT(*) FILTER (WHERE last_order_before_date IS NULL) AS missing_last_order_before,
    COUNT(*) FILTER (WHERE is_churn_next_60d = FALSE AND first_order_after_date IS NULL) AS retention_missing_first_order_after,
    COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE AND first_order_after_date IS NOT NULL) AS churn_with_first_order_after_error
  FROM ml_labels_churn_60d
)

-- ========= ФИНАЛЬНЫЙ ОТЧЕТ =========
SELECT 
  '=========================' AS separator1,
  'CHURN LABELS VALIDATION REPORT' AS title,
  '=========================' AS separator2,
  
  -- Основная статистика
  '📊 ОСНОВНАЯ СТАТИСТИКА:' AS section1,
  cr.total_records AS total_records,
  cr.unique_users AS unique_users,
  cr.unique_snapshots AS unique_snapshots,
  cr.earliest_snapshot AS earliest_snapshot,
  cr.latest_snapshot AS latest_snapshot,
  
  -- Churn rate
  '📈 CHURN RATE:' AS section2,
  cr.churn_count AS churn_cases,
  cr.retention_count AS retention_cases,
  ROUND(cr.churn_rate_percent, 2) AS churn_rate_percent,
  ROUND(cr.retention_rate_percent, 2) AS retention_rate_percent,
  
  -- Консистентность
  '✅ КОНСИСТЕНТНОСТЬ:' AS section3,
  cc.churn_with_orders_error_count AS churn_with_orders_errors,
  cc.retention_without_orders_error_count AS retention_without_orders_errors,
  
  -- Eligibility
  '👥 ELIGIBILITY:' AS section4,
  ec.new_users_included_error_count AS new_users_errors,
  ec.inactive_users_included_error_count AS inactive_users_errors,
  
  -- Метаданные
  '📋 МЕТАДАННЫЕ:' AS section5,
  mc.missing_last_order_before AS missing_last_order_before,
  mc.retention_missing_first_order_after AS retention_missing_first_order_after,
  mc.churn_with_first_order_after_error AS churn_with_first_order_after_error,
  
  -- Общая оценка качества
  '🎯 ОЦЕНКА КАЧЕСТВА:' AS section6,
  CASE 
    WHEN (cc.churn_with_orders_error_count + cc.retention_without_orders_error_count + 
          ec.new_users_included_error_count + ec.inactive_users_included_error_count + 
          mc.churn_with_first_order_after_error) = 0 THEN 'EXCELLENT'
    WHEN (cc.churn_with_orders_error_count + cc.retention_without_orders_error_count + 
          ec.new_users_included_error_count + ec.inactive_users_included_error_count + 
          mc.churn_with_first_order_after_error) <= 10 THEN 'GOOD'
    ELSE 'NEEDS_REVIEW'
  END AS quality_score,
  
  '=========================' AS separator3

FROM churn_rate cr
CROSS JOIN consistency_check cc
CROSS JOIN eligibility_check ec
CROSS JOIN metadata_check mc;

-- ========= ДЕТАЛЬНОЕ РАСПРЕДЕЛЕНИЕ ПО ДАТАМ =========
SELECT 
  '📅 РАСПРЕДЕЛЕНИЕ ПО ДАТАМ:' AS section,
  snapshot_date,
  records_count,
  churn_count,
  ROUND(churn_rate_percent, 1) AS churn_rate_percent
FROM date_distribution dd
ORDER BY snapshot_date;

-- ========= ПРОВЕРКА БАЛАНСА КЛАССОВ =========
DO $$
DECLARE
    churn_rate NUMERIC;
    quality_issues INTEGER;
BEGIN
    -- Получаем churn rate
    SELECT (COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE)::NUMERIC / COUNT(*)::NUMERIC) * 100
    INTO churn_rate
    FROM ml_labels_churn_60d;
    
    -- Подсчитываем общее количество проблем
    SELECT 
      COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE AND EXISTS (
        SELECT 1 FROM orders o
        WHERE o.user_id = ml_labels_churn_60d.user_id
          AND o.status = ANY(ARRAY['paid','shipped','completed'])
          AND o.created_at::date > ml_labels_churn_60d.snapshot_date
          AND o.created_at::date <= (ml_labels_churn_60d.snapshot_date + '60 days'::interval)
      )) +
      COUNT(*) FILTER (WHERE is_churn_next_60d = FALSE AND NOT EXISTS (
        SELECT 1 FROM orders o
        WHERE o.user_id = ml_labels_churn_60d.user_id
          AND o.status = ANY(ARRAY['paid','shipped','completed'])
          AND o.created_at::date > ml_labels_churn_60d.snapshot_date
          AND o.created_at::date <= (ml_labels_churn_60d.snapshot_date + '60 days'::interval)
      ))
    INTO quality_issues
    FROM ml_labels_churn_60d;
    
    -- Выводим итоговую оценку
    RAISE NOTICE '=========================';
    RAISE NOTICE 'ИТОГОВАЯ ОЦЕНКА КАЧЕСТВА';
    RAISE NOTICE '=========================';
    RAISE NOTICE 'Churn Rate: %.1f%%', churn_rate;
    RAISE NOTICE 'Quality Issues: %', quality_issues;
    
    -- Рекомендации
    IF churn_rate < 10 OR churn_rate > 60 THEN
        RAISE WARNING '⚠️  Churn rate (%.1f%%) вне ожидаемого диапазона (20-40%%)', churn_rate;
    ELSE
        RAISE NOTICE '✅ Churn rate (%.1f%%) в ожидаемом диапазоне', churn_rate;
    END IF;
    
    IF quality_issues = 0 THEN
        RAISE NOTICE '✅ Отличное качество данных - нет ошибок консистентности';
    ELSIF quality_issues <= 10 THEN
        RAISE NOTICE '✅ Хорошее качество данных - минимальные ошибки (%)', quality_issues;
    ELSE
        RAISE WARNING '⚠️  Требуется проверка качества - % ошибок консистентности', quality_issues;
    END IF;
    
    RAISE NOTICE '=========================';
END $$;
