-- =========================
-- Generate Churn Target Labels for 60 Days (Weekly Snapshots)
-- =========================
-- Генерация таргетов churn_next_60d для еженедельных снапшотов за последние 6 месяцев
-- Churn = отсутствие заказов в течение 60 дней после снапшота

-- ========= ПАРАМЕТРЫ =========
WITH params AS (
  SELECT
    60 ::int AS churn_window_days,                     -- окно для определения churn (60 дней)
    ARRAY['paid','shipped','completed']::text[] AS ok_status,  -- успешные статусы заказов
    6 ::int AS months_history,                         -- глубина истории в месяцах
    180 ::int AS activity_window_days,                 -- окно активности (180 дней)
    30 ::int AS min_tenure_days                        -- минимальный стаж от первого заказа
),

-- ========= ОПРЕДЕЛЕНИЕ ВРЕМЕННЫХ ГРАНИЦ =========
time_bounds AS (
  SELECT 
    p.churn_window_days,
    p.ok_status,
    p.activity_window_days,
    p.min_tenure_days,
    p.months_history,
    -- Находим максимальную дату заказа
    MAX(o.created_at::date) AS max_order_date,
    -- Последняя валидная дата снапшота (чтобы окно churn было полным)
    (MAX(o.created_at::date) - (p.churn_window_days || ' days')::interval)::date AS max_valid_snapshot_date,
    -- Начальная дата снапшота (6 месяцев назад от max_valid_snapshot_date)
    (MAX(o.created_at::date) - (p.churn_window_days || ' days')::interval - (p.months_history || ' months')::interval)::date AS min_snapshot_date
  FROM orders o, params p
  WHERE o.status = ANY(p.ok_status)
  GROUP BY p.churn_window_days, p.ok_status, p.activity_window_days, p.min_tenure_days, p.months_history
),

-- ========= ГЕНЕРАЦИЯ ЕЖЕНЕДЕЛЬНЫХ СНАПШОТОВ (ПОНЕДЕЛЬНИКИ) =========
weekly_snapshots AS (
  SELECT 
    -- Генерируем понедельники в диапазоне
    generate_series(
      (SELECT min_snapshot_date FROM time_bounds),
      (SELECT max_valid_snapshot_date FROM time_bounds),
      '7 days'::interval
    )::date AS snapshot_date
),

-- ========= ПОЛЬЗОВАТЕЛИ С ПЕРВЫМ ЗАКАЗОМ =========
user_first_orders AS (
  SELECT 
    o.user_id,
    MIN(o.created_at::date) AS first_order_date
  FROM orders o, params p
  WHERE o.status = ANY(p.ok_status)
  GROUP BY o.user_id
),

-- ========= ELIGIBLE ПОЛЬЗОВАТЕЛИ НА КАЖДУЮ ДАТУ =========
eligible_users AS (
  SELECT DISTINCT
    uf.user_id,
    ws.snapshot_date,
    tb.churn_window_days,
    tb.activity_window_days,
    tb.min_tenure_days,
    tb.ok_status,
    uf.first_order_date,
    -- Проверяем условия eligibility
    CASE 
      WHEN uf.first_order_date IS NULL THEN FALSE  -- нет заказов вообще
      WHEN (ws.snapshot_date - uf.first_order_date) < tb.min_tenure_days THEN FALSE  -- новичок
      ELSE TRUE
    END AS is_eligible
  FROM user_first_orders uf
  CROSS JOIN weekly_snapshots ws
  CROSS JOIN time_bounds tb
  WHERE 
    -- Пользователь есть в витрине снапшотов
    EXISTS (
      SELECT 1 FROM ml_user_features_daily_all f 
      WHERE f.user_id = uf.user_id 
        AND f.snapshot_date = ws.snapshot_date
    )
    -- Есть активность за последние 180 дней
    AND EXISTS (
      SELECT 1 FROM orders o
      WHERE o.user_id = uf.user_id 
        AND o.status = ANY(tb.ok_status)
        AND o.created_at::date >= (ws.snapshot_date - (tb.activity_window_days || ' days')::interval)
        AND o.created_at::date <= ws.snapshot_date
    )
),

-- ========= ПОСЛЕДНИЙ ЗАКАЗ ДО СНАПШОТА =========
last_order_before AS (
  SELECT 
    eu.user_id,
    eu.snapshot_date,
    MAX(o.created_at::date) AS last_order_before_date
  FROM eligible_users eu
  JOIN orders o ON o.user_id = eu.user_id
  WHERE 
    eu.is_eligible = TRUE
    AND o.status = ANY(eu.ok_status)
    AND o.created_at::date <= eu.snapshot_date
  GROUP BY eu.user_id, eu.snapshot_date
),

-- ========= ПЕРВЫЙ ЗАКАЗ ПОСЛЕ СНАПШОТА (В ОКНЕ CHURN) =========
first_order_after AS (
  SELECT 
    eu.user_id,
    eu.snapshot_date,
    MIN(o.created_at::date) AS first_order_after_date
  FROM eligible_users eu
  JOIN orders o ON o.user_id = eu.user_id
  WHERE 
    eu.is_eligible = TRUE
    AND o.status = ANY(eu.ok_status)
    AND o.created_at::date > eu.snapshot_date
    AND o.created_at::date <= (eu.snapshot_date + (eu.churn_window_days || ' days')::interval)
  GROUP BY eu.user_id, eu.snapshot_date
),

-- ========= ФОРМИРОВАНИЕ ФИНАЛЬНЫХ CHURN ЛЕЙБЛОВ =========
final_churn_labels AS (
  SELECT 
    eu.user_id,
    eu.snapshot_date,
    eu.first_order_date,
    -- Основной таргет: churn = отсутствие заказов в окне (D, D+60]
    CASE 
      WHEN fo.first_order_after_date IS NULL THEN TRUE   -- НЕТ заказов в окне = CHURN
      ELSE FALSE                                         -- ЕСТЬ заказы в окне = НЕ CHURN
    END AS is_churn_next_60d,
    -- Метаданные для анализа и отладки
    lob.last_order_before_date,
    fo.first_order_after_date
  FROM eligible_users eu
  LEFT JOIN last_order_before lob ON lob.user_id = eu.user_id 
                                  AND lob.snapshot_date = eu.snapshot_date
  LEFT JOIN first_order_after fo ON fo.user_id = eu.user_id 
                                 AND fo.snapshot_date = eu.snapshot_date
  WHERE eu.is_eligible = TRUE
)

-- ========= ВСТАВКА ДАННЫХ С ОБНОВЛЕНИЕМ =========
INSERT INTO ml_labels_churn_60d (
  user_id, 
  snapshot_date, 
  is_churn_next_60d,
  last_order_before_date,
  first_order_after_date
)
SELECT 
  user_id,
  snapshot_date,
  is_churn_next_60d,
  last_order_before_date,
  first_order_after_date
FROM final_churn_labels
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  is_churn_next_60d = EXCLUDED.is_churn_next_60d,
  last_order_before_date = EXCLUDED.last_order_before_date,
  first_order_after_date = EXCLUDED.first_order_after_date;

-- ========= ЛОГИРОВАНИЕ СТАТИСТИКИ =========
DO $$
DECLARE
    total_rows INTEGER;
    churn_count INTEGER;
    retention_count INTEGER;
    churn_rate NUMERIC;
BEGIN
    -- Подсчитываем статистику
    SELECT 
      COUNT(*),
      COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE),
      COUNT(*) FILTER (WHERE is_churn_next_60d = FALSE)
    INTO total_rows, churn_count, retention_count
    FROM ml_labels_churn_60d;
    
    -- Вычисляем процент churn
    churn_rate := (churn_count::NUMERIC / total_rows::NUMERIC) * 100;
    
    -- Логируем результаты
    RAISE NOTICE '=========================';
    RAISE NOTICE 'CHURN LABELS GENERATION COMPLETE';
    RAISE NOTICE '=========================';
    RAISE NOTICE 'Total records: %', total_rows;
    RAISE NOTICE 'Churn cases: % (%.1f%%)', churn_count, churn_rate;
    RAISE NOTICE 'Retention cases: % (%.1f%%)', retention_count, 100 - churn_rate;
    RAISE NOTICE '=========================';
    
    -- Проверяем баланс классов
    IF churn_rate < 10 OR churn_rate > 60 THEN
        RAISE WARNING 'WARNING: Churn rate (%.1f%%) seems unusual. Expected range: 20-40%%', churn_rate;
    ELSE
        RAISE NOTICE 'INFO: Churn rate (%.1f%%) is within expected range (20-40%%)', churn_rate;
    END IF;
END $$;
