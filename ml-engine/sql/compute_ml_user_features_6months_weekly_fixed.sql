-- =========================
-- ML User Features 6 Months Weekly Computation (Fixed)
-- =========================
-- Генерация еженедельных снапшотов RFM-признаков за 6 месяцев

-- ========= ЧАСТЬ 1: ВСТАВКА В ml_user_features_daily_all =========
WITH weekly_dates AS (
  SELECT 
    generate_series(
      date_trunc('week', CURRENT_DATE - '6 months'::interval), 
      date_trunc('week', CURRENT_DATE), 
      '1 week'::interval
    )::date AS snapshot_date
),

-- Последний заказ для каждого пользователя
last_order AS (
  SELECT 
    o.user_id, 
    MAX(o.created_at)::date AS last_dt
  FROM orders o
  WHERE o.status IN ('paid', 'shipped', 'completed')
  GROUP BY o.user_id
),

-- Frequency: количество заказов за 90 дней
freq_90 AS (
  SELECT 
    o.user_id, 
    wd.snapshot_date,
    COUNT(*) AS cnt
  FROM orders o 
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at >= (wd.snapshot_date - '90 days'::interval)
    AND o.created_at < (wd.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, wd.snapshot_date
),

-- Monetary: сумма и количество заказов за 180 дней
mon_180 AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    SUM(o.total_amount)::numeric(12,2) AS sum_amt,
    COUNT(*) AS cnt
  FROM orders o
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at >= (wd.snapshot_date - '180 days'::interval)
    AND o.created_at < (wd.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, wd.snapshot_date
),

-- Lifetime метрики
lifetime AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    COUNT(*) AS orders_lifetime,
    SUM(o.total_amount)::numeric(12,2) AS revenue_lifetime
  FROM orders o
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at < (wd.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, wd.snapshot_date
),

-- Уникальные категории за 180 дней
categories_180 AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    COUNT(DISTINCT prod.category) AS categories_unique
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products prod ON prod.product_id = oi.product_id
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at >= (wd.snapshot_date - '180 days'::interval)
    AND o.created_at < (wd.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, wd.snapshot_date
),

-- Все пользователи
all_users AS (
  SELECT DISTINCT user_id 
  FROM users 
  WHERE user_id IS NOT NULL
)

INSERT INTO ml_user_features_daily_all (
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  aov_180d,
  orders_lifetime,
  revenue_lifetime,
  categories_unique
)
SELECT 
  u.user_id,
  wd.snapshot_date,
  -- Recency: дни с последней покупки
  CASE 
    WHEN lo.last_dt IS NULL THEN NULL
    ELSE (wd.snapshot_date - lo.last_dt)::int
  END AS recency_days,
  -- Frequency: заказы за 90 дней
  COALESCE(f90.cnt, 0) AS frequency_90d,
  -- Monetary: сумма за 180 дней
  COALESCE(m180.sum_amt, 0) AS monetary_180d,
  -- AOV: средний чек за 180 дней
  CASE 
    WHEN m180.cnt > 0 THEN m180.sum_amt / m180.cnt 
    ELSE NULL 
  END AS aov_180d,
  -- Lifetime метрики
  COALESCE(lt.orders_lifetime, 0) AS orders_lifetime,
  COALESCE(lt.revenue_lifetime, 0) AS revenue_lifetime,
  -- Уникальные категории
  COALESCE(c180.categories_unique, 0) AS categories_unique
FROM all_users u
CROSS JOIN weekly_dates wd
LEFT JOIN last_order lo ON lo.user_id = u.user_id
LEFT JOIN freq_90 f90 ON f90.user_id = u.user_id AND f90.snapshot_date = wd.snapshot_date
LEFT JOIN mon_180 m180 ON m180.user_id = u.user_id AND m180.snapshot_date = wd.snapshot_date
LEFT JOIN lifetime lt ON lt.user_id = u.user_id AND lt.snapshot_date = wd.snapshot_date
LEFT JOIN categories_180 c180 ON c180.user_id = u.user_id AND c180.snapshot_date = wd.snapshot_date
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  recency_days = EXCLUDED.recency_days,
  frequency_90d = EXCLUDED.frequency_90d,
  monetary_180d = EXCLUDED.monetary_180d,
  aov_180d = EXCLUDED.aov_180d,
  orders_lifetime = EXCLUDED.orders_lifetime,
  revenue_lifetime = EXCLUDED.revenue_lifetime,
  categories_unique = EXCLUDED.categories_unique;
