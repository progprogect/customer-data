-- =========================
-- ML User Features WITHOUT Data Leakage
-- =========================
-- ИСПРАВЛЕННАЯ версия: строгое соблюдение as-of snapshot_date

-- ОЧИСТКА СТАРЫХ ДАННЫХ
TRUNCATE TABLE ml_user_features_daily_all;
TRUNCATE TABLE ml_user_features_daily_buyers;

-- ========= ГЕНЕРАЦИЯ ЕЖЕНЕДЕЛЬНЫХ СНАПШОТОВ =========
WITH weekly_dates AS (
  SELECT 
    generate_series(
      date_trunc('week', CURRENT_DATE - '6 months'::interval), 
      date_trunc('week', CURRENT_DATE), 
      '1 week'::interval
    )::date AS snapshot_date
),

-- ========= ПОСЛЕДНИЙ ЗАКАЗ AS-OF SNAPSHOT_DATE =========
last_order_as_of AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    MAX(o.created_at::date) AS last_order_date
  FROM orders o
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at::date <= wd.snapshot_date  -- 🔥 КРИТИЧНО: только прошлое!
  GROUP BY o.user_id, wd.snapshot_date
),

-- ========= FREQUENCY: ЗАКАЗЫ ЗА 90 ДНЕЙ AS-OF =========
freq_90_as_of AS (
  SELECT 
    o.user_id, 
    wd.snapshot_date,
    COUNT(*) AS cnt
  FROM orders o 
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at::date <= wd.snapshot_date  -- 🔥 КРИТИЧНО: только прошлое!
    AND o.created_at::date >= (wd.snapshot_date - '90 days'::interval)::date
  GROUP BY o.user_id, wd.snapshot_date
),

-- ========= MONETARY: СУММА ЗА 180 ДНЕЙ AS-OF =========
mon_180_as_of AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    SUM(o.total_amount)::numeric(12,2) AS sum_amt,
    COUNT(*) AS cnt
  FROM orders o
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at::date <= wd.snapshot_date  -- 🔥 КРИТИЧНО: только прошлое!
    AND o.created_at::date >= (wd.snapshot_date - '180 days'::interval)::date
  GROUP BY o.user_id, wd.snapshot_date
),

-- ========= LIFETIME МЕТРИКИ AS-OF =========
lifetime_as_of AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    COUNT(*) AS orders_lifetime,
    SUM(o.total_amount)::numeric(12,2) AS revenue_lifetime
  FROM orders o
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at::date <= wd.snapshot_date  -- 🔥 КРИТИЧНО: только прошлое!
  GROUP BY o.user_id, wd.snapshot_date
),

-- ========= УНИКАЛЬНЫЕ КАТЕГОРИИ ЗА 180 ДНЕЙ AS-OF =========
categories_180_as_of AS (
  SELECT 
    o.user_id,
    wd.snapshot_date,
    COUNT(DISTINCT prod.category) AS categories_unique
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products prod ON prod.product_id = oi.product_id
  CROSS JOIN weekly_dates wd
  WHERE o.status IN ('paid', 'shipped', 'completed')
    AND o.created_at::date <= wd.snapshot_date  -- 🔥 КРИТИЧНО: только прошлое!
    AND o.created_at::date >= (wd.snapshot_date - '180 days'::interval)::date
  GROUP BY o.user_id, wd.snapshot_date
),

-- ========= ПОЛУЧЕНИЕ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ =========
all_users AS (
  SELECT DISTINCT user_id 
  FROM users 
  WHERE user_id IS NOT NULL
)

-- ========= ВСТАВКА ИСПРАВЛЕННЫХ ДАННЫХ =========
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
  -- 🔥 ИСПРАВЛЕННЫЙ RECENCY: только положительные значения
  CASE 
    WHEN lo.last_order_date IS NULL THEN NULL
    ELSE (wd.snapshot_date - lo.last_order_date)::int
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
LEFT JOIN last_order_as_of lo ON lo.user_id = u.user_id AND lo.snapshot_date = wd.snapshot_date
LEFT JOIN freq_90_as_of f90 ON f90.user_id = u.user_id AND f90.snapshot_date = wd.snapshot_date
LEFT JOIN mon_180_as_of m180 ON m180.user_id = u.user_id AND m180.snapshot_date = wd.snapshot_date
LEFT JOIN lifetime_as_of lt ON lt.user_id = u.user_id AND lt.snapshot_date = wd.snapshot_date
LEFT JOIN categories_180_as_of c180 ON c180.user_id = u.user_id AND c180.snapshot_date = wd.snapshot_date;
