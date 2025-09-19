-- =========================
-- ML User Features 30 Days Computation - Simple Version
-- =========================
-- Создание 30 снапшотов за последние 30 дней для RFM-анализа

-- ========= ПАРАМЕТРЫ ОКОН =========
WITH params AS (
  SELECT
    90  ::int                    AS win_f,    -- окно для Frequency
    180 ::int                    AS win_m,    -- окно для Monetary/категорий
    ARRAY['paid','shipped','completed']::text[] AS ok_status
),

-- ========= ГЕНЕРАЦИЯ 30 ДНЕЙ =========
date_range AS (
  SELECT 
    (CURRENT_DATE - (i || ' days')::interval)::date AS snapshot_date
  FROM generate_series(0, 29) AS i
),

-- ========= ПОДГОТОВКА АГРЕГАТОВ =========
last_order AS (
  SELECT o.user_id, MAX(o.created_at)::date AS last_dt
  FROM orders o, params p
  WHERE o.status = ANY(p.ok_status)
  GROUP BY o.user_id
),
freq_90 AS (
  SELECT 
    o.user_id, 
    dr.snapshot_date,
    COUNT(*) AS cnt
  FROM orders o, params p, date_range dr
  WHERE o.status = ANY(p.ok_status)
    AND o.created_at >= (dr.snapshot_date - (p.win_f || ' days')::interval)
    AND o.created_at <  (dr.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, dr.snapshot_date
),
mon_180 AS (
  SELECT 
    o.user_id,
    dr.snapshot_date,
    SUM(o.total_amount)::numeric(12,2) AS sum_amt,
    COUNT(*)                            AS cnt
  FROM orders o, params p, date_range dr
  WHERE o.status = ANY(p.ok_status)
    AND o.created_at >= (dr.snapshot_date - (p.win_m || ' days')::interval)
    AND o.created_at <  (dr.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, dr.snapshot_date
),
life_all AS (
  SELECT o.user_id,
         COUNT(*)                             AS orders_lt,
         SUM(o.total_amount)::numeric(12,2)   AS revenue_lt
  FROM orders o, params p
  WHERE o.status = ANY(p.ok_status)
  GROUP BY o.user_id
),
cats_180 AS (
  SELECT 
    o.user_id,
    dr.snapshot_date,
    COUNT(DISTINCT pr.category) AS cats
  FROM orders o
  JOIN order_items oi ON oi.order_id = o.order_id
  JOIN products pr    ON pr.product_id = oi.product_id, params p, date_range dr
  WHERE o.status = ANY(p.ok_status)
    AND o.created_at >= (dr.snapshot_date - (p.win_m || ' days')::interval)
    AND o.created_at <  (dr.snapshot_date + '1 day'::interval)
  GROUP BY o.user_id, dr.snapshot_date
),

-- ========= ПОЛНЫЙ СПИСОК ПОЛЬЗОВАТЕЛЕЙ =========
u AS (SELECT user_id FROM users),

-- ========= СВОД ДЛЯ ВСЕХ =========
feat_all AS (
  SELECT
    u.user_id,
    dr.snapshot_date,
    CASE WHEN lo.last_dt IS NULL
         THEN NULL
         ELSE GREATEST(0, (dr.snapshot_date - lo.last_dt))::int
    END                                              AS recency_days,
    COALESCE(f.cnt, 0)                               AS frequency_90d,
    COALESCE(m.sum_amt, 0)::numeric(12,2)            AS monetary_180d,
    CASE WHEN COALESCE(m.cnt,0) > 0
         THEN (m.sum_amt / m.cnt)::numeric(12,2)
         ELSE NULL
    END                                              AS aov_180d,
    COALESCE(l.orders_lt, 0)                         AS orders_lifetime,
    COALESCE(l.revenue_lt, 0)::numeric(12,2)         AS revenue_lifetime,
    COALESCE(c.cats, 0)                              AS categories_unique
  FROM u
  CROSS JOIN date_range dr
  LEFT JOIN last_order lo ON lo.user_id = u.user_id
  LEFT JOIN freq_90    f  ON f.user_id = u.user_id AND f.snapshot_date = dr.snapshot_date
  LEFT JOIN mon_180    m  ON m.user_id = u.user_id AND m.snapshot_date = dr.snapshot_date
  LEFT JOIN life_all   l  ON l.user_id = u.user_id
  LEFT JOIN cats_180   c  ON c.user_id = u.user_id AND c.snapshot_date = dr.snapshot_date
),

-- ========= ТОЛЬКО ПОКУПАТЕЛИ =========
buyers AS (
  SELECT DISTINCT user_id FROM life_all WHERE orders_lt > 0
)

-- ========= ЗАПИСИ В ВИТРИНЫ =========
INSERT INTO ml_user_features_daily_all (
  user_id, snapshot_date, recency_days, frequency_90d, monetary_180d, aov_180d,
  orders_lifetime, revenue_lifetime, categories_unique
)
SELECT
  user_id, snapshot_date, recency_days, frequency_90d, monetary_180d, aov_180d,
  orders_lifetime, revenue_lifetime, categories_unique
FROM feat_all
ON CONFLICT (user_id, snapshot_date) DO UPDATE SET
  recency_days      = EXCLUDED.recency_days,
  frequency_90d     = EXCLUDED.frequency_90d,
  monetary_180d     = EXCLUDED.monetary_180d,
  aov_180d          = EXCLUDED.aov_180d,
  orders_lifetime   = EXCLUDED.orders_lifetime,
  revenue_lifetime  = EXCLUDED.revenue_lifetime,
  categories_unique = EXCLUDED.categories_unique
;

-- Вторая вставка для покупателей
WITH feat_all AS (
  SELECT
    u.user_id,
    dr.snapshot_date,
    CASE WHEN lo.last_dt IS NULL
         THEN NULL
         ELSE GREATEST(0, (dr.snapshot_date - lo.last_dt))::int
    END                                              AS recency_days,
    COALESCE(f.cnt, 0)                               AS frequency_90d,
    COALESCE(m.sum_amt, 0)::numeric(12,2)            AS monetary_180d,
    CASE WHEN COALESCE(m.cnt,0) > 0
         THEN (m.sum_amt / m.cnt)::numeric(12,2)
         ELSE NULL
    END                                              AS aov_180d,
    COALESCE(l.orders_lt, 0)                         AS orders_lifetime,
    COALESCE(l.revenue_lt, 0)::numeric(12,2)         AS revenue_lifetime,
    COALESCE(c.cats, 0)                              AS categories_unique
  FROM u
  CROSS JOIN date_range dr
  LEFT JOIN last_order lo ON lo.user_id = u.user_id
  LEFT JOIN freq_90    f  ON f.user_id = u.user_id AND f.snapshot_date = dr.snapshot_date
  LEFT JOIN mon_180    m  ON m.user_id = u.user_id AND m.snapshot_date = dr.snapshot_date
  LEFT JOIN life_all   l  ON l.user_id = u.user_id
  LEFT JOIN cats_180   c  ON c.user_id = u.user_id AND c.snapshot_date = dr.snapshot_date
),
buyers AS (
  SELECT DISTINCT user_id FROM life_all WHERE orders_lt > 0
)
INSERT INTO ml_user_features_daily_buyers (
  user_id, snapshot_date, recency_days, frequency_90d, monetary_180d, aov_180d,
  orders_lifetime, revenue_lifetime, categories_unique
)
SELECT
  f.user_id, f.snapshot_date,
  COALESCE(f.recency_days, 999999) AS recency_days,  -- у баеров всегда не NULL, но подстрахуемся
  f.frequency_90d,
  f.monetary_180d,
  COALESCE(f.aov_180d, 0)::numeric(12,2) AS aov_180d,
  f.orders_lifetime,
  f.revenue_lifetime,
  f.categories_unique
FROM feat_all f
JOIN buyers b USING (user_id)
ON CONFLICT (user_id, snapshot_date) DO UPDATE SET
  recency_days      = EXCLUDED.recency_days,
  frequency_90d     = EXCLUDED.frequency_90d,
  monetary_180d     = EXCLUDED.monetary_180d,
  aov_180d          = EXCLUDED.aov_180d,
  orders_lifetime   = EXCLUDED.orders_lifetime,
  revenue_lifetime  = EXCLUDED.revenue_lifetime,
  categories_unique = EXCLUDED.categories_unique
;
