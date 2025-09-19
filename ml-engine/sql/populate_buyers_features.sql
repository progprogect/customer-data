-- =========================
-- Populate ML User Features Buyers Table
-- =========================
-- Заполнение таблицы только покупателями

INSERT INTO ml_user_features_daily_buyers (
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
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  COALESCE(aov_180d, 0.00) as aov_180d,  -- заменяем NULL на 0
  orders_lifetime,
  revenue_lifetime,
  categories_unique
FROM ml_user_features_daily_all
WHERE orders_lifetime > 0  -- только пользователи с покупками
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  recency_days = EXCLUDED.recency_days,
  frequency_90d = EXCLUDED.frequency_90d,
  monetary_180d = EXCLUDED.monetary_180d,
  aov_180d = EXCLUDED.aov_180d,
  orders_lifetime = EXCLUDED.orders_lifetime,
  revenue_lifetime = EXCLUDED.revenue_lifetime,
  categories_unique = EXCLUDED.categories_unique;
