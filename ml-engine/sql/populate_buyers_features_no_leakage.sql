-- =========================
-- Populate Buyers Features WITHOUT Data Leakage
-- =========================

-- ОЧИСТКА СТАРЫХ ДАННЫХ
TRUNCATE TABLE ml_user_features_daily_buyers;

-- ЗАПОЛНЕНИЕ ТОЛЬКО ПОКУПАТЕЛЯМИ (без утечек)
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
  AND (recency_days IS NULL OR recency_days >= 0);  -- 🔥 КРИТИЧНО: исключаем отрицательные recency
