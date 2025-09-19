-- =========================
-- Populate Training Dataset
-- =========================
-- Объединение фич и таргетов в единый обучающий датасет

INSERT INTO ml_training_dataset (
  user_id,
  snapshot_date,
  recency_days,
  frequency_90d,
  monetary_180d,
  aov_180d,
  orders_lifetime,
  revenue_lifetime,
  categories_unique,
  purchase_next_30d
)
SELECT 
  f.user_id,
  f.snapshot_date,
  f.recency_days,
  f.frequency_90d,
  f.monetary_180d,
  f.aov_180d,
  f.orders_lifetime,
  f.revenue_lifetime,
  f.categories_unique,
  l.purchase_next_30d
FROM ml_user_features_daily_all f
INNER JOIN ml_labels_purchase_30d l 
  ON f.user_id = l.user_id 
  AND f.snapshot_date = l.snapshot_date
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  recency_days = EXCLUDED.recency_days,
  frequency_90d = EXCLUDED.frequency_90d,
  monetary_180d = EXCLUDED.monetary_180d,
  aov_180d = EXCLUDED.aov_180d,
  orders_lifetime = EXCLUDED.orders_lifetime,
  revenue_lifetime = EXCLUDED.revenue_lifetime,
  categories_unique = EXCLUDED.categories_unique,
  purchase_next_30d = EXCLUDED.purchase_next_30d,
  created_at = now();
