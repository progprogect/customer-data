-- =========================
-- ML User Features Tables
-- =========================
-- Создание витринных таблиц для RFM-анализа и кластеризации

-- Все пользователи (включая без покупок)
CREATE TABLE IF NOT EXISTS ml_user_features_daily_all (
  user_id            BIGINT NOT NULL,
  snapshot_date      DATE   NOT NULL,
  recency_days       INT,                 -- дни с последней покупки; NULL если не покупал
  frequency_90d      INT    NOT NULL,     -- кол-во заказов за 90д
  monetary_180d      NUMERIC(12,2) NOT NULL,
  aov_180d           NUMERIC(12,2),       -- NULL если не было заказов в 180д
  orders_lifetime    INT    NOT NULL,
  revenue_lifetime   NUMERIC(12,2) NOT NULL,
  categories_unique  INT    NOT NULL,     -- уник. категорий в 180д (0 если не покупал)
  PRIMARY KEY (user_id, snapshot_date)
);

-- Только покупатели (≥1 успешный заказ за всё время)
CREATE TABLE IF NOT EXISTS ml_user_features_daily_buyers (
  user_id            BIGINT NOT NULL,
  snapshot_date      DATE   NOT NULL,
  recency_days       INT    NOT NULL,
  frequency_90d      INT    NOT NULL,
  monetary_180d      NUMERIC(12,2) NOT NULL,
  aov_180d           NUMERIC(12,2) NOT NULL,
  orders_lifetime    INT    NOT NULL,
  revenue_lifetime   NUMERIC(12,2) NOT NULL,
  categories_unique  INT    NOT NULL,
  PRIMARY KEY (user_id, snapshot_date)
);

-- Рекомендуемые индексы для ускорения расчётов
CREATE INDEX IF NOT EXISTS idx_orders_user_time  ON orders(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON orders(status);
CREATE INDEX IF NOT EXISTS idx_oi_order          ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_oi_product        ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(product_id, category);

-- Индексы для витринных таблиц
CREATE INDEX IF NOT EXISTS idx_ml_all_user_date ON ml_user_features_daily_all(user_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_ml_all_date ON ml_user_features_daily_all(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_ml_buyers_user_date ON ml_user_features_daily_buyers(user_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_ml_buyers_date ON ml_user_features_daily_buyers(snapshot_date);

-- Комментарии к таблицам
COMMENT ON TABLE ml_user_features_daily_all IS 'RFM-признаки для всех пользователей (включая без покупок)';
COMMENT ON TABLE ml_user_features_daily_buyers IS 'RFM-признаки только для покупателей (≥1 заказ)';

COMMENT ON COLUMN ml_user_features_daily_all.recency_days IS 'Дни с последней покупки (NULL если не покупал)';
COMMENT ON COLUMN ml_user_features_daily_all.frequency_90d IS 'Количество заказов за последние 90 дней';
COMMENT ON COLUMN ml_user_features_daily_all.monetary_180d IS 'Сумма заказов за последние 180 дней';
COMMENT ON COLUMN ml_user_features_daily_all.aov_180d IS 'Средний чек за последние 180 дней (NULL если не было заказов)';
COMMENT ON COLUMN ml_user_features_daily_all.orders_lifetime IS 'Общее количество заказов за всё время';
COMMENT ON COLUMN ml_user_features_daily_all.revenue_lifetime IS 'Общая сумма заказов за всё время';
COMMENT ON COLUMN ml_user_features_daily_all.categories_unique IS 'Количество уникальных категорий за последние 180 дней';
