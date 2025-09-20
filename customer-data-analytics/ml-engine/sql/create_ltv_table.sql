-- =========================
-- LTV (Lifetime Value) Table
-- =========================
-- Создание таблицы для расчета и хранения LTV метрик пользователей

-- Создание таблицы LTV
CREATE TABLE IF NOT EXISTS ml_user_ltv (
  user_id              BIGINT NOT NULL,
  signup_date          DATE   NOT NULL,
  
  -- LTV по горизонтам (в USD)
  revenue_3m           NUMERIC(12,2) NOT NULL DEFAULT 0,    -- выручка за первые 3 месяца
  revenue_6m           NUMERIC(12,2) NOT NULL DEFAULT 0,    -- выручка за первые 6 месяцев
  revenue_12m          NUMERIC(12,2) NOT NULL DEFAULT 0,    -- выручка за первые 12 месяцев
  lifetime_revenue     NUMERIC(12,2) NOT NULL DEFAULT 0,    -- выручка за весь период
  
  -- Количество заказов
  orders_3m            INT NOT NULL DEFAULT 0,               -- заказы за 3 месяца
  orders_6m            INT NOT NULL DEFAULT 0,               -- заказы за 6 месяцев
  orders_12m           INT NOT NULL DEFAULT 0,               -- заказы за 12 месяцев
  orders_lifetime      INT NOT NULL DEFAULT 0,               -- заказы за весь период
  
  -- Средний чек
  avg_order_value_3m   NUMERIC(12,2),                        -- AOV за 3 месяца
  avg_order_value_6m   NUMERIC(12,2),                        -- AOV за 6 месяцев
  avg_order_value_12m  NUMERIC(12,2),                        -- AOV за 12 месяцев
  avg_order_value_lifetime NUMERIC(12,2),                    -- AOV за весь период
  
  -- Метаданные
  last_order_date      DATE,                                 -- дата последнего заказа
  first_order_date     DATE,                                 -- дата первого заказа
  days_since_last_order INT,                                 -- дней с последнего заказа
  
  -- Системные поля
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  -- Первичный ключ
  PRIMARY KEY (user_id)
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_ltv_signup_date ON ml_user_ltv(signup_date);
CREATE INDEX IF NOT EXISTS idx_ltv_revenue_6m ON ml_user_ltv(revenue_6m);
CREATE INDEX IF NOT EXISTS idx_ltv_revenue_12m ON ml_user_ltv(revenue_12m);
CREATE INDEX IF NOT EXISTS idx_ltv_lifetime_revenue ON ml_user_ltv(lifetime_revenue);
CREATE INDEX IF NOT EXISTS idx_ltv_orders_lifetime ON ml_user_ltv(orders_lifetime);

-- Комментарии к таблице
COMMENT ON TABLE ml_user_ltv IS 'LTV (Lifetime Value) метрики пользователей по горизонтам 3/6/12 месяцев и lifetime';
COMMENT ON COLUMN ml_user_ltv.revenue_3m IS 'Выручка за первые 3 месяца после регистрации';
COMMENT ON COLUMN ml_user_ltv.revenue_6m IS 'Выручка за первые 6 месяцев после регистрации';
COMMENT ON COLUMN ml_user_ltv.revenue_12m IS 'Выручка за первые 12 месяцев после регистрации';
COMMENT ON COLUMN ml_user_ltv.lifetime_revenue IS 'Общая выручка за весь период';
COMMENT ON COLUMN ml_user_ltv.avg_order_value_3m IS 'Средний чек за первые 3 месяца';
COMMENT ON COLUMN ml_user_ltv.avg_order_value_6m IS 'Средний чек за первые 6 месяцев';
COMMENT ON COLUMN ml_user_ltv.avg_order_value_12m IS 'Средний чек за первые 12 месяцев';
COMMENT ON COLUMN ml_user_ltv.avg_order_value_lifetime IS 'Средний чек за весь период';
