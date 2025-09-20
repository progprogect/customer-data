-- =========================
-- ML User Behavior Weekly Table
-- =========================
-- Создание недельной витрины поведения пользователей для детекции аномалий

-- Таблица для недельной агрегации поведения пользователей
CREATE TABLE IF NOT EXISTS ml_user_behavior_weekly (
  user_id            BIGINT NOT NULL,
  week_start_date    DATE NOT NULL,
  orders_count       INT NOT NULL DEFAULT 0,
  monetary_sum       NUMERIC(12,2) NOT NULL DEFAULT 0,
  categories_count   INT NOT NULL DEFAULT 0,
  aov_weekly         NUMERIC(12,2),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  PRIMARY KEY (user_id, week_start_date)
);

-- Комментарии к полям
COMMENT ON TABLE ml_user_behavior_weekly IS 'Недельная витрина поведения пользователей для детекции аномалий';
COMMENT ON COLUMN ml_user_behavior_weekly.user_id IS 'ID пользователя';
COMMENT ON COLUMN ml_user_behavior_weekly.week_start_date IS 'Начало недели (понедельник) в UTC';
COMMENT ON COLUMN ml_user_behavior_weekly.orders_count IS 'Количество заказов за неделю';
COMMENT ON COLUMN ml_user_behavior_weekly.monetary_sum IS 'Сумма покупок за неделю';
COMMENT ON COLUMN ml_user_behavior_weekly.categories_count IS 'Количество уникальных категорий за неделю';
COMMENT ON COLUMN ml_user_behavior_weekly.aov_weekly IS 'Средний чек за неделю (NULL если нет заказов)';

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_behavior_weekly_user_week ON ml_user_behavior_weekly(user_id, week_start_date);
CREATE INDEX IF NOT EXISTS idx_behavior_weekly_week ON ml_user_behavior_weekly(week_start_date);
CREATE INDEX IF NOT EXISTS idx_behavior_weekly_orders ON ml_user_behavior_weekly(orders_count);
CREATE INDEX IF NOT EXISTS idx_behavior_weekly_monetary ON ml_user_behavior_weekly(monetary_sum);

-- Функция для заполнения витрины
CREATE OR REPLACE FUNCTION populate_behavior_weekly()
RETURNS void AS $$
BEGIN
  -- Очищаем таблицу перед заполнением
  TRUNCATE ml_user_behavior_weekly;
  
  -- Заполняем данными за последние 6 месяцев
  INSERT INTO ml_user_behavior_weekly (
    user_id,
    week_start_date,
    orders_count,
    monetary_sum,
    categories_count,
    aov_weekly
  )
  SELECT 
    o.user_id,
    DATE_TRUNC('week', o.created_at)::DATE AS week_start_date,
    COUNT(DISTINCT o.order_id) AS orders_count,
    COALESCE(SUM(o.total_amount), 0) AS monetary_sum,
    COUNT(DISTINCT p.category) AS categories_count,
    CASE 
      WHEN COUNT(DISTINCT o.order_id) > 0 
      THEN SUM(o.total_amount) / COUNT(DISTINCT o.order_id)
      ELSE NULL 
    END AS aov_weekly
  FROM orders o
  LEFT JOIN order_items oi ON o.order_id = oi.order_id
  LEFT JOIN products p ON oi.product_id = p.product_id
  WHERE o.status = 'completed'
    AND o.created_at >= NOW() - INTERVAL '6 months'
  GROUP BY o.user_id, DATE_TRUNC('week', o.created_at)::DATE
  ORDER BY o.user_id, week_start_date;
  
  -- Логируем результат
  RAISE NOTICE 'Заполнено % записей в ml_user_behavior_weekly', 
    (SELECT COUNT(*) FROM ml_user_behavior_weekly);
END;
$$ LANGUAGE plpgsql;

-- Функция для обновления витрины (добавляет только новые данные)
CREATE OR REPLACE FUNCTION update_behavior_weekly()
RETURNS void AS $$
DECLARE
  last_week DATE;
BEGIN
  -- Находим последнюю неделю в витрине
  SELECT COALESCE(MAX(week_start_date), '1900-01-01'::DATE) 
  INTO last_week 
  FROM ml_user_behavior_weekly;
  
  -- Добавляем только новые данные
  INSERT INTO ml_user_behavior_weekly (
    user_id,
    week_start_date,
    orders_count,
    monetary_sum,
    categories_count,
    aov_weekly
  )
  SELECT 
    o.user_id,
    DATE_TRUNC('week', o.created_at)::DATE AS week_start_date,
    COUNT(DISTINCT o.order_id) AS orders_count,
    COALESCE(SUM(o.total_amount), 0) AS monetary_sum,
    COUNT(DISTINCT p.category) AS categories_count,
    CASE 
      WHEN COUNT(DISTINCT o.order_id) > 0 
      THEN SUM(o.total_amount) / COUNT(DISTINCT o.order_id)
      ELSE NULL 
    END AS aov_weekly
  FROM orders o
  LEFT JOIN order_items oi ON o.order_id = oi.order_id
  LEFT JOIN products p ON oi.product_id = p.product_id
  WHERE o.status = 'completed'
    AND o.created_at >= NOW() - INTERVAL '6 months'
    AND DATE_TRUNC('week', o.created_at)::DATE > last_week
  GROUP BY o.user_id, DATE_TRUNC('week', o.created_at)::DATE
  ORDER BY o.user_id, week_start_date
  ON CONFLICT (user_id, week_start_date) DO UPDATE SET
    orders_count = EXCLUDED.orders_count,
    monetary_sum = EXCLUDED.monetary_sum,
    categories_count = EXCLUDED.categories_count,
    aov_weekly = EXCLUDED.aov_weekly,
    created_at = NOW();
  
  -- Логируем результат
  RAISE NOTICE 'Обновлено ml_user_behavior_weekly, последняя неделя: %', 
    (SELECT MAX(week_start_date) FROM ml_user_behavior_weekly);
END;
$$ LANGUAGE plpgsql;
