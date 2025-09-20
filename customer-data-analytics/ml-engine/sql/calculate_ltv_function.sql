-- =========================
-- LTV Calculation Function
-- =========================
-- Функция для расчета LTV метрик пользователей

-- Функция расчета LTV для одного пользователя
CREATE OR REPLACE FUNCTION calculate_user_ltv(p_user_id BIGINT)
RETURNS TABLE (
  user_id              BIGINT,
  signup_date          DATE,
  revenue_3m           NUMERIC(12,2),
  revenue_6m           NUMERIC(12,2),
  revenue_12m          NUMERIC(12,2),
  lifetime_revenue     NUMERIC(12,2),
  orders_3m            INT,
  orders_6m            INT,
  orders_12m           INT,
  orders_lifetime      INT,
  avg_order_value_3m   NUMERIC(12,2),
  avg_order_value_6m   NUMERIC(12,2),
  avg_order_value_12m  NUMERIC(12,2),
  avg_order_value_lifetime NUMERIC(12,2),
  last_order_date      DATE,
  first_order_date     DATE,
  days_since_last_order INT
) AS $$
BEGIN
  RETURN QUERY
  WITH user_signup AS (
    SELECT 
      u.user_id,
      u.registered_at::DATE as signup_date
    FROM users u
    WHERE u.user_id = p_user_id
  ),
  user_orders AS (
    SELECT 
      o.user_id,
      o.created_at::DATE as order_date,
      o.total_amount,
      o.status
    FROM orders o
    WHERE o.user_id = p_user_id
      AND o.status IN ('paid', 'shipped', 'completed')
  ),
  ltv_calculations AS (
    SELECT 
      us.user_id,
      us.signup_date,
      
      -- Выручка по горизонтам
      COALESCE(SUM(CASE 
        WHEN uo.order_date <= us.signup_date + INTERVAL '3 months' 
        THEN uo.total_amount ELSE 0 END), 0) as revenue_3m,
      
      COALESCE(SUM(CASE 
        WHEN uo.order_date <= us.signup_date + INTERVAL '6 months' 
        THEN uo.total_amount ELSE 0 END), 0) as revenue_6m,
      
      COALESCE(SUM(CASE 
        WHEN uo.order_date <= us.signup_date + INTERVAL '12 months' 
        THEN uo.total_amount ELSE 0 END), 0) as revenue_12m,
      
      COALESCE(SUM(uo.total_amount), 0) as lifetime_revenue,
      
      -- Количество заказов по горизонтам
      COUNT(CASE 
        WHEN uo.order_date <= us.signup_date + INTERVAL '3 months' 
        THEN 1 END)::INT as orders_3m,
      
      COUNT(CASE 
        WHEN uo.order_date <= us.signup_date + INTERVAL '6 months' 
        THEN 1 END)::INT as orders_6m,
      
      COUNT(CASE 
        WHEN uo.order_date <= us.signup_date + INTERVAL '12 months' 
        THEN 1 END)::INT as orders_12m,
      
      COUNT(*)::INT as orders_lifetime,
      
      -- Даты заказов
      MAX(uo.order_date) as last_order_date,
      MIN(uo.order_date) as first_order_date
      
    FROM user_signup us
    LEFT JOIN user_orders uo ON us.user_id = uo.user_id
    GROUP BY us.user_id, us.signup_date
  )
  SELECT 
    lc.user_id,
    lc.signup_date,
    lc.revenue_3m,
    lc.revenue_6m,
    lc.revenue_12m,
    lc.lifetime_revenue,
    lc.orders_3m,
    lc.orders_6m,
    lc.orders_12m,
    lc.orders_lifetime,
    
    -- Средний чек (избегаем деления на ноль)
    CASE WHEN lc.orders_3m > 0 THEN lc.revenue_3m / lc.orders_3m ELSE 0 END as avg_order_value_3m,
    CASE WHEN lc.orders_6m > 0 THEN lc.revenue_6m / lc.orders_6m ELSE 0 END as avg_order_value_6m,
    CASE WHEN lc.orders_12m > 0 THEN lc.revenue_12m / lc.orders_12m ELSE 0 END as avg_order_value_12m,
    CASE WHEN lc.orders_lifetime > 0 THEN lc.lifetime_revenue / lc.orders_lifetime ELSE 0 END as avg_order_value_lifetime,
    
    lc.last_order_date,
    lc.first_order_date,
    CASE 
      WHEN lc.last_order_date IS NOT NULL 
      THEN (CURRENT_DATE - lc.last_order_date)::INT 
      ELSE NULL 
    END as days_since_last_order
    
  FROM ltv_calculations lc;
END;
$$ LANGUAGE plpgsql;

-- Функция для расчета LTV всех пользователей
CREATE OR REPLACE FUNCTION calculate_all_ltv()
RETURNS VOID AS $$
BEGIN
  -- Очищаем таблицу
  TRUNCATE TABLE ml_user_ltv;
  
  -- Вставляем данные для всех пользователей
  WITH ltv_data AS (
    SELECT 
      u.user_id,
      u.registered_at::DATE as signup_date,
      
      -- Выручка по горизонтам
      COALESCE(SUM(CASE 
        WHEN o.created_at::DATE <= u.registered_at::DATE + INTERVAL '3 months' 
        THEN o.total_amount ELSE 0 END), 0) as revenue_3m,
      
      COALESCE(SUM(CASE 
        WHEN o.created_at::DATE <= u.registered_at::DATE + INTERVAL '6 months' 
        THEN o.total_amount ELSE 0 END), 0) as revenue_6m,
      
      COALESCE(SUM(CASE 
        WHEN o.created_at::DATE <= u.registered_at::DATE + INTERVAL '12 months' 
        THEN o.total_amount ELSE 0 END), 0) as revenue_12m,
      
      COALESCE(SUM(o.total_amount), 0) as lifetime_revenue,
      
      -- Количество заказов по горизонтам
      COUNT(CASE 
        WHEN o.created_at::DATE <= u.registered_at::DATE + INTERVAL '3 months' 
        THEN 1 END)::INT as orders_3m,
      
      COUNT(CASE 
        WHEN o.created_at::DATE <= u.registered_at::DATE + INTERVAL '6 months' 
        THEN 1 END)::INT as orders_6m,
      
      COUNT(CASE 
        WHEN o.created_at::DATE <= u.registered_at::DATE + INTERVAL '12 months' 
        THEN 1 END)::INT as orders_12m,
      
      COUNT(*)::INT as orders_lifetime,
      
      -- Даты заказов
      MAX(o.created_at::DATE) as last_order_date,
      MIN(o.created_at::DATE) as first_order_date
      
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id 
      AND o.status IN ('paid', 'shipped', 'completed')
    GROUP BY u.user_id, u.registered_at::DATE
  )
  INSERT INTO ml_user_ltv (
    user_id, signup_date, revenue_3m, revenue_6m, revenue_12m, lifetime_revenue,
    orders_3m, orders_6m, orders_12m, orders_lifetime,
    avg_order_value_3m, avg_order_value_6m, avg_order_value_12m, avg_order_value_lifetime,
    last_order_date, first_order_date, days_since_last_order
  )
  SELECT 
    ld.user_id, ld.signup_date, ld.revenue_3m, ld.revenue_6m, ld.revenue_12m, ld.lifetime_revenue,
    ld.orders_3m, ld.orders_6m, ld.orders_12m, ld.orders_lifetime,
    
    -- Средний чек (избегаем деления на ноль)
    CASE WHEN ld.orders_3m > 0 THEN ld.revenue_3m / ld.orders_3m ELSE 0 END,
    CASE WHEN ld.orders_6m > 0 THEN ld.revenue_6m / ld.orders_6m ELSE 0 END,
    CASE WHEN ld.orders_12m > 0 THEN ld.revenue_12m / ld.orders_12m ELSE 0 END,
    CASE WHEN ld.orders_lifetime > 0 THEN ld.lifetime_revenue / ld.orders_lifetime ELSE 0 END,
    
    ld.last_order_date,
    ld.first_order_date,
    CASE 
      WHEN ld.last_order_date IS NOT NULL 
      THEN (CURRENT_DATE - ld.last_order_date)::INT 
      ELSE NULL 
    END
    
  FROM ltv_data ld;
  
  -- Обновляем updated_at
  UPDATE ml_user_ltv SET updated_at = now();
END;
$$ LANGUAGE plpgsql;

-- Функция для получения агрегированной статистики LTV
CREATE OR REPLACE FUNCTION get_ltv_summary()
RETURNS TABLE (
  metric_name           TEXT,
  value_3m             NUMERIC(12,2),
  value_6m             NUMERIC(12,2),
  value_12m            NUMERIC(12,2),
  value_lifetime       NUMERIC(12,2)
) AS $$
BEGIN
  RETURN QUERY
  WITH ltv_stats AS (
    SELECT 
      -- Средние значения
      AVG(revenue_3m) as avg_revenue_3m,
      AVG(revenue_6m) as avg_revenue_6m,
      AVG(revenue_12m) as avg_revenue_12m,
      AVG(lifetime_revenue) as avg_lifetime_revenue,
      
      -- Медианные значения
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_3m) as median_revenue_3m,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_6m) as median_revenue_6m,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue_12m) as median_revenue_12m,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_revenue) as median_lifetime_revenue,
      
      -- 95-й перцентиль
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY revenue_3m) as p95_revenue_3m,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY revenue_6m) as p95_revenue_6m,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY revenue_12m) as p95_revenue_12m,
      PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lifetime_revenue) as p95_lifetime_revenue,
      
      -- Количество пользователей
      COUNT(*) as total_users,
      COUNT(CASE WHEN revenue_3m > 0 THEN 1 END) as users_with_orders_3m,
      COUNT(CASE WHEN revenue_6m > 0 THEN 1 END) as users_with_orders_6m,
      COUNT(CASE WHEN revenue_12m > 0 THEN 1 END) as users_with_orders_12m,
      COUNT(CASE WHEN lifetime_revenue > 0 THEN 1 END) as users_with_orders_lifetime
      
    FROM ml_user_ltv
  )
  SELECT 'Average LTV'::TEXT, 
         ltv_stats.avg_revenue_3m::NUMERIC(12,2), ltv_stats.avg_revenue_6m::NUMERIC(12,2), 
         ltv_stats.avg_revenue_12m::NUMERIC(12,2), ltv_stats.avg_lifetime_revenue::NUMERIC(12,2)
  FROM ltv_stats
  
  UNION ALL
  
  SELECT 'Median LTV'::TEXT,
         ltv_stats.median_revenue_3m::NUMERIC(12,2), ltv_stats.median_revenue_6m::NUMERIC(12,2),
         ltv_stats.median_revenue_12m::NUMERIC(12,2), ltv_stats.median_lifetime_revenue::NUMERIC(12,2)
  FROM ltv_stats
  
  UNION ALL
  
  SELECT '95th Percentile LTV'::TEXT,
         ltv_stats.p95_revenue_3m::NUMERIC(12,2), ltv_stats.p95_revenue_6m::NUMERIC(12,2),
         ltv_stats.p95_revenue_12m::NUMERIC(12,2), ltv_stats.p95_lifetime_revenue::NUMERIC(12,2)
  FROM ltv_stats
  
  UNION ALL
  
  SELECT 'Users with Orders'::TEXT,
         ltv_stats.users_with_orders_3m::NUMERIC(12,2), 
         ltv_stats.users_with_orders_6m::NUMERIC(12,2),
         ltv_stats.users_with_orders_12m::NUMERIC(12,2), 
         ltv_stats.users_with_orders_lifetime::NUMERIC(12,2)
  FROM ltv_stats;
END;
$$ LANGUAGE plpgsql;

-- Комментарии к функциям
COMMENT ON FUNCTION calculate_user_ltv(BIGINT) IS 'Расчет LTV метрик для одного пользователя';
COMMENT ON FUNCTION calculate_all_ltv() IS 'Расчет LTV метрик для всех пользователей и заполнение таблицы ml_user_ltv';
COMMENT ON FUNCTION get_ltv_summary() IS 'Получение агрегированной статистики LTV по всем пользователям';
