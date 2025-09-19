-- =========================
-- Generate Target Labels for 6 Months (Weekly Snapshots) - Fixed
-- =========================
-- Упрощенная генерация таргетов purchase_next_30d для еженедельных снапшотов за 6 месяцев

-- ========= ГЕНЕРАЦИЯ ЕЖЕНЕДЕЛЬНЫХ СНАПШОТОВ =========
WITH weekly_snapshots AS (
  SELECT 
    generate_series(
      date_trunc('week', CURRENT_DATE - '6 months'::interval), 
      date_trunc('week', CURRENT_DATE - '30 days'::interval),  -- исключаем последние 30 дней
      '1 week'::interval
    )::date AS snapshot_date
),

-- ========= ПОЛУЧЕНИЕ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ =========
all_users AS (
  SELECT DISTINCT user_id 
  FROM users 
  WHERE user_id IS NOT NULL
),

-- ========= CROSS JOIN: КАЖДЫЙ ПОЛЬЗОВАТЕЛЬ × КАЖДЫЙ СНАПШОТ =========
user_snapshots AS (
  SELECT 
    u.user_id,
    ws.snapshot_date
  FROM all_users u
  CROSS JOIN weekly_snapshots ws
),

-- ========= ОПРЕДЕЛЕНИЕ ПЕРВОГО ЗАКАЗА В ЦЕЛЕВОМ ОКНЕ =========
target_orders AS (
  SELECT 
    us.user_id,
    us.snapshot_date,
    o.order_id,
    o.created_at::date AS order_date,
    -- Нумеруем заказы в окне по дате
    ROW_NUMBER() OVER (PARTITION BY us.user_id, us.snapshot_date ORDER BY o.created_at) AS order_rank
  FROM user_snapshots us
  JOIN orders o ON o.user_id = us.user_id
  WHERE 
    o.status IN ('paid', 'shipped', 'completed')
    -- Заказ в окне (snapshot_date+1, snapshot_date+30]
    AND o.created_at::date > us.snapshot_date 
    AND o.created_at::date <= (us.snapshot_date + '30 days'::interval)::date
)

-- ========= ВСТАВКА ДАННЫХ С ОБНОВЛЕНИЕМ =========
INSERT INTO ml_labels_purchase_30d (
  user_id, 
  snapshot_date, 
  purchase_next_30d, 
  first_order_id, 
  first_order_date
)
SELECT 
  us.user_id,
  us.snapshot_date,
  CASE 
    WHEN t.order_id IS NOT NULL THEN TRUE 
    ELSE FALSE 
  END AS purchase_next_30d,
  t.order_id AS first_order_id,
  t.order_date AS first_order_date
FROM user_snapshots us
LEFT JOIN target_orders t ON t.user_id = us.user_id 
                         AND t.snapshot_date = us.snapshot_date 
                         AND t.order_rank = 1  -- только первый заказ в окне
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  purchase_next_30d = EXCLUDED.purchase_next_30d,
  first_order_id = EXCLUDED.first_order_id,
  first_order_date = EXCLUDED.first_order_date;
