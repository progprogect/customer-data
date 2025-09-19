-- =========================
-- Generate Target Labels for 6 Months (Weekly Snapshots)
-- =========================
-- Генерация таргетов purchase_next_30d для еженедельных снапшотов за 6 месяцев

-- ========= ПАРАМЕТРЫ =========
WITH params AS (
  SELECT
    30 ::int AS target_window_days,                    -- окно для таргета (30 дней)
    ARRAY['paid','shipped','completed']::text[] AS ok_status,  -- успешные статусы заказов
    6 ::int AS months_history                          -- глубина истории в месяцах
),

-- ========= ОПРЕДЕЛЕНИЕ ВРЕМЕННЫХ ГРАНИЦ =========
time_bounds AS (
  SELECT 
    p.target_window_days,
    p.ok_status,
    -- Находим максимальную дату заказа
    MAX(o.created_at::date) AS max_order_date,
    -- Последняя валидная дата снапшота (чтобы окно target было полным)
    (MAX(o.created_at::date) - (p.target_window_days || ' days')::interval)::date AS max_valid_snapshot_date,
    -- Начальная дата снапшота (6 месяцев назад от max_valid_snapshot_date)
    (MAX(o.created_at::date) - (p.target_window_days || ' days')::interval - (p.months_history || ' months')::interval)::date AS min_snapshot_date
  FROM orders o, params p
  WHERE o.status = ANY(p.ok_status)
),

-- ========= ГЕНЕРАЦИЯ ЕЖЕНЕДЕЛЬНЫХ СНАПШОТОВ (ПОНЕДЕЛЬНИКИ) =========
weekly_snapshots AS (
  SELECT 
    -- Генерируем понедельники в диапазоне
    (t.min_snapshot_date + ((generate_series(0, 
      EXTRACT(days FROM (t.max_valid_snapshot_date - t.min_snapshot_date))::int / 7) * 7) || ' days')::interval)::date AS snapshot_date,
    t.max_order_date,
    t.max_valid_snapshot_date,
    t.target_window_days,
    t.ok_status
  FROM time_bounds t
  WHERE 
    -- Проверяем, что дата попадает в валидный диапазон
    (t.min_snapshot_date + ((generate_series(0, 
      EXTRACT(days FROM (t.max_valid_snapshot_date - t.min_snapshot_date))::int / 7) * 7) || ' days')::interval)::date 
    <= t.max_valid_snapshot_date
    -- И что это понедельник (1 = понедельник в PostgreSQL)
    AND EXTRACT(dow FROM (t.min_snapshot_date + ((generate_series(0, 
      EXTRACT(days FROM (t.max_valid_snapshot_date - t.min_snapshot_date))::int / 7) * 7) || ' days')::interval)::date) = 1
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
    ws.snapshot_date,
    ws.target_window_days,
    ws.ok_status
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
    o.status = ANY(us.ok_status)
    -- Заказ в окне (snapshot_date+1, snapshot_date+30]
    AND o.created_at::date > us.snapshot_date 
    AND o.created_at::date <= (us.snapshot_date + (us.target_window_days || ' days')::interval)::date
),

-- ========= ФОРМИРОВАНИЕ ФИНАЛЬНЫХ ЛЕЙБЛОВ =========
final_labels AS (
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
  user_id,
  snapshot_date,
  purchase_next_30d,
  first_order_id,
  first_order_date
FROM final_labels
ON CONFLICT (user_id, snapshot_date) 
DO UPDATE SET 
  purchase_next_30d = EXCLUDED.purchase_next_30d,
  first_order_id = EXCLUDED.first_order_id,
  first_order_date = EXCLUDED.first_order_date;

-- ========= ЛОГИРОВАНИЕ СТАТИСТИКИ =========
DO $$
DECLARE
    total_rows INTEGER;
    positive_class_count INTEGER;
    negative_class_count INTEGER;
    positive_class_percent NUMERIC(5,2);
    min_snapshot DATE;
    max_snapshot DATE;
    max_order_in_db DATE;
    log_message TEXT;
BEGIN
    -- Получаем статистику
    SELECT COUNT(*) INTO total_rows FROM ml_labels_purchase_30d;
    
    SELECT COUNT(*) INTO positive_class_count 
    FROM ml_labels_purchase_30d 
    WHERE purchase_next_30d = TRUE;
    
    SELECT COUNT(*) INTO negative_class_count 
    FROM ml_labels_purchase_30d 
    WHERE purchase_next_30d = FALSE;
    
    positive_class_percent := (positive_class_count::NUMERIC / NULLIF(total_rows, 0)) * 100;
    
    SELECT MIN(snapshot_date), MAX(snapshot_date) 
    INTO min_snapshot, max_snapshot 
    FROM ml_labels_purchase_30d;
    
    SELECT MAX(created_at::date) 
    INTO max_order_in_db 
    FROM orders 
    WHERE status IN ('paid', 'shipped', 'completed');
    
    -- Формируем лог сообщение
    log_message := format(
        E'[ML TARGET LABELS GENERATED]\n' ||
        '📊 Общая статистика:\n' ||
        '  • Всего строк (срезов): %s\n' ||
        '  • Положительный класс (purchase_next_30d=1): %s\n' ||
        '  • Отрицательный класс (purchase_next_30d=0): %s\n' ||
        '  • Процент положительного класса: %s%%\n' ||
        '📅 Временные границы:\n' ||
        '  • Минимальный snapshot_date: %s\n' ||
        '  • Максимальный snapshot_date: %s\n' ||
        '  • Последний заказ в системе: %s\n' ||
        '✅ Генерация завершена успешно!',
        total_rows,
        positive_class_count,
        negative_class_count,
        positive_class_percent,
        min_snapshot,
        max_snapshot,
        max_order_in_db
    );
    
    -- Выводим в лог
    RAISE NOTICE '%', log_message;
    
    -- Проверяем критические условия
    IF positive_class_percent < 5 OR positive_class_percent > 30 THEN
        RAISE WARNING 'Процент положительного класса (%%) выходит за рекомендуемые границы 5-30%%', positive_class_percent;
    END IF;
    
    IF total_rows = 0 THEN
        RAISE EXCEPTION 'Не сгенерировано ни одной строки! Проверьте данные.';
    END IF;
    
END $$;
