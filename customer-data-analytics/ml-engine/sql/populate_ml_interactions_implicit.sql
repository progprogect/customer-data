-- =========================
-- Populate ML Interactions Implicit
-- Заполнение витрины взаимодействий данными
-- =========================

-- Очистка таблицы перед заполнением (для обновления)
DELETE FROM ml_interactions_implicit;

-- Заполнение витрины данными за последние 6 месяцев
-- Агрегируем по (user_id, product_id, date) для избежания дублей по времени
INSERT INTO ml_interactions_implicit (
    user_id,
    product_id, 
    event_ts,
    qty,
    price,
    amount,
    weight
)
SELECT 
    user_id,
    product_id,
    MAX(event_ts) as event_ts,  -- Берем последнее время покупки в этот день
    SUM(qty) as qty,            -- Суммируем количество за день
    AVG(price) as price,        -- Средняя цена за единицу в этот день
    SUM(amount) as amount,      -- Общая сумма за день
    1.0 as weight               -- Бинарный вес
FROM (
    SELECT 
        o.user_id,
        oi.product_id,
        o.created_at as event_ts,
        oi.quantity as qty,
        oi.unit_price as price,
        (oi.quantity * oi.unit_price) as amount,
        DATE(o.created_at) as purchase_date
    FROM orders o
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
    WHERE 
        -- Только успешные заказы
        o.status IN ('paid', 'completed')
        -- За последние 6 месяцев
        AND o.created_at >= NOW() - INTERVAL '6 months'
        -- Только активные товары
        AND p.is_active = true
        -- Исключаем некорректные данные
        AND oi.quantity > 0
        AND oi.unit_price >= 0
        AND o.user_id IS NOT NULL
        AND oi.product_id IS NOT NULL
) raw_data
GROUP BY user_id, product_id, purchase_date
ORDER BY event_ts DESC;

-- Обновляем статистику для оптимизатора
ANALYZE ml_interactions_implicit;

-- Показываем статистику заполнения
SELECT 
    'Interactions Summary' as summary_type,
    COUNT(*) as total_interactions,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT product_id) as unique_products,
    MIN(event_ts) as earliest_interaction,
    MAX(event_ts) as latest_interaction,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount
FROM ml_interactions_implicit;

-- Статистика плотности для CF
SELECT 
    'Density Analysis' as analysis_type,
    AVG(interactions_per_user) as avg_interactions_per_user,
    AVG(interactions_per_product) as avg_interactions_per_product,
    COUNT(*) as total_user_product_pairs
FROM (
    SELECT 
        user_id,
        COUNT(*) as interactions_per_user
    FROM ml_interactions_implicit 
    GROUP BY user_id
) user_stats
CROSS JOIN (
    SELECT 
        product_id,
        COUNT(*) as interactions_per_product
    FROM ml_interactions_implicit 
    GROUP BY product_id
) product_stats;

-- Проверка покрытия пользователей (≥2 товара для CF)
SELECT 
    'User Coverage Analysis' as analysis_type,
    COUNT(*) as users_with_2plus_products,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT user_id) FROM ml_interactions_implicit), 2) as coverage_percent
FROM (
    SELECT user_id
    FROM ml_interactions_implicit
    GROUP BY user_id
    HAVING COUNT(DISTINCT product_id) >= 2
) users_cf_ready;
