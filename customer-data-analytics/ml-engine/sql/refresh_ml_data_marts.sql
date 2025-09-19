-- =========================
-- Refresh ML Data Marts
-- Скрипт для периодического обновления витрин рекомендательной системы
-- =========================

-- Можно запускать ежедневно или по требованию
-- Рекомендуется: ежедневно в 2:00 AM для обновления витрин

BEGIN;

-- Логирование начала обновления
INSERT INTO ml_refresh_log (refresh_type, status, started_at) 
VALUES ('full_refresh', 'started', NOW());

-- ========================================
-- 1. ОБНОВЛЕНИЕ ВИТРИНЫ ВЗАИМОДЕЙСТВИЙ
-- ========================================

-- Создаем временную таблицу для новых данных
CREATE TEMP TABLE temp_interactions_update AS
SELECT 
    user_id,
    product_id,
    MAX(event_ts) as event_ts,
    SUM(qty) as qty,
    AVG(price) as price,
    SUM(amount) as amount,
    1.0 as weight,
    purchase_date
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
        -- Только данные новее чем уже есть в витрине (инкрементальное обновление)
        AND o.created_at > COALESCE(
            (SELECT MAX(event_ts) FROM ml_interactions_implicit), 
            '1900-01-01'::timestamptz
        )
) raw_data
GROUP BY user_id, product_id, purchase_date;

-- Вставляем новые взаимодействия
INSERT INTO ml_interactions_implicit (
    user_id, product_id, event_ts, qty, price, amount, weight
)
SELECT 
    user_id, product_id, event_ts, qty, price, amount, weight
FROM temp_interactions_update
ON CONFLICT (user_id, product_id, event_ts) DO UPDATE SET
    qty = EXCLUDED.qty + ml_interactions_implicit.qty,
    amount = EXCLUDED.amount + ml_interactions_implicit.amount,
    updated_at = NOW();

-- Удаляем старые данные (>6 месяцев)
DELETE FROM ml_interactions_implicit 
WHERE event_ts < NOW() - INTERVAL '6 months';

-- ========================================
-- 2. ОБНОВЛЕНИЕ ВИТРИНЫ ПРИЗНАКОВ ТОВАРОВ
-- ========================================

-- Обновляем существующие товары (может измениться price, rating, etc.)
UPDATE ml_item_content_features mcf
SET 
    brand = normalize_text_field(p.brand),
    category = normalize_text_field(p.category),
    color = normalize_text_field(p.color),
    size = normalize_text_field(p.size),
    material = normalize_text_field(p.material),
    gender = normalize_text_field(p.gender),
    style = normalize_text_field(p.style),
    price_current = p.price,
    rating = p.rating,
    tags_normalized = normalize_tags(p.tags),
    tags_count = COALESCE(array_length(normalize_tags(p.tags), 1), 0),
    title = p.title,
    description_short = create_short_description(p.title, p.description),
    is_active = p.is_active,
    updated_at = NOW()
FROM products p
WHERE mcf.product_id = p.product_id
    AND p.is_active = true;

-- Добавляем новые активные товары
INSERT INTO ml_item_content_features (
    product_id, brand, category, color, size, material, gender, style,
    price_current, rating, tags_normalized, tags_count, title, 
    description_short, is_active
)
SELECT 
    p.product_id,
    normalize_text_field(p.brand),
    normalize_text_field(p.category),
    normalize_text_field(p.color),
    normalize_text_field(p.size),
    normalize_text_field(p.material),
    normalize_text_field(p.gender),
    normalize_text_field(p.style),
    p.price,
    p.rating,
    normalize_tags(p.tags),
    COALESCE(array_length(normalize_tags(p.tags), 1), 0),
    p.title,
    create_short_description(p.title, p.description),
    p.is_active
FROM products p
LEFT JOIN ml_item_content_features mcf ON p.product_id = mcf.product_id
WHERE p.is_active = true 
    AND mcf.product_id IS NULL;

-- Деактивируем товары, которые стали неактивными
UPDATE ml_item_content_features mcf
SET is_active = false, updated_at = NOW()
FROM products p
WHERE mcf.product_id = p.product_id
    AND p.is_active = false
    AND mcf.is_active = true;

-- ========================================
-- 3. ОБНОВЛЕНИЕ ПОПУЛЯРНОСТИ И МЕТРИК
-- ========================================

-- Пересчитываем популярность за последние 30 дней
UPDATE ml_item_content_features mcf
SET popularity_30d = COALESCE(popularity_stats.total_amount, 0)
FROM (
    SELECT 
        mii.product_id,
        SUM(mii.amount) as total_amount
    FROM ml_interactions_implicit mii
    WHERE mii.event_ts >= NOW() - INTERVAL '30 days'
    GROUP BY mii.product_id
) popularity_stats
WHERE mcf.product_id = popularity_stats.product_id;

-- Устанавливаем 0 для товаров без продаж
UPDATE ml_item_content_features 
SET popularity_30d = 0 
WHERE popularity_30d IS NULL;

-- Пересчитываем нормализованные числовые признаки
WITH price_stats AS (
    SELECT 
        MIN(price_current) as min_price,
        MAX(price_current) as max_price,
        MIN(COALESCE(popularity_30d, 0)) as min_popularity,
        MAX(COALESCE(popularity_30d, 0)) as max_popularity,
        MIN(COALESCE(rating, 0)) as min_rating,
        MAX(COALESCE(rating, 5)) as max_rating
    FROM ml_item_content_features
    WHERE price_current IS NOT NULL AND is_active = true
)
UPDATE ml_item_content_features mcf
SET numeric_features = jsonb_build_object(
    'price_normalized', 
    CASE 
        WHEN ps.max_price > ps.min_price THEN 
            (mcf.price_current - ps.min_price) / (ps.max_price - ps.min_price)
        ELSE 0.5 
    END,
    'popularity_normalized',
    CASE 
        WHEN ps.max_popularity > ps.min_popularity THEN 
            (COALESCE(mcf.popularity_30d, 0) - ps.min_popularity) / (ps.max_popularity - ps.min_popularity)
        ELSE 0.0
    END,
    'rating_normalized',
    CASE 
        WHEN ps.max_rating > ps.min_rating THEN 
            (COALESCE(mcf.rating, 2.5) - ps.min_rating) / (ps.max_rating - ps.min_rating)
        ELSE 0.5
    END,
    'tags_count_normalized',
    LEAST(mcf.tags_count / 10.0, 1.0)
)
FROM price_stats ps
WHERE mcf.is_active = true;

-- ========================================
-- 4. ОБНОВЛЕНИЕ СТАТИСТИКИ И ЗАВЕРШЕНИЕ
-- ========================================

-- Обновляем статистику таблиц
ANALYZE ml_interactions_implicit;
ANALYZE ml_item_content_features;

-- Логируем успешное завершение
UPDATE ml_refresh_log 
SET status = 'completed', completed_at = NOW(), 
    details = jsonb_build_object(
        'interactions_count', (SELECT COUNT(*) FROM ml_interactions_implicit),
        'products_count', (SELECT COUNT(*) FROM ml_item_content_features WHERE is_active = true)
    )
WHERE refresh_type = 'full_refresh' 
    AND status = 'started' 
    AND started_at::date = NOW()::date;

COMMIT;

-- Показываем результаты обновления
SELECT 
    'Data Marts Refresh Completed' as status,
    COUNT(*) as interactions_total,
    COUNT(DISTINCT user_id) as users_unique,
    COUNT(DISTINCT product_id) as products_unique,
    MAX(event_ts) as latest_interaction
FROM ml_interactions_implicit;

SELECT 
    'Content Features Updated' as status,
    COUNT(*) as total_products,
    COUNT(*) FILTER (WHERE is_active = true) as active_products,
    COUNT(*) FILTER (WHERE popularity_30d > 0) as products_with_sales_30d,
    AVG(popularity_30d) as avg_popularity_30d
FROM ml_item_content_features;
