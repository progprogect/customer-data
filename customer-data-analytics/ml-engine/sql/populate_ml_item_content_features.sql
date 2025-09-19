-- =========================
-- Populate ML Item Content Features
-- Заполнение витрины признаков товаров
-- =========================

-- Очистка таблицы перед заполнением
DELETE FROM ml_item_content_features;

-- Заполнение витрины с нормализованными данными
INSERT INTO ml_item_content_features (
    product_id,
    brand,
    category, 
    color,
    size,
    material,
    gender,
    style,
    price_current,
    rating,
    tags_normalized,
    tags_count,
    title,
    description_short,
    is_active
)
SELECT 
    p.product_id,
    normalize_text_field(p.brand) as brand,
    normalize_text_field(p.category) as category,
    normalize_text_field(p.color) as color,
    normalize_text_field(p.size) as size,
    normalize_text_field(p.material) as material,
    normalize_text_field(p.gender) as gender,
    normalize_text_field(p.style) as style,
    p.price as price_current,
    p.rating,
    normalize_tags(p.tags) as tags_normalized,
    COALESCE(array_length(normalize_tags(p.tags), 1), 0) as tags_count,
    p.title,
    create_short_description(p.title, p.description) as description_short,
    p.is_active
FROM products p
WHERE p.is_active = true  -- Только активные товары
ORDER BY p.product_id;

-- Вычисляем популярность товаров за последние 30 дней
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

-- Вычисляем среднюю цену за последние 90 дней (пока копируем текущую)
UPDATE ml_item_content_features 
SET price_mean_90d = price_current;

-- Создаем JSON с масштабированными числовыми признаками
-- Пока простое решение - нормализуем по min-max в диапазоне [0,1]
WITH price_stats AS (
    SELECT 
        MIN(price_current) as min_price,
        MAX(price_current) as max_price,
        MIN(COALESCE(popularity_30d, 0)) as min_popularity,
        MAX(COALESCE(popularity_30d, 0)) as max_popularity,
        MIN(COALESCE(rating, 0)) as min_rating,
        MAX(COALESCE(rating, 5)) as max_rating
    FROM ml_item_content_features
    WHERE price_current IS NOT NULL
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
    LEAST(mcf.tags_count / 10.0, 1.0)  -- нормализуем количество тегов, max=10
)
FROM price_stats ps;

-- Обновляем статистику
ANALYZE ml_item_content_features;

-- Показываем результаты заполнения
SELECT 
    'Content Features Summary' as summary_type,
    COUNT(*) as total_products,
    COUNT(*) FILTER (WHERE brand IS NOT NULL) as with_brand,
    COUNT(*) FILTER (WHERE category IS NOT NULL) as with_category,
    COUNT(*) FILTER (WHERE tags_count > 0) as with_tags,
    COUNT(*) FILTER (WHERE popularity_30d > 0) as with_sales_30d,
    AVG(price_current) as avg_price,
    AVG(popularity_30d) as avg_popularity_30d,
    AVG(tags_count) as avg_tags_count
FROM ml_item_content_features;

-- Статистика по категориям
SELECT 
    'Category Distribution' as analysis_type,
    category,
    COUNT(*) as product_count,
    AVG(price_current) as avg_price,
    AVG(popularity_30d) as avg_popularity
FROM ml_item_content_features
WHERE category IS NOT NULL
GROUP BY category
ORDER BY product_count DESC
LIMIT 10;

-- Топ теги
SELECT 
    'Top Tags' as analysis_type,
    unnest(tags_normalized) as tag,
    COUNT(*) as usage_count
FROM ml_item_content_features
WHERE tags_count > 0
GROUP BY unnest(tags_normalized)
ORDER BY usage_count DESC
LIMIT 15;
