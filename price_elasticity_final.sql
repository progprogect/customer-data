-- =========================
-- Финальный анализ данных для ценовой эластичности
-- Customer Data Analytics
-- =========================

-- 1. ОСНОВНАЯ СТАТИСТИКА ПО ТОВАРАМ С ВАРИАЦИЯМИ ЦЕН
-- =========================

SELECT 
    'Товары с вариациями цен' as metric,
    COUNT(DISTINCT product_id) as count
FROM (
    SELECT product_id
    FROM order_items
    GROUP BY product_id
    HAVING COUNT(DISTINCT unit_price) > 1
) sub

UNION ALL

SELECT 
    'Всего товаров с заказами' as metric,
    COUNT(DISTINCT product_id) as count
FROM order_items

UNION ALL

SELECT 
    'Процент товаров с вариациями' as metric,
    ROUND(
        (SELECT COUNT(*) FROM (
            SELECT product_id FROM order_items 
            GROUP BY product_id 
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub)::float / 
        (SELECT COUNT(DISTINCT product_id) FROM order_items) * 100, 
        2
    ) as count;

-- =========================
-- 2. ТОП-20 ТОВАРОВ С НАИБОЛЬШИМИ ВАРИАЦИЯМИ ЦЕН
-- =========================

SELECT 
    product_id,
    COUNT(DISTINCT unit_price) AS price_points,
    MIN(unit_price) AS min_price,
    MAX(unit_price) AS max_price,
    ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) AS price_variance_pct,
    COUNT(*) AS total_orders
FROM order_items
GROUP BY product_id
HAVING COUNT(DISTINCT unit_price) > 1
ORDER BY price_points DESC, price_variance_pct DESC
LIMIT 20;

-- =========================
-- 3. АНАЛИЗ ПО КАТЕГОРИЯМ
-- =========================

SELECT 
    p.category,
    COUNT(DISTINCT oi.product_id) as products_with_variations,
    ROUND(AVG(price_variance_pct), 2) as avg_variance_pct,
    ROUND(MIN(price_variance_pct), 2) as min_variance_pct,
    ROUND(MAX(price_variance_pct), 2) as max_variance_pct
FROM (
    SELECT 
        product_id,
        ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) AS price_variance_pct
    FROM order_items
    GROUP BY product_id
    HAVING COUNT(DISTINCT unit_price) > 1
) price_analysis
JOIN products p ON price_analysis.product_id = p.product_id
GROUP BY p.category
ORDER BY avg_variance_pct DESC;

-- =========================
-- 4. АНАЛИЗ ТАБЛИЦЫ PRODUCT_PRICES
-- =========================

SELECT 
    'Товары с историей цен' as metric,
    COUNT(DISTINCT product_id) as count
FROM product_prices

UNION ALL

SELECT 
    'Всего записей истории цен' as metric,
    COUNT(*) as count
FROM product_prices

UNION ALL

SELECT 
    'Период данных (дни)' as metric,
    EXTRACT(DAYS FROM (MAX(valid_from) - MIN(valid_from))) as count
FROM product_prices;

-- =========================
-- 5. ИТОГОВАЯ ОЦЕНКА КАЧЕСТВА ДАННЫХ
-- =========================

WITH metrics AS (
    SELECT 
        (SELECT COUNT(DISTINCT product_id) FROM order_items) as total_products,
        (SELECT COUNT(*) FROM (
            SELECT product_id FROM order_items 
            GROUP BY product_id 
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub) as products_with_variations,
        (SELECT ROUND(AVG(price_points), 2) FROM (
            SELECT COUNT(DISTINCT unit_price) as price_points
            FROM order_items 
            GROUP BY product_id
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub) as avg_price_points,
        (SELECT ROUND(AVG(variance_pct), 2) FROM (
            SELECT 
                ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) as variance_pct
            FROM order_items 
            GROUP BY product_id
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub) as avg_variance_pct
)
SELECT 
    total_products,
    products_with_variations,
    ROUND(products_with_variations::float / total_products * 100, 2) as coverage_pct,
    avg_price_points,
    avg_variance_pct,
    CASE 
        WHEN products_with_variations::float / total_products > 0.3 
             AND avg_price_points >= 3 
             AND avg_variance_pct >= 5 
        THEN 'ОТЛИЧНО - данные подходят для анализа эластичности'
        WHEN products_with_variations::float / total_products > 0.2 
             AND avg_price_points >= 2 
             AND avg_variance_pct >= 3 
        THEN 'ХОРОШО - данные подходят с ограничениями'
        WHEN products_with_variations::float / total_products > 0.1 
             AND avg_price_points >= 2 
        THEN 'УДОВЛЕТВОРИТЕЛЬНО - нужны дополнительные данные'
        ELSE 'ПЛОХО - недостаточно данных для анализа эластичности'
    END as assessment
FROM metrics;
