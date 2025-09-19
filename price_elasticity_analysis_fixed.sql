-- =========================
-- Анализ данных для ценовой эластичности (исправленная версия)
-- Customer Data Analytics
-- =========================

-- 1. АНАЛИЗ СТРУКТУРЫ ДАННЫХ
-- =========================

-- Проверяем наличие данных в основных таблицах
SELECT 
    'products' as table_name,
    COUNT(*) as total_records,
    COUNT(price) as price_records,
    MIN(price) as min_price,
    MAX(price) as max_price,
    ROUND(AVG(price), 2) as avg_price
FROM products
WHERE is_active = true

UNION ALL

SELECT 
    'order_items' as table_name,
    COUNT(*) as total_records,
    COUNT(unit_price) as price_records,
    MIN(unit_price) as min_price,
    MAX(unit_price) as max_price,
    ROUND(AVG(unit_price), 2) as avg_price
FROM order_items

UNION ALL

SELECT 
    'product_prices' as table_name,
    COUNT(*) as total_records,
    COUNT(price) as price_records,
    MIN(price) as min_price,
    MAX(price) as max_price,
    ROUND(AVG(price), 2) as avg_price
FROM product_prices;

-- =========================
-- 2. АНАЛИЗ ВАРИАЦИЙ ЦЕН В ORDER_ITEMS
-- =========================

-- Основной анализ: сколько уникальных цен у каждого товара
SELECT 
    product_id,
    COUNT(DISTINCT unit_price) AS price_points,
    MIN(unit_price) AS min_price,
    MAX(unit_price) AS max_price,
    ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) AS price_variance_pct,
    COUNT(*) AS total_orders,
    MIN(o.created_at) AS first_order,
    MAX(o.created_at) AS last_order
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
GROUP BY product_id
HAVING COUNT(DISTINCT unit_price) > 1  -- только товары с вариациями цен
ORDER BY price_points DESC, price_variance_pct DESC
LIMIT 20;

-- =========================
-- 3. ВРЕМЕННОЙ АНАЛИЗ ИЗМЕНЕНИЙ ЦЕН
-- =========================

-- Анализ частоты изменений цен по месяцам
WITH monthly_price_changes AS (
    SELECT 
        product_id,
        DATE_TRUNC('month', o.created_at) as month,
        COUNT(DISTINCT unit_price) as unique_prices_in_month,
        MIN(unit_price) as min_price_month,
        MAX(unit_price) as max_price_month
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    GROUP BY product_id, DATE_TRUNC('month', o.created_at)
    HAVING COUNT(DISTINCT unit_price) > 1
)
SELECT 
    product_id,
    COUNT(*) as months_with_changes,
    ROUND(AVG(unique_prices_in_month), 2) as avg_price_changes_per_month,
    MIN(min_price_month) as global_min_price,
    MAX(max_price_month) as global_max_price,
    ROUND((MAX(max_price_month) - MIN(min_price_month)) / MIN(min_price_month) * 100, 2) as total_variance_pct
FROM monthly_price_changes
GROUP BY product_id
ORDER BY months_with_changes DESC, total_variance_pct DESC
LIMIT 20;

-- =========================
-- 4. АНАЛИЗ ПО КАТЕГОРИЯМ
-- =========================

-- Вариации цен по категориям товаров
SELECT 
    p.category,
    COUNT(DISTINCT oi.product_id) as products_with_variations,
    COUNT(DISTINCT oi.unit_price) as total_unique_prices,
    ROUND(AVG(price_variance_pct), 2) as avg_variance_pct,
    ROUND(MIN(price_variance_pct), 2) as min_variance_pct,
    ROUND(MAX(price_variance_pct), 2) as max_variance_pct
FROM (
    SELECT 
        product_id,
        COUNT(DISTINCT unit_price) AS price_points,
        MIN(unit_price) AS min_price,
        MAX(unit_price) AS max_price,
        ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) AS price_variance_pct
    FROM order_items
    GROUP BY product_id
    HAVING COUNT(DISTINCT unit_price) > 1
) price_analysis
JOIN products p ON price_analysis.product_id = p.product_id
GROUP BY p.category
ORDER BY avg_variance_pct DESC;

-- =========================
-- 5. АНАЛИЗ ТАБЛИЦЫ PRODUCT_PRICES
-- =========================

-- Проверяем данные в специальной таблице истории цен
SELECT 
    COUNT(DISTINCT product_id) as products_with_price_history,
    COUNT(*) as total_price_records,
    MIN(valid_from) as earliest_date,
    MAX(valid_from) as latest_date,
    ROUND(AVG(price), 2) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM product_prices;

-- Детальный анализ по товарам в product_prices
SELECT 
    product_id,
    COUNT(*) as price_changes,
    MIN(valid_from) as first_change,
    MAX(valid_from) as last_change,
    MIN(price) as min_price,
    MAX(price) as max_price,
    ROUND((MAX(price) - MIN(price)) / MIN(price) * 100, 2) as variance_pct
FROM product_prices
GROUP BY product_id
ORDER BY price_changes DESC, variance_pct DESC
LIMIT 20;

-- =========================
-- 6. СРАВНЕНИЕ ИСТОЧНИКОВ ЦЕН
-- =========================

-- Сравниваем цены между products и order_items
SELECT 
    p.product_id,
    p.price as current_price,
    ROUND(AVG(oi.unit_price), 2) as avg_historical_price,
    MIN(oi.unit_price) as min_historical_price,
    MAX(oi.unit_price) as max_historical_price,
    ROUND((p.price - AVG(oi.unit_price)) / AVG(oi.unit_price) * 100, 2) as current_vs_avg_diff_pct
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.price
HAVING COUNT(oi.unit_price) > 0
ORDER BY ABS((p.price - AVG(oi.unit_price)) / AVG(oi.unit_price) * 100) DESC
LIMIT 20;

-- =========================
-- 7. ОБЩАЯ ОЦЕНКА КАЧЕСТВА ДАННЫХ
-- =========================

-- Итоговая статистика для принятия решения
WITH price_quality_metrics AS (
    SELECT 
        -- Общие метрики
        (SELECT COUNT(DISTINCT product_id) FROM order_items) as total_products_with_orders,
        (SELECT COUNT(*) FROM (
            SELECT product_id FROM order_items 
            GROUP BY product_id 
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub) as products_with_price_variations,
        
        -- Метрики вариаций
        (SELECT ROUND(AVG(price_points), 2) FROM (
            SELECT COUNT(DISTINCT unit_price) as price_points
            FROM order_items 
            GROUP BY product_id
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub) as avg_price_points,
        
        -- Метрики временного охвата
        (SELECT EXTRACT(DAYS FROM (MAX(created_at) - MIN(created_at))) as data_span_days FROM orders) as data_span_days,
        
        -- Метрики диапазона цен
        (SELECT ROUND(AVG(variance_pct), 2) FROM (
            SELECT 
                ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) as variance_pct
            FROM order_items 
            GROUP BY product_id
            HAVING COUNT(DISTINCT unit_price) > 1
        ) sub) as avg_price_variance_pct
)
SELECT 
    total_products_with_orders,
    products_with_price_variations,
    ROUND(products_with_price_variations::float / total_products_with_orders * 100, 2) as variation_coverage_pct,
    avg_price_points,
    data_span_days,
    avg_price_variance_pct,
    
    -- Оценка качества данных
    CASE 
        WHEN products_with_price_variations::float / total_products_with_orders > 0.3 
             AND avg_price_points >= 3 
             AND avg_price_variance_pct >= 5 
        THEN 'ОТЛИЧНО - данные подходят для анализа эластичности'
        WHEN products_with_price_variations::float / total_products_with_orders > 0.2 
             AND avg_price_points >= 2 
             AND avg_price_variance_pct >= 3 
        THEN 'ХОРОШО - данные подходят с ограничениями'
        WHEN products_with_price_variations::float / total_products_with_orders > 0.1 
             AND avg_price_points >= 2 
        THEN 'УДОВЛЕТВОРИТЕЛЬНО - нужны дополнительные данные'
        ELSE 'ПЛОХО - недостаточно данных для анализа эластичности'
    END as data_quality_assessment
FROM price_quality_metrics;
