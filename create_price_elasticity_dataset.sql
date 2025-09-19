-- =========================
-- Создание агрегированного датасета для анализа ценовой эластичности
-- Customer Data Analytics - Шаг 2
-- =========================

-- 1. АНАЛИЗ ПРИОРИТЕТНЫХ КАТЕГОРИЙ ДЛЯ MVP
-- =========================

-- Выбираем категории с наибольшими вариациями цен и достаточным объемом данных
WITH category_metrics AS (
    SELECT 
        p.category,
        COUNT(DISTINCT p.product_id) as total_products,
        COUNT(DISTINCT oi.product_id) as products_with_orders,
        COUNT(oi.order_item_id) as total_order_items,
        ROUND(AVG(price_variance_pct), 2) as avg_price_variance,
        ROUND(MAX(price_variance_pct), 2) as max_price_variance,
        MIN(oi.created_at) as first_order,
        MAX(oi.created_at) as last_order
    FROM products p
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    LEFT JOIN orders o ON oi.order_id = o.order_id
    LEFT JOIN (
        SELECT 
            product_id,
            ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) AS price_variance_pct
        FROM order_items
        GROUP BY product_id
        HAVING COUNT(DISTINCT unit_price) > 1
    ) price_analysis ON p.product_id = price_analysis.product_id
    WHERE p.is_active = true
    GROUP BY p.category
    HAVING COUNT(oi.order_item_id) > 100  -- минимум 100 заказов
)
SELECT 
    category,
    total_products,
    products_with_orders,
    total_order_items,
    avg_price_variance,
    max_price_variance,
    (last_order - first_order) as data_span_days
FROM category_metrics
ORDER BY avg_price_variance DESC, total_order_items DESC
LIMIT 10;

-- =========================
-- 2. СОЗДАНИЕ ВИТРИНЫ ML_PRICE_ELASTICITY
-- =========================

-- Создаем таблицу для агрегированных данных
CREATE TABLE IF NOT EXISTS ml_price_elasticity (
    category VARCHAR(50) NOT NULL,
    week_start DATE NOT NULL,
    avg_price NUMERIC(12,2),
    units_sold INTEGER DEFAULT 0,
    revenue NUMERIC(12,2) DEFAULT 0,
    price_changes_count INTEGER DEFAULT 0,
    orders_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (category, week_start)
);

-- Создаем индексы для производительности
CREATE INDEX IF NOT EXISTS idx_ml_price_elasticity_category_week 
    ON ml_price_elasticity(category, week_start);
CREATE INDEX IF NOT EXISTS idx_ml_price_elasticity_week 
    ON ml_price_elasticity(week_start);

-- =========================
-- 3. АГРЕГАЦИЯ ДАННЫХ ПО НЕДЕЛЯМ
-- =========================

-- Сначала очищаем таблицу (для пересчета)
TRUNCATE TABLE ml_price_elasticity;

-- Создаем CTE для агрегации данных по неделям
WITH weekly_sales AS (
    -- Агрегируем продажи по категориям и неделям
    SELECT 
        p.category,
        DATE_TRUNC('week', o.created_at)::DATE as week_start,
        SUM(oi.quantity) as units_sold,
        SUM(oi.quantity * oi.unit_price) as revenue,
        COUNT(DISTINCT oi.order_id) as orders_count,
        AVG(oi.unit_price) as avg_selling_price
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE p.is_active = true
        AND o.status IN ('completed', 'paid', 'shipped')
        AND o.created_at >= '2024-01-01'  -- последний год
    GROUP BY p.category, DATE_TRUNC('week', o.created_at)::DATE
),
weekly_prices AS (
    -- Агрегируем цены по категориям и неделям
    SELECT 
        p.category,
        DATE_TRUNC('week', pp.valid_from)::DATE as week_start,
        AVG(pp.price) as avg_price,
        COUNT(DISTINCT pp.product_id) as price_changes_count
    FROM product_prices pp
    JOIN products p ON pp.product_id = p.product_id
    WHERE p.is_active = true
        AND pp.valid_from >= '2024-01-01'
    GROUP BY p.category, DATE_TRUNC('week', pp.valid_from)::DATE
),
all_weeks AS (
    -- Генерируем все недели для заполнения пропусков
    SELECT DISTINCT
        p.category,
        week_start
    FROM (
        SELECT category FROM products WHERE is_active = true
    ) p
    CROSS JOIN (
        SELECT generate_series(
            '2024-01-01'::DATE,
            CURRENT_DATE,
            '1 week'::INTERVAL
        )::DATE as week_start
    ) weeks
),
combined_data AS (
    -- Объединяем продажи и цены
    SELECT 
        aw.category,
        aw.week_start,
        COALESCE(wp.avg_price, 0) as avg_price,
        COALESCE(ws.units_sold, 0) as units_sold,
        COALESCE(ws.revenue, 0) as revenue,
        COALESCE(wp.price_changes_count, 0) as price_changes_count,
        COALESCE(ws.orders_count, 0) as orders_count
    FROM all_weeks aw
    LEFT JOIN weekly_sales ws ON aw.category = ws.category AND aw.week_start = ws.week_start
    LEFT JOIN weekly_prices wp ON aw.category = wp.category AND aw.week_start = wp.week_start
)
-- Вставляем данные в витрину
INSERT INTO ml_price_elasticity (
    category, week_start, avg_price, units_sold, revenue, 
    price_changes_count, orders_count
)
SELECT 
    category, week_start, avg_price, units_sold, revenue,
    price_changes_count, orders_count
FROM combined_data
WHERE category IN (
    -- Берем только приоритетные категории
    SELECT category FROM (
        SELECT 
            p.category,
            COUNT(oi.order_item_id) as total_orders,
            ROUND(AVG(price_variance_pct), 2) as avg_variance
        FROM products p
        LEFT JOIN order_items oi ON p.product_id = oi.product_id
        LEFT JOIN (
            SELECT 
                product_id,
                ROUND((MAX(unit_price) - MIN(unit_price)) / MIN(unit_price) * 100, 2) AS price_variance_pct
            FROM order_items
            GROUP BY product_id
            HAVING COUNT(DISTINCT unit_price) > 1
        ) price_analysis ON p.product_id = price_analysis.product_id
        WHERE p.is_active = true
        GROUP BY p.category
        HAVING COUNT(oi.order_item_id) > 100
        ORDER BY avg_variance DESC
        LIMIT 5
    ) top_categories
);

-- =========================
-- 4. ВАЛИДАЦИЯ СОЗДАННОГО ДАТАСЕТА
-- =========================

-- Проверяем качество агрегированных данных
SELECT 
    'Общая статистика' as metric,
    COUNT(*) as total_records,
    COUNT(DISTINCT category) as categories,
    MIN(week_start) as first_week,
    MAX(week_start) as last_week
FROM ml_price_elasticity

UNION ALL

SELECT 
    'Записи с продажами' as metric,
    COUNT(*) as total_records,
    COUNT(DISTINCT category) as categories,
    NULL as first_week,
    NULL as last_week
FROM ml_price_elasticity
WHERE units_sold > 0

UNION ALL

SELECT 
    'Записи с ценами' as metric,
    COUNT(*) as total_records,
    COUNT(DISTINCT category) as categories,
    NULL as first_week,
    NULL as last_week
FROM ml_price_elasticity
WHERE avg_price > 0;

-- Детальная статистика по категориям
SELECT 
    category,
    COUNT(*) as total_weeks,
    COUNT(CASE WHEN units_sold > 0 THEN 1 END) as weeks_with_sales,
    COUNT(CASE WHEN avg_price > 0 THEN 1 END) as weeks_with_prices,
    ROUND(AVG(avg_price), 2) as avg_price_overall,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(revenue), 2) as total_revenue,
    ROUND(AVG(units_sold), 2) as avg_units_per_week
FROM ml_price_elasticity
GROUP BY category
ORDER BY total_units_sold DESC;

-- =========================
-- 5. ПРИМЕР ДАННЫХ ДЛЯ ВИЗУАЛИЗАЦИИ
-- =========================

-- Топ-5 категорий с данными для построения графика эластичности
SELECT 
    category,
    week_start,
    avg_price,
    units_sold,
    revenue,
    price_changes_count
FROM ml_price_elasticity
WHERE category IN (
    SELECT category 
    FROM ml_price_elasticity 
    GROUP BY category 
    ORDER BY SUM(units_sold) DESC 
    LIMIT 5
)
AND units_sold > 0  -- только недели с продажами
ORDER BY category, week_start
LIMIT 50;
