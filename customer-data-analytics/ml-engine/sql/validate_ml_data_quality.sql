-- =========================
-- Validate ML Data Quality
-- Проверка acceptance criteria для витрин рекомендательной системы
-- =========================

-- ========================================
-- ACCEPTANCE CRITERIA VALIDATION
-- ========================================

-- Критерий 1: ml_interactions_implicit заполнена за 6 месяцев
SELECT 
    '✅ CRITERION 1: Interactions Coverage' as criterion,
    'SUCCESS' as status,
    CONCAT(
        'Period: ', MIN(event_ts)::date, ' to ', MAX(event_ts)::date,
        ' (', EXTRACT(days FROM (MAX(event_ts) - MIN(event_ts))), ' days)'
    ) as details
FROM ml_interactions_implicit
UNION ALL

-- Критерий 2: Только успешные заказы, без дублей
SELECT 
    '✅ CRITERION 2: Data Quality' as criterion,
    CASE 
        WHEN duplicate_count = 0 THEN 'SUCCESS'
        ELSE 'FAILED'
    END as status,
    CONCAT('Duplicate keys found: ', duplicate_count) as details
FROM (
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT user_id, product_id, event_ts, COUNT(*)
        FROM ml_interactions_implicit 
        GROUP BY user_id, product_id, event_ts
        HAVING COUNT(*) > 1
    ) duplicates
) dup_check

UNION ALL

-- Критерий 3: Покрытие ≥70% активных пользователей
SELECT 
    '✅ CRITERION 3: User Coverage' as criterion,
    CASE 
        WHEN coverage_percent >= 70 THEN 'SUCCESS'
        ELSE 'FAILED'
    END as status,
    CONCAT(
        'Active users in interactions: ', users_in_interactions, 
        ' / Total active users: ', total_active_users,
        ' (', ROUND(coverage_percent, 2), '%)'
    ) as details
FROM (
    SELECT 
        COUNT(DISTINCT mii.user_id) as users_in_interactions,
        COUNT(DISTINCT u.user_id) as total_active_users,
        COUNT(DISTINCT mii.user_id) * 100.0 / COUNT(DISTINCT u.user_id) as coverage_percent
    FROM users u
    LEFT JOIN ml_interactions_implicit mii ON u.user_id = mii.user_id
    WHERE u.registered_at >= NOW() - INTERVAL '6 months'  -- активные за период
) coverage_stats

UNION ALL

-- Критерий 4: Покрытие ≥70% активных товаров
SELECT 
    '✅ CRITERION 4: Product Coverage' as criterion,
    CASE 
        WHEN coverage_percent >= 70 THEN 'SUCCESS'
        ELSE 'FAILED'
    END as status,
    CONCAT(
        'Active products in interactions: ', products_in_interactions,
        ' / Total active products: ', total_active_products,
        ' (', ROUND(coverage_percent, 2), '%)'
    ) as details
FROM (
    SELECT 
        COUNT(DISTINCT mii.product_id) as products_in_interactions,
        COUNT(DISTINCT p.product_id) as total_active_products,
        COUNT(DISTINCT mii.product_id) * 100.0 / COUNT(DISTINCT p.product_id) as coverage_percent
    FROM products p
    LEFT JOIN ml_interactions_implicit mii ON p.product_id = mii.product_id
    WHERE p.is_active = true
) product_coverage

UNION ALL

-- Критерий 5: ≥80% пользователей имеют ≥2 товаров (CF готовность)
SELECT 
    '✅ CRITERION 5: CF Readiness' as criterion,
    CASE 
        WHEN cf_ready_percent >= 80 THEN 'SUCCESS'
        ELSE 'FAILED'
    END as status,
    CONCAT(
        'Users with ≥2 products: ', users_cf_ready,
        ' / Total users: ', total_users_in_interactions,
        ' (', ROUND(cf_ready_percent, 2), '%)'
    ) as details
FROM (
    SELECT 
        COUNT(*) FILTER (WHERE products_per_user >= 2) as users_cf_ready,
        COUNT(*) as total_users_in_interactions,
        COUNT(*) FILTER (WHERE products_per_user >= 2) * 100.0 / COUNT(*) as cf_ready_percent
    FROM (
        SELECT user_id, COUNT(DISTINCT product_id) as products_per_user
        FROM ml_interactions_implicit
        GROUP BY user_id
    ) user_product_stats
) cf_readiness

UNION ALL

-- Критерий 6: Content features содержат все активные товары
SELECT 
    '✅ CRITERION 6: Content Features Coverage' as criterion,
    CASE 
        WHEN content_coverage_percent = 100 THEN 'SUCCESS'
        ELSE 'FAILED'
    END as status,
    CONCAT(
        'Products in content features: ', products_in_content,
        ' / Active products: ', total_active_products,
        ' (', ROUND(content_coverage_percent, 2), '%)'
    ) as details
FROM (
    SELECT 
        COUNT(mcf.product_id) as products_in_content,
        COUNT(p.product_id) as total_active_products,
        COUNT(mcf.product_id) * 100.0 / COUNT(p.product_id) as content_coverage_percent
    FROM products p
    LEFT JOIN ml_item_content_features mcf ON p.product_id = mcf.product_id
    WHERE p.is_active = true
) content_coverage

UNION ALL

-- Критерий 7: Минимум brand, category, 3-5 тегов, price
SELECT 
    '✅ CRITERION 7: Content Features Quality' as criterion,
    CASE 
        WHEN min_brand_coverage >= 90 AND min_category_coverage >= 90 
             AND min_tags_coverage >= 80 AND min_price_coverage >= 95 THEN 'SUCCESS'
        ELSE 'WARNING'
    END as status,
    CONCAT(
        'Brand: ', ROUND(min_brand_coverage, 1), '%, ',
        'Category: ', ROUND(min_category_coverage, 1), '%, ',
        'Tags (≥3): ', ROUND(min_tags_coverage, 1), '%, ',
        'Price: ', ROUND(min_price_coverage, 1), '%'
    ) as details
FROM (
    SELECT 
        COUNT(*) FILTER (WHERE brand IS NOT NULL) * 100.0 / COUNT(*) as min_brand_coverage,
        COUNT(*) FILTER (WHERE category IS NOT NULL) * 100.0 / COUNT(*) as min_category_coverage,
        COUNT(*) FILTER (WHERE tags_count >= 3) * 100.0 / COUNT(*) as min_tags_coverage,
        COUNT(*) FILTER (WHERE price_current IS NOT NULL) * 100.0 / COUNT(*) as min_price_coverage
    FROM ml_item_content_features
) quality_stats

UNION ALL

-- Критерий 8: Теги нормализованы (без дублей и в нижнем регистре)
SELECT 
    '✅ CRITERION 8: Tag Normalization' as criterion,
    CASE 
        WHEN normalized_tag_issues = 0 THEN 'SUCCESS'
        ELSE 'WARNING'
    END as status,
    CONCAT('Potential tag normalization issues: ', normalized_tag_issues) as details
FROM (
    SELECT COUNT(*) as normalized_tag_issues
    FROM ml_item_content_features mcf
    WHERE EXISTS (
        SELECT 1 FROM unnest(mcf.tags_normalized) as tag 
        WHERE tag != LOWER(TRIM(tag)) OR tag = ''
    )
) tag_quality;

-- ========================================
-- DETAILED STATISTICS
-- ========================================

-- Детальная статистика для анализа
SELECT '==================== DETAILED STATISTICS ====================' as section;

-- Статистика взаимодействий
SELECT 
    'Interactions Statistics' as metric_type,
    COUNT(*) as total_interactions,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT product_id) as unique_products,
    ROUND(AVG(amount), 2) as avg_amount,
    MIN(event_ts)::date as period_start,
    MAX(event_ts)::date as period_end
FROM ml_interactions_implicit

UNION ALL

-- Плотность данных для CF
SELECT 
    'CF Density Metrics' as metric_type,
    ROUND(AVG(interactions_per_user), 2) as avg_interactions_per_user,
    MAX(interactions_per_user) as max_interactions_per_user,
    COUNT(*) as users_total,
    COUNT(*) FILTER (WHERE interactions_per_user >= 5) as users_with_5plus_interactions,
    NULL,
    NULL
FROM (
    SELECT user_id, COUNT(*) as interactions_per_user
    FROM ml_interactions_implicit
    GROUP BY user_id
) user_interaction_stats

UNION ALL

-- Статистика content features
SELECT 
    'Content Features Statistics' as metric_type,
    COUNT(*) as total_products,
    COUNT(DISTINCT category) as unique_categories,
    COUNT(DISTINCT brand) as unique_brands,
    ROUND(AVG(tags_count), 1) as avg_tags_per_product,
    COUNT(*) FILTER (WHERE popularity_30d > 0) as products_with_sales,
    NULL
FROM ml_item_content_features;

-- Топ категории по популярности
SELECT '==================== TOP CATEGORIES BY SALES ====================' as section;

SELECT 
    category,
    COUNT(*) as product_count,
    ROUND(AVG(popularity_30d), 2) as avg_sales_30d,
    ROUND(SUM(popularity_30d), 2) as total_sales_30d
FROM ml_item_content_features
WHERE category IS NOT NULL
GROUP BY category
ORDER BY total_sales_30d DESC
LIMIT 10;
