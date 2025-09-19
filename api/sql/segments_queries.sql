-- =========================
-- SQL Queries for Segments API
-- =========================
-- Запросы для получения данных о сегментах пользователей

-- 1. Распределение сегментов на дату
-- GET /segments/distribution?date=2025-09-18
CREATE OR REPLACE FUNCTION get_segments_distribution(target_date DATE DEFAULT NULL)
RETURNS TABLE (
    snapshot_date DATE,
    cluster_id INT,
    users_count BIGINT,
    total_users BIGINT,
    share NUMERIC
) AS $$
BEGIN
    -- Если дата не указана, берем последний доступный снапшот
    IF target_date IS NULL THEN
        SELECT MAX(snapshot_date) INTO target_date FROM user_segments_kmeans;
    END IF;
    
    RETURN QUERY
    WITH segment_counts AS (
        SELECT 
            s.snapshot_date,
            s.cluster_id,
            COUNT(*) as users_count
        FROM user_segments_kmeans s
        WHERE s.snapshot_date = target_date
        GROUP BY s.snapshot_date, s.cluster_id
    ),
    total_count AS (
        SELECT SUM(sc.users_count)::BIGINT as total FROM segment_counts sc
    )
    SELECT 
        sc.snapshot_date,
        sc.cluster_id,
        sc.users_count,
        tc.total as total_users,
        ROUND(sc.users_count::NUMERIC / tc.total, 4) as share
    FROM segment_counts sc
    CROSS JOIN total_count tc
    ORDER BY sc.cluster_id;
END;
$$ LANGUAGE plpgsql;

-- 2. Динамика сегментов за период
-- GET /segments/dynamics?from=2025-09-01&to=2025-09-18
CREATE OR REPLACE FUNCTION get_segments_dynamics(
    date_from DATE,
    date_to DATE,
    granularity TEXT DEFAULT 'day'
)
RETURNS TABLE (
    cluster_id INT,
    date_point DATE,
    users_count BIGINT
) AS $$
DECLARE
    date_interval INTERVAL;
BEGIN
    -- Валидация granularity
    IF granularity NOT IN ('day', 'week', 'month') THEN
        RAISE EXCEPTION 'Invalid granularity. Must be day, week, or month';
    END IF;
    
    -- Определяем интервал группировки
    CASE granularity
        WHEN 'day' THEN date_interval := INTERVAL '1 day';
        WHEN 'week' THEN date_interval := INTERVAL '1 week';
        WHEN 'month' THEN date_interval := INTERVAL '1 month';
    END CASE;
    
    RETURN QUERY
    SELECT 
        s.cluster_id,
        CASE granularity
            WHEN 'day' THEN s.snapshot_date
            WHEN 'week' THEN DATE_TRUNC('week', s.snapshot_date)::DATE
            WHEN 'month' THEN DATE_TRUNC('month', s.snapshot_date)::DATE
        END as date_point,
        COUNT(*) as users_count
    FROM user_segments_kmeans s
    WHERE s.snapshot_date BETWEEN date_from AND date_to
    GROUP BY 
        s.cluster_id,
        CASE granularity
            WHEN 'day' THEN s.snapshot_date
            WHEN 'week' THEN DATE_TRUNC('week', s.snapshot_date)::DATE
            WHEN 'month' THEN DATE_TRUNC('month', s.snapshot_date)::DATE
        END
    ORDER BY s.cluster_id, date_point;
END;
$$ LANGUAGE plpgsql;

-- 3. Матрица переходов между сегментами
-- GET /segments/migration?date=2025-09-18
CREATE OR REPLACE FUNCTION get_segments_migration(target_date DATE)
RETURNS TABLE (
    from_cluster INT,
    to_cluster INT,
    users_moved BIGINT
) AS $$
DECLARE
    prev_date DATE;
BEGIN
    -- Вычисляем предыдущую дату
    prev_date := target_date - INTERVAL '1 day';
    
    -- Проверяем наличие данных за обе даты
    IF NOT EXISTS (SELECT 1 FROM user_segments_kmeans WHERE snapshot_date = target_date) THEN
        RAISE EXCEPTION 'No data available for target date %', target_date;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM user_segments_kmeans WHERE snapshot_date = prev_date) THEN
        RAISE EXCEPTION 'No data available for previous date %', prev_date;
    END IF;
    
    RETURN QUERY
    WITH today_segments AS (
        SELECT user_id, cluster_id as cluster_today
        FROM user_segments_kmeans
        WHERE snapshot_date = target_date
    ),
    yesterday_segments AS (
        SELECT user_id, cluster_id as cluster_yesterday
        FROM user_segments_kmeans
        WHERE snapshot_date = prev_date
    )
    SELECT 
        ys.cluster_yesterday as from_cluster,
        ts.cluster_today as to_cluster,
        COUNT(*) as users_moved
    FROM today_segments ts
    JOIN yesterday_segments ys USING (user_id)
    GROUP BY ys.cluster_yesterday, ts.cluster_today
    ORDER BY ys.cluster_yesterday, ts.cluster_today;
END;
$$ LANGUAGE plpgsql;

-- 4. Метаданные о кластерах
-- GET /segments/meta
CREATE OR REPLACE FUNCTION get_segments_meta()
RETURNS TABLE (
    cluster_id INT,
    cluster_name TEXT,
    description TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        0 as cluster_id,
        'Спящие клиенты' as cluster_name,
        'Пользователи с низкой активностью и давними покупками' as description
    UNION ALL
    SELECT 
        1 as cluster_id,
        'VIP / Лояльные клиенты' as cluster_name,
        'Активные пользователи с высокими тратами и частыми покупками' as description
    UNION ALL
    SELECT 
        2 as cluster_id,
        'Обычные клиенты' as cluster_name,
        'Пользователи со средними показателями активности и трат' as description;
END;
$$ LANGUAGE plpgsql;

-- 5. Последняя доступная дата
CREATE OR REPLACE FUNCTION get_last_available_date()
RETURNS DATE AS $$
DECLARE
    last_date DATE;
BEGIN
    SELECT MAX(snapshot_date) INTO last_date FROM user_segments_kmeans;
    RETURN last_date;
END;
$$ LANGUAGE plpgsql;

-- 6. Проверка доступности данных за дату
CREATE OR REPLACE FUNCTION is_data_available(target_date DATE)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM user_segments_kmeans 
        WHERE snapshot_date = target_date
    );
END;
$$ LANGUAGE plpgsql;

-- Создание индексов для оптимизации
CREATE INDEX IF NOT EXISTS idx_user_segments_date_cluster 
ON user_segments_kmeans(snapshot_date, cluster_id);

CREATE INDEX IF NOT EXISTS idx_user_segments_user_date 
ON user_segments_kmeans(user_id, snapshot_date);
