-- =========================
-- Validate NO Data Leakage
-- =========================
-- Комплексная проверка отсутствия утечек данных

DO $$
DECLARE
    -- Проверка recency
    min_recency INTEGER;
    max_recency INTEGER;
    negative_recency_count INTEGER;
    null_recency_count INTEGER;
    
    -- Основная статистика
    total_rows INTEGER;
    total_buyers INTEGER;
    unique_users INTEGER;
    unique_snapshots INTEGER;
    
    -- Временные границы
    min_snapshot DATE;
    max_snapshot DATE;
    
    validation_passed BOOLEAN := TRUE;
    log_message TEXT;
BEGIN
    -- === ПРОВЕРКА RECENCY ===
    SELECT 
        MIN(recency_days),
        MAX(recency_days),
        COUNT(CASE WHEN recency_days < 0 THEN 1 END),
        COUNT(CASE WHEN recency_days IS NULL THEN 1 END)
    INTO min_recency, max_recency, negative_recency_count, null_recency_count
    FROM ml_user_features_daily_all;
    
    -- === ОСНОВНАЯ СТАТИСТИКА ===
    SELECT COUNT(*) INTO total_rows FROM ml_user_features_daily_all;
    SELECT COUNT(*) INTO total_buyers FROM ml_user_features_daily_buyers;
    
    SELECT COUNT(DISTINCT user_id) INTO unique_users FROM ml_user_features_daily_all;
    SELECT COUNT(DISTINCT snapshot_date) INTO unique_snapshots FROM ml_user_features_daily_all;
    
    SELECT MIN(snapshot_date), MAX(snapshot_date) 
    INTO min_snapshot, max_snapshot 
    FROM ml_user_features_daily_all;
    
    -- === ФОРМИРОВАНИЕ ОТЧЕТА ===
    log_message := 
        E'[DATA LEAKAGE VALIDATION REPORT]\n' ||
        E'==================================\n' ||
        E'🔍 ПРОВЕРКА УТЕЧЕК ДАННЫХ:\n' ||
        '  • Минимальный recency_days: ' || COALESCE(min_recency::text, 'NULL') || E'\n' ||
        '  • Максимальный recency_days: ' || COALESCE(max_recency::text, 'NULL') || E'\n' ||
        '  • Отрицательных recency: ' || negative_recency_count || E'\n' ||
        '  • NULL recency: ' || null_recency_count || E'\n' ||
        E'\n📊 ОСНОВНАЯ СТАТИСТИКА:\n' ||
        '  • Всего строк (все пользователи): ' || total_rows || E'\n' ||
        '  • Строк покупателей: ' || total_buyers || E'\n' ||
        '  • Уникальных пользователей: ' || unique_users || E'\n' ||
        '  • Уникальных снапшотов: ' || unique_snapshots || E'\n' ||
        '  • Период: ' || min_snapshot || ' — ' || max_snapshot || E'\n';
    
    -- === КРИТИЧЕСКИЕ ПРОВЕРКИ ===
    
    -- Проверка 1: Отсутствие отрицательных recency
    IF negative_recency_count > 0 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Найдено ' || negative_recency_count || ' отрицательных recency_days!';
        validation_passed := FALSE;
    ELSE
        log_message := log_message || E'\n✅ Отрицательные recency_days: НЕ НАЙДЕНЫ';
    END IF;
    
    -- Проверка 2: Минимальный recency >= 0
    IF min_recency IS NOT NULL AND min_recency < 0 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: min_recency < 0 (' || min_recency || ')';
        validation_passed := FALSE;
    ELSE
        log_message := log_message || E'\n✅ Минимальный recency: OK (' || COALESCE(min_recency::text, 'NULL') || ')';
    END IF;
    
    -- Проверка 3: Разумные границы для recency
    IF max_recency IS NOT NULL AND max_recency > 1000 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Очень большой max_recency (' || max_recency || ' дней)';
    ELSE
        log_message := log_message || E'\n✅ Максимальный recency: OK (' || COALESCE(max_recency::text, 'NULL') || ')';
    END IF;
    
    -- Проверка 4: Размер датасета
    IF total_rows = 0 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Датасет пуст!';
        validation_passed := FALSE;
    ELSIF total_rows < 50000 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Маленький датасет (' || total_rows || ' < 50K)';
    ELSE
        log_message := log_message || E'\n✅ Размер датасета: OK (' || total_rows || ' строк)';
    END IF;
    
    -- Проверка 5: Покупатели vs все пользователи
    IF total_buyers = 0 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Нет покупателей в датасете!';
        validation_passed := FALSE;
    ELSE
        log_message := log_message || E'\n✅ Покупатели: OK (' || total_buyers || ' строк)';
    END IF;
    
    -- === ФИНАЛЬНЫЙ ВЕРДИКТ ===
    IF validation_passed THEN
        log_message := log_message || E'\n\n🎉 ИТОГ: УТЕЧКИ ДАННЫХ НЕ ОБНАРУЖЕНЫ! ДАТАСЕТ ЧИСТЫЙ!';
    ELSE
        log_message := log_message || E'\n\n🚨 ИТОГ: ОБНАРУЖЕНЫ УТЕЧКИ ДАННЫХ! ТРЕБУЕТСЯ ИСПРАВЛЕНИЕ!';
    END IF;
    
    -- Выводим полный отчет
    RAISE NOTICE '%', log_message;
    
    -- Если критические ошибки - прерываем выполнение
    IF NOT validation_passed THEN
        RAISE EXCEPTION 'Валидация утечек провалена! Обнаружены критические утечки данных.';
    END IF;
    
END $$;
