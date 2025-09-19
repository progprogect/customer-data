-- =========================
-- Log ML Features Statistics
-- =========================
-- Логирование статистики сгенерированных фич

DO $$
DECLARE
    total_rows_all INTEGER;
    total_rows_buyers INTEGER;
    weekly_snapshots_count INTEGER;
    unique_users INTEGER;
    min_snapshot DATE;
    max_snapshot DATE;
    log_message TEXT;
BEGIN
    -- Получаем статистику
    SELECT COUNT(*) INTO total_rows_all FROM ml_user_features_daily_all;
    SELECT COUNT(*) INTO total_rows_buyers FROM ml_user_features_daily_buyers;
    
    SELECT COUNT(DISTINCT snapshot_date) INTO weekly_snapshots_count 
    FROM ml_user_features_daily_all;
    
    SELECT COUNT(DISTINCT user_id) INTO unique_users 
    FROM ml_user_features_daily_all;
    
    SELECT MIN(snapshot_date), MAX(snapshot_date) 
    INTO min_snapshot, max_snapshot 
    FROM ml_user_features_daily_all;
    
    -- Формируем лог сообщение
    log_message := format(
        E'[ML FEATURES GENERATED - 6 MONTHS WEEKLY]\n' ||
        '📊 Статистика витрины фич:\n' ||
        '  • Всего строк (все пользователи): %s\n' ||
        '  • Строк только покупатели: %s\n' ||
        '  • Уникальных пользователей: %s\n' ||
        '  • Еженедельных снапшотов: %s\n' ||
        '📅 Временной диапазон:\n' ||
        '  • Минимальный snapshot_date: %s\n' ||
        '  • Максимальный snapshot_date: %s\n' ||
        '✅ Генерация витрины завершена успешно!',
        total_rows_all,
        total_rows_buyers,
        unique_users,
        weekly_snapshots_count,
        min_snapshot,
        max_snapshot
    );
    
    -- Выводим в лог
    RAISE NOTICE '%', log_message;
    
    -- Проверяем критические условия
    IF total_rows_all = 0 THEN
        RAISE EXCEPTION 'Не сгенерировано ни одной строки в ml_user_features_daily_all!';
    END IF;
    
    IF weekly_snapshots_count < 20 THEN
        RAISE WARNING 'Сгенерировано мало снапшотов (%): ожидалось минимум 20 для 6 месяцев', weekly_snapshots_count;
    END IF;
    
END $$;
