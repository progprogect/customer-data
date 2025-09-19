-- =========================
-- Log Target Labels Statistics
-- =========================
-- Логирование статистики сгенерированных таргетов

DO $$
DECLARE
    total_rows INTEGER;
    positive_class_count INTEGER;
    negative_class_count INTEGER;
    positive_class_percent NUMERIC(5,2);
    min_snapshot DATE;
    max_snapshot DATE;
    max_order_in_db DATE;
    log_message TEXT;
BEGIN
    -- Получаем статистику
    SELECT COUNT(*) INTO total_rows FROM ml_labels_purchase_30d;
    
    SELECT COUNT(*) INTO positive_class_count 
    FROM ml_labels_purchase_30d 
    WHERE purchase_next_30d = TRUE;
    
    SELECT COUNT(*) INTO negative_class_count 
    FROM ml_labels_purchase_30d 
    WHERE purchase_next_30d = FALSE;
    
    positive_class_percent := (positive_class_count::NUMERIC / NULLIF(total_rows, 0)) * 100;
    
    SELECT MIN(snapshot_date), MAX(snapshot_date) 
    INTO min_snapshot, max_snapshot 
    FROM ml_labels_purchase_30d;
    
    SELECT MAX(created_at::date) 
    INTO max_order_in_db 
    FROM orders 
    WHERE status IN ('paid', 'shipped', 'completed');
    
    -- Формируем лог сообщение
    log_message := 
        E'[ML TARGET LABELS GENERATED]\n' ||
        '📊 Общая статистика:' || E'\n' ||
        '  • Всего строк (срезов): ' || total_rows || E'\n' ||
        '  • Положительный класс (purchase_next_30d=1): ' || positive_class_count || E'\n' ||
        '  • Отрицательный класс (purchase_next_30d=0): ' || negative_class_count || E'\n' ||
        '  • Процент положительного класса: ' || positive_class_percent || '%' || E'\n' ||
        '📅 Временные границы:' || E'\n' ||
        '  • Минимальный snapshot_date: ' || min_snapshot || E'\n' ||
        '  • Максимальный snapshot_date: ' || max_snapshot || E'\n' ||
        '  • Последний заказ в системе: ' || max_order_in_db || E'\n' ||
        '✅ Генерация завершена успешно!';
    
    -- Выводим в лог
    RAISE NOTICE 'ML TARGET LABELS GENERATED';
    RAISE NOTICE 'Всего строк: %, Положительный класс: %, Процент: %', total_rows, positive_class_count, positive_class_percent;
    
    -- Проверяем критические условия
    IF positive_class_percent < 5 OR positive_class_percent > 30 THEN
        RAISE WARNING 'Процент положительного класса (%) выходит за рекомендуемые границы 5-30%', positive_class_percent;
    END IF;
    
    IF total_rows = 0 THEN
        RAISE EXCEPTION 'Не сгенерировано ни одной строки! Проверьте данные.';
    END IF;
    
END $$;
