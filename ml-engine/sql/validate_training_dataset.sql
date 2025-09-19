-- =========================
-- Validate Training Dataset Quality
-- =========================
-- Комплексная валидация качества обучающего датасета

DO $$
DECLARE
    -- Основная статистика
    total_rows INTEGER;
    unique_users INTEGER;
    unique_snapshots INTEGER;
    
    -- Баланс классов
    positive_class INTEGER;
    negative_class INTEGER;
    positive_percent NUMERIC(5,2);
    
    -- Анализ NULL значений
    null_recency INTEGER;
    null_aov INTEGER;
    total_nulls INTEGER;
    
    -- Временные границы
    min_snapshot DATE;
    max_snapshot DATE;
    
    -- Статистика по признакам
    avg_recency NUMERIC(8,2);
    avg_frequency NUMERIC(8,2);
    avg_monetary NUMERIC(12,2);
    avg_orders_lifetime NUMERIC(8,2);
    
    log_message TEXT;
    validation_passed BOOLEAN := TRUE;
BEGIN
    -- === ОСНОВНАЯ СТАТИСТИКА ===
    SELECT COUNT(*) INTO total_rows FROM ml_training_dataset;
    
    SELECT COUNT(DISTINCT user_id) INTO unique_users FROM ml_training_dataset;
    
    SELECT COUNT(DISTINCT snapshot_date) INTO unique_snapshots FROM ml_training_dataset;
    
    -- === БАЛАНС КЛАССОВ ===
    SELECT COUNT(*) INTO positive_class 
    FROM ml_training_dataset 
    WHERE purchase_next_30d = TRUE;
    
    SELECT COUNT(*) INTO negative_class 
    FROM ml_training_dataset 
    WHERE purchase_next_30d = FALSE;
    
    positive_percent := (positive_class::NUMERIC / NULLIF(total_rows, 0)) * 100;
    
    -- === АНАЛИЗ NULL ЗНАЧЕНИЙ ===
    SELECT COUNT(*) INTO null_recency 
    FROM ml_training_dataset 
    WHERE recency_days IS NULL;
    
    SELECT COUNT(*) INTO null_aov 
    FROM ml_training_dataset 
    WHERE aov_180d IS NULL;
    
    total_nulls := null_recency + null_aov;
    
    -- === ВРЕМЕННЫЕ ГРАНИЦЫ ===
    SELECT MIN(snapshot_date), MAX(snapshot_date) 
    INTO min_snapshot, max_snapshot 
    FROM ml_training_dataset;
    
    -- === СТАТИСТИКА ПО ПРИЗНАКАМ ===
    SELECT 
        AVG(recency_days),
        AVG(frequency_90d),
        AVG(monetary_180d),
        AVG(orders_lifetime)
    INTO avg_recency, avg_frequency, avg_monetary, avg_orders_lifetime
    FROM ml_training_dataset;
    
    -- === ФОРМИРОВАНИЕ ОТЧЕТА ===
    log_message := 
        E'[TRAINING DATASET VALIDATION REPORT]\n' ||
        E'=====================================\n' ||
        E'📊 ОСНОВНАЯ СТАТИСТИКА:\n' ||
        '  • Всего строк: ' || total_rows || E'\n' ||
        '  • Уникальных пользователей: ' || unique_users || E'\n' ||
        '  • Уникальных снапшотов: ' || unique_snapshots || E'\n' ||
        '  • Период: ' || min_snapshot || ' — ' || max_snapshot || E'\n' ||
        E'\n🎯 БАЛАНС КЛАССОВ:\n' ||
        '  • Положительный класс (покупки): ' || positive_class || E'\n' ||
        '  • Отрицательный класс (нет покупок): ' || negative_class || E'\n' ||
        '  • Процент положительного класса: ' || positive_percent || '%' || E'\n' ||
        E'\n🔍 АНАЛИЗ ПРОПУСКОВ:\n' ||
        '  • NULL в recency_days: ' || null_recency || E'\n' ||
        '  • NULL в aov_180d: ' || null_aov || E'\n' ||
        '  • Общее количество NULL: ' || total_nulls || E'\n' ||
        E'\n📈 СТАТИСТИКА ПРИЗНАКОВ:\n' ||
        '  • Средний recency_days: ' || COALESCE(ROUND(avg_recency, 2), 0) || E'\n' ||
        '  • Средний frequency_90d: ' || ROUND(avg_frequency, 2) || E'\n' ||
        '  • Средний monetary_180d: $' || ROUND(avg_monetary, 2) || E'\n' ||
        '  • Средний orders_lifetime: ' || ROUND(avg_orders_lifetime, 2) || E'\n';
    
    -- === ВАЛИДАЦИОННЫЕ ПРОВЕРКИ ===
    
    -- Проверка 1: Размер датасета
    IF total_rows = 0 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Датасет пуст!';
        validation_passed := FALSE;
    ELSIF total_rows < 10000 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Мало данных для обучения (' || total_rows || ' < 10K)';
    ELSE
        log_message := log_message || E'\n✅ Размер датасета: OK (' || total_rows || ' строк)';
    END IF;
    
    -- Проверка 2: Соответствие ожидаемому размеру (69K из лейблов)
    IF total_rows != 69000 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Размер не соответствует ожидаемому (69K)';
    ELSE
        log_message := log_message || E'\n✅ Размер соответствует ожидаемому: OK';
    END IF;
    
    -- Проверка 3: Баланс классов
    IF positive_percent < 5 OR positive_percent > 50 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Дисбаланс классов (' || positive_percent || '%)';
    ELSE
        log_message := log_message || E'\n✅ Баланс классов: OK (' || positive_percent || '%)';
    END IF;
    
    -- Проверка 4: Критические NULL значения
    IF null_recency > total_rows * 0.8 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Слишком много NULL в recency_days';
        validation_passed := FALSE;
    ELSE
        log_message := log_message || E'\n✅ NULL в recency_days: OK (' || null_recency || ')';
    END IF;
    
    -- Проверка 5: Уникальность пользователей
    IF unique_users < 1000 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Мало уникальных пользователей (' || unique_users || ')';
    ELSE
        log_message := log_message || E'\n✅ Количество пользователей: OK (' || unique_users || ')';
    END IF;
    
    -- === ФИНАЛЬНЫЙ ВЕРДИКТ ===
    IF validation_passed THEN
        log_message := log_message || E'\n\n🎉 ИТОГ: ДАТАСЕТ ГОТОВ К ОБУЧЕНИЮ!';
    ELSE
        log_message := log_message || E'\n\n💥 ИТОГ: КРИТИЧЕСКИЕ ОШИБКИ! ОБУЧЕНИЕ НЕВОЗМОЖНО!';
    END IF;
    
    -- Выводим полный отчет
    RAISE NOTICE '%', log_message;
    
    -- Если критические ошибки - прерываем выполнение
    IF NOT validation_passed THEN
        RAISE EXCEPTION 'Валидация датасета провалена! Исправьте критические ошибки.';
    END IF;
    
END $$;
