-- =========================
-- Validate Time Split Quality
-- =========================
-- Комплексная валидация качества временного разреза

DO $$
DECLARE
    -- Основная статистика
    train_rows INTEGER;
    valid_rows INTEGER;
    test_rows INTEGER;
    total_rows INTEGER;
    
    -- Баланс классов
    train_pos_rate NUMERIC(5,2);
    valid_pos_rate NUMERIC(5,2);
    test_pos_rate NUMERIC(5,2);
    
    -- Проверка пересечений
    overlaps_count INTEGER;
    
    -- Статистика по фичам
    train_avg_recency NUMERIC(8,2);
    valid_avg_recency NUMERIC(8,2);
    test_avg_recency NUMERIC(8,2);
    
    train_avg_frequency NUMERIC(8,2);
    valid_avg_frequency NUMERIC(8,2);
    test_avg_frequency NUMERIC(8,2);
    
    train_avg_monetary NUMERIC(12,2);
    valid_avg_monetary NUMERIC(12,2);
    test_avg_monetary NUMERIC(12,2);
    
    -- Границы дат
    train_min_date DATE;
    train_max_date DATE;
    valid_min_date DATE;
    valid_max_date DATE;
    test_min_date DATE;
    test_max_date DATE;
    
    log_message TEXT;
    validation_passed BOOLEAN := TRUE;
BEGIN
    -- === ОСНОВНАЯ СТАТИСТИКА ===
    SELECT COUNT(*) INTO train_rows FROM ml_training_dataset WHERE split = 'train';
    SELECT COUNT(*) INTO valid_rows FROM ml_training_dataset WHERE split = 'valid';
    SELECT COUNT(*) INTO test_rows FROM ml_training_dataset WHERE split = 'test';
    total_rows := train_rows + valid_rows + test_rows;
    
    -- === БАЛАНС КЛАССОВ ===
    SELECT 
        ROUND(COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
    INTO train_pos_rate 
    FROM ml_training_dataset WHERE split = 'train';
    
    SELECT 
        ROUND(COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
    INTO valid_pos_rate 
    FROM ml_training_dataset WHERE split = 'valid';
    
    SELECT 
        ROUND(COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END)::NUMERIC / COUNT(*) * 100, 2)
    INTO test_pos_rate 
    FROM ml_training_dataset WHERE split = 'test';
    
    -- === ПРОВЕРКА ПЕРЕСЕЧЕНИЙ ===
    SELECT COUNT(*) INTO overlaps_count
    FROM (
        SELECT user_id, snapshot_date, COUNT(DISTINCT split) as split_count
        FROM ml_training_dataset 
        GROUP BY user_id, snapshot_date
        HAVING COUNT(DISTINCT split) > 1
    ) t;
    
    -- === ГРАНИЦЫ ДАТ ===
    SELECT MIN(snapshot_date), MAX(snapshot_date) INTO train_min_date, train_max_date 
    FROM ml_training_dataset WHERE split = 'train';
    
    SELECT MIN(snapshot_date), MAX(snapshot_date) INTO valid_min_date, valid_max_date 
    FROM ml_training_dataset WHERE split = 'valid';
    
    SELECT MIN(snapshot_date), MAX(snapshot_date) INTO test_min_date, test_max_date 
    FROM ml_training_dataset WHERE split = 'test';
    
    -- === СТАТИСТИКА ПО ФИЧАМ ===
    SELECT AVG(recency_days), AVG(frequency_90d), AVG(monetary_180d) 
    INTO train_avg_recency, train_avg_frequency, train_avg_monetary
    FROM ml_training_dataset WHERE split = 'train';
    
    SELECT AVG(recency_days), AVG(frequency_90d), AVG(monetary_180d) 
    INTO valid_avg_recency, valid_avg_frequency, valid_avg_monetary
    FROM ml_training_dataset WHERE split = 'valid';
    
    SELECT AVG(recency_days), AVG(frequency_90d), AVG(monetary_180d) 
    INTO test_avg_recency, test_avg_frequency, test_avg_monetary
    FROM ml_training_dataset WHERE split = 'test';
    
    -- === ФОРМИРОВАНИЕ ОТЧЕТА ===
    log_message := 
        E'[TIME SPLIT VALIDATION REPORT]\n' ||
        E'===============================\n' ||
        E'📊 РАЗМЕРЫ СПЛИТОВ:\n' ||
        '  • Train: ' || train_rows || ' строк (' || ROUND(train_rows::NUMERIC / total_rows * 100, 1) || '%)' || E'\n' ||
        '  • Valid: ' || valid_rows || ' строк (' || ROUND(valid_rows::NUMERIC / total_rows * 100, 1) || '%)' || E'\n' ||
        '  • Test:  ' || test_rows || ' строк (' || ROUND(test_rows::NUMERIC / total_rows * 100, 1) || '%)' || E'\n' ||
        '  • Всего: ' || total_rows || ' строк' || E'\n' ||
        E'\n🎯 БАЛАНС КЛАССОВ:\n' ||
        '  • Train positive rate: ' || train_pos_rate || '%' || E'\n' ||
        '  • Valid positive rate: ' || valid_pos_rate || '%' || E'\n' ||
        '  • Test positive rate:  ' || test_pos_rate || '%' || E'\n' ||
        E'\n📅 ГРАНИЦЫ ДАТ:\n' ||
        '  • Train: ' || train_min_date || ' — ' || train_max_date || E'\n' ||
        '  • Valid: ' || valid_min_date || ' — ' || valid_max_date || E'\n' ||
        '  • Test:  ' || test_min_date || ' — ' || test_max_date || E'\n' ||
        E'\n📈 СРЕДНИЕ ЗНАЧЕНИЯ ФИЧЕЙ:\n' ||
        '  • Recency  - Train: ' || ROUND(train_avg_recency, 1) || ', Valid: ' || ROUND(valid_avg_recency, 1) || ', Test: ' || ROUND(test_avg_recency, 1) || E'\n' ||
        '  • Frequency - Train: ' || ROUND(train_avg_frequency, 1) || ', Valid: ' || ROUND(valid_avg_frequency, 1) || ', Test: ' || ROUND(test_avg_frequency, 1) || E'\n' ||
        '  • Monetary - Train: $' || ROUND(train_avg_monetary, 0) || ', Valid: $' || ROUND(valid_avg_monetary, 0) || ', Test: $' || ROUND(test_avg_monetary, 0) || E'\n';
    
    -- === ВАЛИДАЦИОННЫЕ ПРОВЕРКИ ===
    
    -- Проверка 1: Отсутствие пересечений
    IF overlaps_count > 0 THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Найдено ' || overlaps_count || ' пересечений между сплитами!';
        validation_passed := FALSE;
    ELSE
        log_message := log_message || E'\n✅ Пересечения между сплитами: НЕ НАЙДЕНЫ';
    END IF;
    
    -- Проверка 2: Временная последовательность
    IF train_max_date >= valid_min_date OR valid_max_date >= test_min_date THEN
        log_message := log_message || E'\n❌ КРИТИЧЕСКАЯ ОШИБКА: Нарушена временная последовательность!';
        validation_passed := FALSE;
    ELSE
        log_message := log_message || E'\n✅ Временная последовательность: КОРРЕКТНА';
    END IF;
    
    -- Проверка 3: Баланс классов (±5 п.п. от train)
    IF ABS(valid_pos_rate - train_pos_rate) > 5 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Valid баланс отличается от Train на ' || ROUND(ABS(valid_pos_rate - train_pos_rate), 1) || ' п.п.';
    END IF;
    
    IF ABS(test_pos_rate - train_pos_rate) > 5 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Test баланс отличается от Train на ' || ROUND(ABS(test_pos_rate - train_pos_rate), 1) || ' п.п.';
    END IF;
    
    IF ABS(valid_pos_rate - train_pos_rate) <= 5 AND ABS(test_pos_rate - train_pos_rate) <= 5 THEN
        log_message := log_message || E'\n✅ Баланс классов: В ПРЕДЕЛАХ НОРМЫ (±5 п.п.)';
    END IF;
    
    -- Проверка 4: Размеры сплитов
    IF valid_rows < 5000 OR test_rows < 5000 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Маленькие размеры valid/test (<5K строк)';
    ELSE
        log_message := log_message || E'\n✅ Размеры сплитов: ДОСТАТОЧНЫЕ';
    END IF;
    
    -- Проверка 5: Экстремальные отличия в фичах (>50% отличие)
    IF ABS(valid_avg_frequency - train_avg_frequency) / train_avg_frequency > 0.5 OR
       ABS(test_avg_frequency - train_avg_frequency) / train_avg_frequency > 0.5 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Экстремальные отличия в frequency между сплитами';
    END IF;
    
    IF ABS(valid_avg_monetary - train_avg_monetary) / NULLIF(train_avg_monetary, 0) > 0.5 OR
       ABS(test_avg_monetary - train_avg_monetary) / NULLIF(train_avg_monetary, 0) > 0.5 THEN
        log_message := log_message || E'\n⚠️ ПРЕДУПРЕЖДЕНИЕ: Экстремальные отличия в monetary между сплитами';
    END IF;
    
    -- === ФИНАЛЬНЫЙ ВЕРДИКТ ===
    IF validation_passed THEN
        log_message := log_message || E'\n\n🎉 ИТОГ: ВРЕМЕННОЙ РАЗРЕЗ КАЧЕСТВЕННЫЙ! ГОТОВ ДЛЯ ОБУЧЕНИЯ!';
    ELSE
        log_message := log_message || E'\n\n🚨 ИТОГ: ОБНАРУЖЕНЫ КРИТИЧЕСКИЕ ПРОБЛЕМЫ! ТРЕБУЕТСЯ ИСПРАВЛЕНИЕ!';
    END IF;
    
    -- Выводим полный отчет
    RAISE NOTICE '%', log_message;
    
    -- Если критические ошибки - прерываем выполнение
    IF NOT validation_passed THEN
        RAISE EXCEPTION 'Валидация временного разреза провалена! Обнаружены критические ошибки.';
    END IF;
    
END $$;
