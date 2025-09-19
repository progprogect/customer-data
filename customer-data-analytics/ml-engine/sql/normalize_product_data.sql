-- =========================
-- Normalize Product Data
-- Нормализация и очистка данных товаров
-- =========================

-- Функция для нормализации тегов
CREATE OR REPLACE FUNCTION normalize_tags(input_tags TEXT[])
RETURNS TEXT[] AS $$
DECLARE
    tag TEXT;
    normalized_tags TEXT[] := '{}';
    clean_tag TEXT;
BEGIN
    -- Если массив пустой или NULL, возвращаем пустой массив
    IF input_tags IS NULL OR array_length(input_tags, 1) IS NULL THEN
        RETURN normalized_tags;
    END IF;
    
    -- Обрабатываем каждый тег
    FOREACH tag IN ARRAY input_tags
    LOOP
        -- Очистка тега: нижний регистр, удаление пробелов, NULL проверка
        clean_tag := LOWER(TRIM(tag));
        
        -- Пропускаем пустые теги
        IF clean_tag IS NOT NULL AND LENGTH(clean_tag) > 0 THEN
            -- Добавляем только если тега еще нет в массиве (убираем дубли)
            IF NOT (clean_tag = ANY(normalized_tags)) THEN
                normalized_tags := array_append(normalized_tags, clean_tag);
            END IF;
        END IF;
    END LOOP;
    
    RETURN normalized_tags;
END;
$$ LANGUAGE plpgsql;

-- Функция для нормализации текстового поля
CREATE OR REPLACE FUNCTION normalize_text_field(input_text TEXT)
RETURNS TEXT AS $$
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Приводим к нижнему регистру и убираем лишние пробелы
    RETURN LOWER(TRIM(input_text));
END;
$$ LANGUAGE plpgsql;

-- Функция для создания краткого описания из title
CREATE OR REPLACE FUNCTION create_short_description(title TEXT, description TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Если есть description, берем первые 200 символов
    IF description IS NOT NULL AND LENGTH(description) > 0 THEN
        RETURN LEFT(description, 200);
    END IF;
    
    -- Иначе используем title
    IF title IS NOT NULL THEN
        RETURN title;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Тестируем функции на примере
SELECT 
    normalize_tags(ARRAY['Brand A', 'SPORT', 'sport', ' casual ', NULL, '', 'TECH']) as test_tags,
    normalize_text_field(' TEST Brand ') as test_brand,
    create_short_description('iPhone 15', 'This is a great smartphone with amazing features') as test_desc;
