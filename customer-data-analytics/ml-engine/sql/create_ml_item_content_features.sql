-- =========================
-- ML Item Content Features Table
-- Витрина признаков товаров для Content-Based Filtering
-- =========================

-- Создание таблицы для хранения обработанных признаков товаров
CREATE TABLE IF NOT EXISTS ml_item_content_features (
    product_id          BIGINT PRIMARY KEY,
    
    -- Основные атрибуты (категориальные)
    brand               TEXT,
    category            TEXT,
    color               TEXT,
    size                TEXT,
    material            TEXT,
    gender              TEXT,
    style               TEXT,
    
    -- Числовые признаки
    price_current       NUMERIC(12,2),
    price_mean_90d      NUMERIC(12,2),    -- будем вычислять позже
    rating              NUMERIC(3,2),
    popularity_30d      NUMERIC(12,2),    -- будем вычислять на основе продаж
    
    -- Обработанные теги (нормализованные)
    tags_normalized     TEXT[],           -- очищенные теги
    tags_count          INT DEFAULT 0,
    
    -- Текстовые поля для TF-IDF (в будущем)
    title               TEXT,
    description_short   TEXT,
    
    -- Векторные представления (для будущего развития)
    tags_tfidf_vec      TEXT,             -- сериализованный sparse vector
    numeric_features    JSONB,            -- масштабированные числовые признаки
    
    -- Метаданные
    is_active           BOOLEAN NOT NULL DEFAULT true,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы для быстрого поиска по признакам
CREATE INDEX IF NOT EXISTS idx_ml_content_features_brand 
    ON ml_item_content_features(brand);

CREATE INDEX IF NOT EXISTS idx_ml_content_features_category 
    ON ml_item_content_features(category);

CREATE INDEX IF NOT EXISTS idx_ml_content_features_style 
    ON ml_item_content_features(style);

CREATE INDEX IF NOT EXISTS idx_ml_content_features_color 
    ON ml_item_content_features(color);

CREATE INDEX IF NOT EXISTS idx_ml_content_features_price 
    ON ml_item_content_features(price_current);

CREATE INDEX IF NOT EXISTS idx_ml_content_features_popularity 
    ON ml_item_content_features(popularity_30d DESC);

CREATE INDEX IF NOT EXISTS idx_ml_content_features_active 
    ON ml_item_content_features(is_active);

-- GIN индекс для поиска по тегам
CREATE INDEX IF NOT EXISTS idx_ml_content_features_tags 
    ON ml_item_content_features USING GIN (tags_normalized);

-- GIN индекс для JSONB поля
CREATE INDEX IF NOT EXISTS idx_ml_content_features_numeric 
    ON ml_item_content_features USING GIN (numeric_features);

-- Функция для обновления updated_at
CREATE OR REPLACE FUNCTION update_ml_content_features_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автообновления updated_at
CREATE TRIGGER trigger_ml_content_features_updated_at
    BEFORE UPDATE ON ml_item_content_features
    FOR EACH ROW
    EXECUTE FUNCTION update_ml_content_features_updated_at();

-- Комментарии к полям
COMMENT ON TABLE ml_item_content_features IS 
    'Витрина обработанных признаков товаров для Content-Based рекомендаций';

COMMENT ON COLUMN ml_item_content_features.product_id IS 
    'ID товара из таблицы products';

COMMENT ON COLUMN ml_item_content_features.tags_normalized IS 
    'Очищенные и нормализованные теги (lowercase, без дублей)';

COMMENT ON COLUMN ml_item_content_features.popularity_30d IS 
    'Популярность товара за 30 дней (общая сумма продаж)';

COMMENT ON COLUMN ml_item_content_features.tags_tfidf_vec IS 
    'Сериализованный TF-IDF вектор тегов (для косинусного сходства)';

COMMENT ON COLUMN ml_item_content_features.numeric_features IS 
    'Масштабированные числовые признаки в JSON формате';
