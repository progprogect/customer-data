-- =========================
-- ML Item Similarity Content Table
-- Таблица для хранения content-based похожести товаров
-- =========================

-- Создание таблицы для офлайн хранения похожих товаров
CREATE TABLE IF NOT EXISTS ml_item_sim_content (
    product_id          BIGINT NOT NULL,
    similar_product_id  BIGINT NOT NULL,
    sim_score           NUMERIC(8,6) NOT NULL CHECK (sim_score >= 0 AND sim_score <= 1),
    features_used       TEXT NOT NULL,    -- список использованных признаков
    sim_breakdown       JSONB,            -- детализация по типам признаков
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (product_id, similar_product_id)
);

-- Основные индексы для производительности
CREATE INDEX IF NOT EXISTS idx_ml_item_sim_product_score 
    ON ml_item_sim_content(product_id, sim_score DESC);

CREATE INDEX IF NOT EXISTS idx_ml_item_sim_similar_score 
    ON ml_item_sim_content(similar_product_id, sim_score DESC);

CREATE INDEX IF NOT EXISTS idx_ml_item_sim_score 
    ON ml_item_sim_content(sim_score DESC);

CREATE INDEX IF NOT EXISTS idx_ml_item_sim_features 
    ON ml_item_sim_content(features_used);

-- GIN индекс для JSONB поиска
CREATE INDEX IF NOT EXISTS idx_ml_item_sim_breakdown 
    ON ml_item_sim_content USING GIN (sim_breakdown);

-- Функция для обновления updated_at
CREATE OR REPLACE FUNCTION update_ml_item_sim_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автообновления updated_at
CREATE TRIGGER trigger_ml_item_sim_updated_at
    BEFORE UPDATE ON ml_item_sim_content
    FOR EACH ROW
    EXECUTE FUNCTION update_ml_item_sim_updated_at();

-- Таблица для категорийных правил (cross-category recommendations)
CREATE TABLE IF NOT EXISTS ml_category_cross_rules (
    primary_category    TEXT NOT NULL,
    allowed_category    TEXT NOT NULL,
    cross_weight        NUMERIC(3,2) NOT NULL DEFAULT 1.0 CHECK (cross_weight > 0),
    max_cross_percent   NUMERIC(3,2) NOT NULL DEFAULT 0.3 CHECK (max_cross_percent >= 0 AND max_cross_percent <= 1),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (primary_category, allowed_category)
);

-- Заполнение базовых правил cross-category
INSERT INTO ml_category_cross_rules (primary_category, allowed_category, cross_weight, max_cross_percent) VALUES
    ('clothing', 'accessories', 0.8, 0.3),
    ('clothing', 'shoes', 0.7, 0.2),
    ('shoes', 'accessories', 0.6, 0.3),
    ('shoes', 'clothing', 0.5, 0.2),
    ('electronics', 'accessories', 0.7, 0.2),
    ('smartphones', 'electronics', 0.8, 0.3),
    ('smartphones', 'accessories', 0.6, 0.3),
    ('headphones', 'electronics', 0.8, 0.2),
    ('laptops', 'electronics', 0.9, 0.3),
    ('laptops', 'accessories', 0.7, 0.2),
    ('home', 'kitchen', 0.8, 0.3),
    ('kitchen', 'home', 0.8, 0.3),
    ('toys', 'clothing', 0.3, 0.1),
    ('beauty', 'accessories', 0.5, 0.2)
ON CONFLICT (primary_category, allowed_category) DO NOTHING;

-- Индексы для категорийных правил
CREATE INDEX IF NOT EXISTS idx_ml_category_cross_primary 
    ON ml_category_cross_rules(primary_category);

-- Комментарии
COMMENT ON TABLE ml_item_sim_content IS 
    'Content-based похожесть товаров для офлайн поиска рекомендаций';

COMMENT ON COLUMN ml_item_sim_content.sim_score IS 
    'Cosine similarity между товарами [0..1]';

COMMENT ON COLUMN ml_item_sim_content.features_used IS 
    'Список признаков: tfidf_tags,brand,category,style,numeric';

COMMENT ON COLUMN ml_item_sim_content.sim_breakdown IS 
    'Детализация по компонентам: {"tfidf": 0.7, "brand": 1.0, "category": 1.0, "numeric": 0.5}';

COMMENT ON TABLE ml_category_cross_rules IS 
    'Правила для cross-category рекомендаций';

COMMENT ON COLUMN ml_category_cross_rules.cross_weight IS 
    'Множитель для cross-category рекомендаций';

COMMENT ON COLUMN ml_category_cross_rules.max_cross_percent IS 
    'Максимальный % cross-category рекомендаций в выдаче';
