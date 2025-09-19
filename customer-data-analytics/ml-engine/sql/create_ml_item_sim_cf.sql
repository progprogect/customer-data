-- =========================
-- ML Item Similarity CF Table
-- Таблица для хранения Collaborative Filtering похожести товаров
-- =========================

-- Создание таблицы для item-item CF similarities
CREATE TABLE IF NOT EXISTS ml_item_sim_cf (
    product_id          BIGINT NOT NULL,
    similar_product_id  BIGINT NOT NULL,
    sim_score           NUMERIC(8,6) NOT NULL CHECK (sim_score >= 0 AND sim_score <= 1),
    co_users            INT NOT NULL CHECK (co_users >= 0),
    algorithm           TEXT NOT NULL DEFAULT 'item_knn_cosine',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (product_id, similar_product_id)
);

-- Основные индексы для производительности
CREATE INDEX IF NOT EXISTS idx_ml_item_sim_cf_product_score 
    ON ml_item_sim_cf(product_id, sim_score DESC);

CREATE INDEX IF NOT EXISTS idx_ml_item_sim_cf_similar_score 
    ON ml_item_sim_cf(similar_product_id, sim_score DESC);

CREATE INDEX IF NOT EXISTS idx_ml_item_sim_cf_co_users 
    ON ml_item_sim_cf(co_users DESC);

CREATE INDEX IF NOT EXISTS idx_ml_item_sim_cf_algorithm 
    ON ml_item_sim_cf(algorithm);

-- Функция для обновления updated_at
CREATE OR REPLACE FUNCTION update_ml_item_sim_cf_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автообновления updated_at
CREATE TRIGGER trigger_ml_item_sim_cf_updated_at
    BEFORE UPDATE ON ml_item_sim_cf
    FOR EACH ROW
    EXECUTE FUNCTION update_ml_item_sim_cf_updated_at();

-- Таблица для кеширования пользовательских рекомендаций
CREATE TABLE IF NOT EXISTS ml_user_recommendations_cache (
    user_id             BIGINT NOT NULL,
    algorithm           TEXT NOT NULL,
    recommendations     JSONB NOT NULL,
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at          TIMESTAMPTZ NOT NULL,
    
    PRIMARY KEY (user_id, algorithm)
);

-- Индекс для очистки истекших записей
CREATE INDEX IF NOT EXISTS idx_ml_user_reco_cache_expires 
    ON ml_user_recommendations_cache(expires_at);

-- Функция для очистки истекших записей кеша
CREATE OR REPLACE FUNCTION cleanup_expired_recommendations()
RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM ml_user_recommendations_cache 
    WHERE expires_at < NOW();
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Комментарии
COMMENT ON TABLE ml_item_sim_cf IS 
    'Item-item Collaborative Filtering похожести на основе ко-покупок';

COMMENT ON COLUMN ml_item_sim_cf.sim_score IS 
    'Cosine similarity между товарами по ко-покупкам [0..1]';

COMMENT ON COLUMN ml_item_sim_cf.co_users IS 
    'Количество пользователей, которые покупали оба товара';

COMMENT ON COLUMN ml_item_sim_cf.algorithm IS 
    'Алгоритм: item_knn_cosine, item_knn_jaccard, etc.';

COMMENT ON TABLE ml_user_recommendations_cache IS 
    'Кеш пользовательских рекомендаций для производительности';

COMMENT ON COLUMN ml_user_recommendations_cache.recommendations IS 
    'JSON массив рекомендаций с метаданными';

COMMENT ON COLUMN ml_user_recommendations_cache.expires_at IS 
    'Время истечения кеша (TTL)';
