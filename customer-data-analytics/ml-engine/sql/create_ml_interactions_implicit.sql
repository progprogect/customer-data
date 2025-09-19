-- =========================
-- ML Interactions Implicit Table
-- Витрина взаимодействий для Collaborative Filtering
-- =========================

-- Создание таблицы для хранения implicit feedback данных
CREATE TABLE IF NOT EXISTS ml_interactions_implicit (
    user_id         BIGINT NOT NULL,
    product_id      BIGINT NOT NULL,
    event_ts        TIMESTAMPTZ NOT NULL,
    qty             NUMERIC(12,3) NOT NULL CHECK (qty > 0),
    price           NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    amount          NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    weight          FLOAT NOT NULL DEFAULT 1.0 CHECK (weight > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (user_id, product_id, event_ts)
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_ml_interactions_user_time 
    ON ml_interactions_implicit(user_id, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_ml_interactions_product_time 
    ON ml_interactions_implicit(product_id, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_ml_interactions_time 
    ON ml_interactions_implicit(event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_ml_interactions_weight 
    ON ml_interactions_implicit(weight DESC);

-- Дополнительные составные индексы для CF алгоритмов
CREATE INDEX IF NOT EXISTS idx_ml_interactions_user_product 
    ON ml_interactions_implicit(user_id, product_id);

CREATE INDEX IF NOT EXISTS idx_ml_interactions_product_user 
    ON ml_interactions_implicit(product_id, user_id);

-- Комментарии к полям
COMMENT ON TABLE ml_interactions_implicit IS 
    'Витрина implicit feedback взаимодействий пользователь-товар для Collaborative Filtering';

COMMENT ON COLUMN ml_interactions_implicit.user_id IS 
    'ID пользователя из таблицы users';

COMMENT ON COLUMN ml_interactions_implicit.product_id IS 
    'ID товара из таблицы products';

COMMENT ON COLUMN ml_interactions_implicit.event_ts IS 
    'Timestamp события покупки (created_at заказа)';

COMMENT ON COLUMN ml_interactions_implicit.qty IS 
    'Количество купленных единиц товара';

COMMENT ON COLUMN ml_interactions_implicit.price IS 
    'Цена за единицу товара (unit_price)';

COMMENT ON COLUMN ml_interactions_implicit.amount IS 
    'Общая сумма покупки (qty * price)';

COMMENT ON COLUMN ml_interactions_implicit.weight IS 
    'Вес сигнала для CF алгоритма (начинаем с 1.0 = бинарный)';

COMMENT ON COLUMN ml_interactions_implicit.created_at IS 
    'Timestamp создания записи в витрине';
