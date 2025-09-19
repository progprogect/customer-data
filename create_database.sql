-- =========================
-- Customer Data Database Schema
-- PostgreSQL 16+ compatible
-- =========================

-- Включаем расширение для case-insensitive text
CREATE EXTENSION IF NOT EXISTS citext;

-- =========================
-- 1) Справочники / пользователи
-- =========================
CREATE TABLE users (
  user_id        BIGSERIAL PRIMARY KEY,
  email          CITEXT UNIQUE,
  phone          TEXT,
  registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  country        TEXT,
  city           TEXT,
  gender         TEXT,
  birth_date     DATE
);

CREATE INDEX idx_users_registered_at ON users(registered_at);

-- =========================
-- 2) Каталог товаров (без субкатегорий)
-- =========================
CREATE TABLE product_categories (
  category TEXT PRIMARY KEY
);

CREATE TABLE products (
  product_id      BIGSERIAL PRIMARY KEY,
  title           TEXT NOT NULL,
  description     TEXT,
  category        TEXT REFERENCES product_categories(category),
  brand           TEXT,
  price           NUMERIC(12,2) CHECK (price >= 0),
  currency        TEXT NOT NULL DEFAULT 'USD',
  -- Часто используемые атрибуты (широкая модель)
  color           TEXT,
  size            TEXT,
  material        TEXT,
  gender          TEXT,           -- 'male'/'female'/'unisex' (или свои значения)
  style           TEXT,           -- sport/casual/classic/...
  rating          NUMERIC(3,2) CHECK (rating IS NULL OR (rating >= 0 AND rating <= 5)),
  tags            TEXT[],         -- свободные теги
  -- Для редких/динамичных свойств
  attributes_json JSONB,          -- {"heel_height":"5cm","sole":"rubber",...}
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_active       BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand    ON products(brand);
CREATE INDEX idx_products_price    ON products(price);
CREATE INDEX idx_products_color    ON products(color);
CREATE INDEX idx_products_style    ON products(style);
CREATE INDEX idx_products_tags_gin ON products USING GIN (tags);
CREATE INDEX idx_products_attr_gin ON products USING GIN (attributes_json);

-- История цен (для эластичности)
CREATE TABLE product_prices (
  product_id   BIGINT REFERENCES products(product_id) ON DELETE CASCADE,
  valid_from   DATE NOT NULL,
  price        NUMERIC(12,2) NOT NULL CHECK (price >= 0),
  currency     TEXT NOT NULL DEFAULT 'USD',
  promo_flag   BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (product_id, valid_from)
);

CREATE INDEX idx_product_prices_pid_date ON product_prices(product_id, valid_from);

-- =========================
-- 3) Заказы и позиции
-- =========================
CREATE TABLE orders (
  order_id      BIGSERIAL PRIMARY KEY,
  user_id       BIGINT REFERENCES users(user_id),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  status        TEXT NOT NULL,           -- e.g. 'created','paid','shipped','cancelled','completed'
  total_amount  NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
  currency      TEXT NOT NULL DEFAULT 'USD',
  promo_id      BIGINT                   -- на будущее (промо/купоны)
);

CREATE INDEX idx_orders_user_created ON orders(user_id, created_at);
CREATE INDEX idx_orders_status       ON orders(status);

CREATE TABLE order_items (
  order_item_id BIGSERIAL PRIMARY KEY,
  order_id      BIGINT REFERENCES orders(order_id) ON DELETE CASCADE,
  product_id    BIGINT REFERENCES products(product_id),
  quantity      NUMERIC(12,3) NOT NULL CHECK (quantity > 0),
  unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
  currency      TEXT NOT NULL DEFAULT 'USD'
);

CREATE INDEX idx_order_items_order   ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- =========================
-- 4) События поведения (event log)
-- =========================
CREATE TABLE user_events (
  event_id     BIGSERIAL PRIMARY KEY,
  user_id      BIGINT REFERENCES users(user_id),
  event_time   TIMESTAMPTZ NOT NULL,
  event_type   TEXT NOT NULL,        -- 'view_product','add_to_cart','purchase','search','page_view',...
  product_id   BIGINT,               -- если событие про товар
  value        NUMERIC,              -- опц.: цена/время/счётчик
  meta         JSONB                 -- опц.: {"query":"nike", "page":"plp", ...}
);

CREATE INDEX idx_events_user_time ON user_events(user_id, event_time);
CREATE INDEX idx_events_type_time ON user_events(event_type, event_time);
CREATE INDEX idx_events_product   ON user_events(product_id);
CREATE INDEX idx_events_meta_gin  ON user_events USING GIN (meta);

-- =========================
-- 5) Представление "контентный профиль товара"
-- =========================
CREATE OR REPLACE VIEW vw_product_profile AS
SELECT
  p.product_id,
  p.title,
  p.category,
  p.brand,
  p.price,
  p.currency,
  p.color,
  p.size,
  p.material,
  p.gender,
  p.style,
  p.rating,
  p.tags,
  p.attributes_json
FROM products p;
